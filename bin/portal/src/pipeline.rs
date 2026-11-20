use std::{
    collections::HashMap,
    sync::Arc,
    path::Path
};
use eyre::Result;
use tracing::{info, warn, debug};
use tokio::{
    sync::{mpsc, oneshot, Semaphore, OwnedSemaphorePermit},
    time::{Instant, Duration, interval},
};
use tokio_util::sync::CancellationToken;
use blake3::Hash;
use libp2p::{
    PeerId,
    kad::QueryId
};
use sqlx::PgPool;
use crate::admission::{
    BASE_BLOB_PATH, ToPipeline, CHUNK_SIZE
};
use crate::blob::{
    LocalBlobContext, LocalBlob, LocalChunk, InflightPush,
    RemoteBlobContext, RemoteBlob, RemoteChunk, InflightPull
};
use peyk::{
    blob_transfer,
    blob_transfer::{Direction, TransferOutcome, Outcome, TIMEOUT_WINDOW},
    FromSwarm, ToSwarm,
    protocol::Manifest,
};

// one hour
const MANIFEST_RECORD_TTL: u64 = 3600;

// provider hints are valid for 5 minutes
const STORAGE_PROVIDER_HINT_DECAY: u64 = 5 * 60;

const MAX_RETRIES: usize = 5;

// 30 minutes before a blob is marked as failed
const BLOB_STALE_DEADLINE: u64 = 30 * 60;

// 5 minutes before a chunk is marked as failed
const CHUNK_STALE_DEADLINE: u64 = 5 * 60;

struct Pipeline {
    db_pool: PgPool,
    remote_blob: Option<RemoteBlobContext>,
    local_blob: Option<LocalBlobContext>,

    // record decaying WouldStore gossips
    storage_provider_hints: HashMap<PeerId, Instant>,
    storage_permits: HashMap<PeerId, Instant>,

    tx_swarm: mpsc::Sender<ToSwarm>,

    blob_transfer_control: peyk::Control,
    tx_transfer_outcome: mpsc::Sender<TransferOutcome>,

    // to gate i/o usage
    download_permit: Arc<Semaphore>,
    upload_permit: Arc<Semaphore>
}

impl Pipeline {
    fn new(
        db_pool: PgPool,
        tx_swarm: mpsc::Sender<ToSwarm>,
        blob_transfer_control: peyk::Control,
        tx_transfer_outcome: mpsc::Sender<TransferOutcome>
    ) -> Self {
        Pipeline {
            db_pool,
            remote_blob: None,
            local_blob: None,
            storage_provider_hints: HashMap::new(),
            storage_permits: HashMap::new(),
            tx_swarm,
            blob_transfer_control,
            tx_transfer_outcome,
            download_permit: Arc::new(Semaphore::new(32)), // 32 * 4 = 128 MiB of inflight downloads
            upload_permit: Arc::new(Semaphore::new(20)),   // 20 * 4 =  80 MiB of inflight uploads
        }
    }

    async fn add_remote_blob(
        &mut self,
        id: String
    ) {
        if self.remote_blob.is_some() {
            warn!("Another remote blob is already being pulled.");
            return
        }
        self.remote_blob = Some(RemoteBlobContext {
            id,
            created_at: Instant::now(),
            blob: RemoteBlob::AwaitingManifest(vec![])
        });
        self.maintain_remote_blob().await;
    }

    async fn add_local_blob(
        &mut self,
        id: String,
        root_hash: Hash,
        merkle_leaves: Vec<Hash>,
        _admission_permit: OwnedSemaphorePermit
    ) {
        if self.local_blob.is_some() {
            warn!("Another local blob is already being pushed.");
            return
        }
        let base_path = Path::new(BASE_BLOB_PATH).join(&id);
        if let Err(e) = tokio::fs::create_dir_all(&base_path).await {
            warn!(blob = %id, error = %e, "Failed to create the local blob folder.");
            return
        }
        let chunks: HashMap<Hash, LocalChunk> = merkle_leaves
            .iter()
            .map(|hash| (*hash, LocalChunk::AwaitingAssignment))
            .collect();
        self.local_blob = Some(LocalBlobContext {
            id,
            created_at: Instant::now(),
            blob: LocalBlob::Distributing {
                root_hash, merkle_leaves, chunks, base_path, _admission_permit
            }
        });
        self.assign_chunks();
    }

    async fn handle_publish_manifest_result(
        &mut self,
        id: String,
        query_id: QueryId,
        succeeded: bool
    ) {
        let Some(LocalBlobContext {
            id: local_id,
            blob: local_blob,
            ..
        }) = self.local_blob.as_mut()
        else {
            return
        };
        // blob
        let LocalBlob::AwaitingPublishManifest { attempts, failed, .. } = local_blob else {
            debug!(blob = %id, query_id = %query_id, "Unsolicited publish manifest result");
            return
        };
        if id != *local_id {
            debug!(blob = %local_id, incoming_blob = %id,
                "Publish manifest Ids mismatch."
            );
            return
        }
        if !attempts.iter().any(|(ex_query_id, _)| *ex_query_id == query_id) {
            debug!(query_id = %query_id, "Unsolicited publish manifest query result");
            return
        }
        if succeeded {
            debug!(blob = %local_id, "Blob is now distributed globally.");
            *local_blob = LocalBlob::Published;
            self.archive_local_blob();
        } else {
            *failed += 1;
            if *failed >= MAX_RETRIES {
                *local_blob = LocalBlob::Failed;
                debug!(blob = %local_id,
                    "Max publish manifest retries reached, so blob has failed."
                );
                self.archive_local_blob();
            }
        }
    }

    async fn publish_manifest(&mut self) {
        let Some(LocalBlobContext {
            id,
            blob: LocalBlob::AwaitingPublishManifest { manifest, attempts, .. },
            ..
        }) = self.local_blob.as_mut()
        else {
            debug!("The local blob is not publishing manifest.");
            return
        };
        let (tx_reply, rx) = oneshot::channel::<QueryId>();
        if let Err(e) = self.tx_swarm.send(ToSwarm::PublishManifest {
            blob_id: id.clone(),
            manifest: manifest.clone(),
            expires_at: Instant::now().checked_add(
                Duration::from_secs(MANIFEST_RECORD_TTL)
            ).unwrap_or(Instant::now()), // decay immediately
            tx_reply
        }).await {
            warn!(blob = %id, error = %e, "Failed to send the manifest to the Swarm channel.");
        } else {
            let query_id = match rx.await {
                Ok(r) => r,
                Err(_) => {
                    debug!("Publish manifest reply channel is closed.");
                    return
                }
            };
            debug!(blob = %id, "Manifest is now being published.");
            attempts.push((query_id, Instant::now()));
        }
    }

    async fn request_storage_permits(&mut self) {
        if self.storage_provider_hints.is_empty() {
            warn!("No storage providers out there to request permits from.");
            return
        }
        let now = Instant::now();
        self.storage_permits.retain(|_, expires_at| *expires_at > now);
        if !self.storage_permits.is_empty() {
                // todo: check if there are enough permits
            return
        }
        // todo: peers size should not be large
        let _ = self.tx_swarm.send(ToSwarm::StoragePermit {
            peers: self.storage_provider_hints.keys().cloned().collect()
        }).await;
    }

    fn archive_local_blob(&mut self) {
        let Some(LocalBlobContext {
            id,
            ..
        }) = self.local_blob.take()
        else {
            return
        };
        info!(blob = %id, "Archiving local blob.");
        // todo: archival
    }

    fn assign_chunks(&mut self) {
        let num_available_permits = self.upload_permit.available_permits();
        if num_available_permits == 0 {
            // nothing to be done
            return
        }
        let Some(LocalBlobContext {
            blob: LocalBlob::Distributing { chunks, base_path, .. },
            ..
        }) = self.local_blob.as_mut()
        else {
            return
        };
        let now = Instant::now();
        let peers: Vec<PeerId> = self
            .storage_permits
            .iter()
            .filter(|(_, expires_at)| **expires_at > now)
            .map(|(peer, _)| *peer)
            .collect();
        if peers.is_empty() {
            warn!("No eligible peers to assign.");
            return
        }
        let is_eligible = |lc: &LocalChunk, peers: &[PeerId]| match lc {
            LocalChunk::AwaitingAssignment => true,
            LocalChunk::Assigned { inflight, failed, .. }  => {
                let has_timed_out = if let Some(InflightPush { created_at, .. }) = inflight {
                    now.duration_since(*created_at).as_secs() > TIMEOUT_WINDOW
                } else {
                    true
                };
                has_timed_out
                && peers
                    .iter()
                    .any(|peer| failed.get(peer).copied().unwrap_or_default() < MAX_RETRIES)
            },
            _ => false
        };
        for (hash, chunk) in chunks
            .iter_mut()
            .filter(|(_, c)| is_eligible(c, &peers))
            .take(num_available_permits)
        {
            let mut eligible_peers = peers.clone();
            eligible_peers.retain(|peer| match chunk {
                LocalChunk::AwaitingAssignment => true,
                LocalChunk::Assigned { failed, .. } => {
                    *failed.entry(*peer).or_default() < MAX_RETRIES
                }
                _ => false
            });
            // todo: peer selection strategy(fastest, most reliable, ...)
            let chosen_peer = {
                let mut rng = rand::rng();
                use rand::seq::IndexedRandom;
                let Some(peer) = eligible_peers.choose(&mut rng) else {
                    warn!("Could not choose a peer: IndexedRandom misbehaved.");
                    continue
                };
                *peer
            };
            let hash_hex = hash.to_hex();
            let Ok(permit) = self
                .upload_permit
                .clone()
                .try_acquire_owned()
            else {
                debug!(to = %chosen_peer, hash = %hash_hex,
                    "Could not get an upload permit for transfer."
                );
                break
            };
            let now = Instant::now();
            let (path, failed) = match chunk {
                LocalChunk::AwaitingAssignment => (
                    base_path.as_path().join(hash_hex.as_str()),
                    HashMap::new()
                ),
                LocalChunk::Assigned { path, failed, .. } => (path.clone(), failed.clone()),
                _ => continue
            };
            let inflight = Some(InflightPush {
                to: chosen_peer,
                created_at: now
            });
            *chunk = LocalChunk::Assigned {
                path: path.clone(),
                inflight,
                failed
            };
            // push it
            debug!(chunk = %hash_hex, to = %chosen_peer, "Chunk push is initiated.");
            blob_transfer::push(
                self.blob_transfer_control.clone(),
                chosen_peer,
                *hash,
                path.clone(),
                permit,
                self.tx_transfer_outcome.clone(),
            );
        }
    }

    async fn handle_manifest(
        &mut self,
        id: String,
        manifest: Manifest
    ) {
        let Manifest::Blob { root_hash, chunks: chunk_owners } = manifest;
        let Some(RemoteBlobContext {
            id: remote_id,
            blob: remote_blob,
            ..
        }) = self.remote_blob.as_mut()
        else {
            debug!(blob = %id, "Unsolicited blob manifest.");
            return
        };
        if *remote_id != id {
            debug!(expected = %remote_id, given = %id, "Blob manifest Ids mismatch.");
            return
        }
        match remote_blob {
            RemoteBlob::AwaitingManifest(_) => {
                // todo: validate root hash
                let blob_path = Path::new(BASE_BLOB_PATH).join(&id);
                if let Err(e) = tokio::fs::create_dir_all(&blob_path).await {
                    warn!(blob = %id, error = %e, path = %blob_path.display(),
                        "Failed to create a folder for the remote blob."
                    );
                    *remote_blob = RemoteBlob::Failed;
                    // todo: failing here is cruel
                    return
                }
                let chunks: HashMap<Hash, RemoteChunk> = chunk_owners
                    .iter()
                    .map(|(plain_hash, peer)| {
                        let hash = Hash::from(*plain_hash);
                        (
                            hash,
                            RemoteChunk::Pulling {
                                path: blob_path.as_path().join(hash.to_hex().as_str()),
                                owners: [*peer].into(),
                                inflight: None,
                                failed: HashMap::new()
                            }
                        )
                    })
                    .collect();
                *remote_blob = RemoteBlob::Gathering {
                    root_hash: root_hash.into(),
                    merkle_leaves: chunk_owners
                        .into_iter()
                        .map(|(h, _)| h.into())
                        .collect(),
                    base_path: blob_path,
                    chunks
                };
            }
            RemoteBlob::Failed => {
                // todo
            }
            _ => ()
        };
    }

    async fn maintain_local_blob(&mut self) {
        let Some(LocalBlobContext {
            created_at: blob_created_at,
            blob: local_blob,
            ..
        }) = self.local_blob.as_mut()
        else {
            return
        };
        let now = Instant::now();
        if now.duration_since(*blob_created_at).as_secs() > BLOB_STALE_DEADLINE {
            *local_blob = LocalBlob::Failed;
            self.archive_local_blob();
            return
        }
        match local_blob {
            LocalBlob::Distributing { .. } => {
                self.assign_chunks();
            }
            LocalBlob::AwaitingPublishManifest { attempts, .. } => {
                if !attempts.is_empty()
                    && now
                        .duration_since(attempts.first().unwrap().1)
                        .as_secs()
                        > BLOB_STALE_DEADLINE
                {
                    *local_blob = LocalBlob::Failed;
                    self.archive_local_blob();
                    return
                }
                if !attempts.is_empty()
                    && now
                        .duration_since(attempts.last().unwrap().1)
                        .as_secs()
                        < TIMEOUT_WINDOW
                {
                    // nothing to be done
                    return
                }
                self.publish_manifest().await;
            }
            _ => ()
        };
    }

    async fn maintain_remote_blob(&mut self) {
        let Some(RemoteBlobContext {
            id,
            created_at: blob_created_at,
            blob: remote_blob
        }) = self.remote_blob.as_mut()
        else {
            return
        };
        let now = Instant::now();
        if now.duration_since(*blob_created_at).as_secs() > BLOB_STALE_DEADLINE {
            *remote_blob = RemoteBlob::Failed;
            self.archive_remote_blob();
            return
        }
        match remote_blob {
            RemoteBlob::AwaitingManifest(attempts) => {
                if !attempts.is_empty() {
                    if now
                        .duration_since(*attempts.first().unwrap())
                        .as_secs()
                        > CHUNK_STALE_DEADLINE
                    {
                        warn!(blob = %id, "Cannot request more manifest lookups: timeout reached.");
                        *remote_blob = RemoteBlob::Failed;
                        return
                    }
                    if now
                        .duration_since(*attempts.last().unwrap())
                        .as_secs()
                        < TIMEOUT_WINDOW
                    {
                        // nothing to be done
                        return
                    }
                }
                if attempts.len() >= MAX_RETRIES {
                    warn!(blob = %id,
                        "Cannot request more manifest lookups: max retries attempted."
                    );
                    *remote_blob = RemoteBlob::Failed;
                    return
                }
                if let Err(e) = self.tx_swarm.send(ToSwarm::LookupManifest {
                    blobs: vec![id.clone()],
                }).await {
                    warn!(blob = %id, error = %e,
                        "Failed to send a `lookup manifest` request to the Swarm channel."
                    );
                } else {
                    attempts.push(now);
                    debug!(blob = %id, remaining = MAX_RETRIES - attempts.len(),
                        "Sent another manifest lookup request."
                    );
                }
            },
            RemoteBlob::Gathering { chunks, .. } => {
                for (hash, chunk) in chunks.iter_mut() {
                    let hash_str = hash.to_string();
                    match chunk {
                        RemoteChunk::Pulling { path, owners, inflight, failed } => {
                            if inflight.is_some() {
                                continue
                            }
                            let Ok(permit) = self
                                .download_permit
                                .clone()
                                .try_acquire_owned()
                            else {
                                continue
                            };
                            let mut eligible_peers: Vec<(PeerId, usize)> = owners
                                .iter()
                                .filter_map(|peer| {
                                    let attempts = failed
                                        .get(peer)
                                        .copied()
                                        .unwrap_or_default();
                                    if attempts < MAX_RETRIES {
                                        Some((*peer, attempts))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            eligible_peers.sort_by(|x, y| x.1.cmp(&y.1));
                            let Some((peer, attempts)) = eligible_peers.first() else {
                                *chunk = RemoteChunk::Failed;
                                continue
                            };
                            *inflight = Some(InflightPull {
                                from: *peer,
                                created_at: now
                            });
                            // pull it
                            blob_transfer::pull(
                                self.blob_transfer_control.clone(),
                                *peer,
                                *hash,
                                path.clone(),
                                CHUNK_SIZE,
                                permit,
                                self.tx_transfer_outcome.clone(),
                            );
                            debug!(chunk = %hash_str, from = %peer, attempt_no = %attempts,
                                "Chunk pull is initiated."
                            );
                        }
                        RemoteChunk::Failed => {
                            // todo
                        }
                        _ => (),
                    };
                }
                // todo: wait for inflight pulls to finish
                if chunks.values().any(|c| matches!(c, RemoteChunk::Failed)) {
                    *remote_blob = RemoteBlob::Failed;
                }
            },
            RemoteBlob::Failed => {
                // todo
            }
        };
    }

    async fn check_for_new_blob_requests(&mut self) {
        // todo
    }

    async fn maintain(&mut self) {
        let now = Instant::now();
        self.storage_provider_hints
            .retain(|_, created_at| {
                now.duration_since(*created_at).as_secs() < STORAGE_PROVIDER_HINT_DECAY
            });
        self.maintain_local_blob().await;
        self.maintain_remote_blob().await;
        self.check_for_new_blob_requests().await;
    }

    fn archive_remote_blob(&mut self) {
        let Some(RemoteBlobContext {
            id,
            ..
        }) = self.remote_blob.take()
        else {
            return
        };
        debug!(blob = %id, "Archiving remote blob.");
        // todo: archival
    }

    async fn handle_transfer_outcome_for_local_blob(
        &mut self,
        from: PeerId,
        hash: Hash,
        succeeded: bool
    ) {
        let Some(LocalBlobContext {
            blob: local_blob,
            ..
        }) = self.local_blob.as_mut()
        else {
            debug!("No Local blob.");
            return
        };
        debug!(chunk = %hash.to_hex(), from = %from, succeeded = %succeeded,
            "A new outcome for the local blob."
        );
        let LocalBlob::Distributing { root_hash, merkle_leaves, chunks, .. } = local_blob
        else {
            debug!("Local blob is no distributing.");
            return
        };
        let Some(chunk) = chunks.get_mut(&hash) else {
            debug!("Unsolicited chunk.");
            return
        };
        let LocalChunk::Assigned { inflight, failed, .. } = chunk else {
            debug!("Not pushing.");
            return
        };
        let Some(InflightPush { to, .. }) = inflight else {
            debug!("No active inflight push.");
            // todo: late success
            return
        };
        if *to != from {
            debug!(expected = %to, "Peers do not match.");
            return
        }
        if succeeded {
            *chunk = LocalChunk::Pushed(from);
            if chunks.values().all(|c| matches!(c, LocalChunk::Pushed(_))) {
                let manifest = Manifest::Blob {
                    root_hash: *root_hash.as_bytes(),
                    chunks: merkle_leaves
                        .iter()
                        .map(|h| (
                            *h.as_bytes(),
                            chunks
                                .get(h)
                                .unwrap()
                                .extract_owner()
                                .unwrap()
                        ))
                        .collect()
                };
                *local_blob = LocalBlob::AwaitingPublishManifest {
                    manifest,
                    attempts: vec![],
                    failed: 0
                };
                self.publish_manifest().await;
            }
        } else {
            let attempts = failed.entry(from).or_default();
            *attempts += 1;
            *inflight = None;
            if *attempts >= MAX_RETRIES {
                *chunk = LocalChunk::Failed;
                // todo: wait for inflight pushes to finish
                if chunks.values().any(|c| matches!(c, LocalChunk::Failed)) {
                    *local_blob = LocalBlob::Failed;
                }
            }
        }
    }

    async fn handle_transfer_outcome_for_remote_blob(
        &mut self,
        from: PeerId,
        hash: Hash,
        succeeded: bool
    ) {
        let Some(RemoteBlobContext {
            blob: remote_blob,
            ..
        }) = self.remote_blob.as_mut()
        else {
            debug!("No Remote blob.");
            return
        };
        debug!(chunk = %hash.to_hex(), succeeded = %succeeded, from = %from,
            "A new outcome for the remote blob."
        );
        let RemoteBlob::Gathering { chunks, .. } = remote_blob else {
            debug!("Remote blob is not gathering.");
            return
        };
        let Some(chunk) = chunks.get_mut(&hash) else {
            debug!("Chunk not found.");
            return
        };
        let RemoteChunk::Pulling { inflight, failed, .. } = chunk else {
            debug!("Chunk not pulling.");
            return
        };
        let Some(InflightPull { from: ex_from, .. }) = inflight else {
            debug!("No active inflight pull for the chunk.");
            return
        };
        if *ex_from != from {
            debug!(expected = %ex_from, "Peers do not match.");
            return
        }
        if succeeded {
            *chunk = RemoteChunk::Pulled(from);
            if chunks.values().all(|c| matches!(c, RemoteChunk::Pulled(_))) {
                self.archive_remote_blob();
            }
        } else {
            let attempts = failed.entry(from).or_default();
            *attempts += 1;
            if *attempts >= MAX_RETRIES {
                *chunk = RemoteChunk::Failed;
                return
            }
            *inflight = None;
            // todo: wait for inflight pulls to finish
            if chunks.values().any(|c| matches!(c, RemoteChunk::Failed)) {
                *remote_blob = RemoteBlob::Failed;
            }
        }
    }

    async fn handle_transfer_outcome(
        &mut self,
        from: PeerId,
        hash: Hash,
        direction: Direction,
        succeeded: bool
    ) {
        match direction {
            Direction::Pull => {
                self.handle_transfer_outcome_for_remote_blob(from, hash, succeeded).await;
                self.maintain_remote_blob().await;
            }
            Direction::Push => {
                self.handle_transfer_outcome_for_local_blob(from, hash, succeeded).await;
                self.maintain_local_blob().await;
            }
        }
    }
}

pub async fn run(
    db_pool: PgPool,
    mut rx_admission: mpsc::Receiver<ToPipeline>,
    mut rx_handler: mpsc::Receiver<FromSwarm>,
    tx_swarm: mpsc::Sender<ToSwarm>,
    blob_transfer_control: peyk::Control,
    shutdown: CancellationToken
) -> Result<tokio::task::JoinHandle<()>> {
    //  setup blob transfer
    let (tx_transfer_outcome, mut rx_transfer_outcome) = mpsc::channel(256);
    let mut pipeline = Pipeline::new(
        db_pool,
        tx_swarm,
        blob_transfer_control,
        tx_transfer_outcome
    );
    let mut timer_maintenance = interval(Duration::from_secs(10));
    let jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Received shutdown request.");
                    break
                },
                // to advance blobs to completion
                _i = timer_maintenance.tick() => {
                    pipeline.maintain().await;
                },
                // swarm handlers
                hm = rx_handler.recv() => match hm {
                    Some(h_msg) => match h_msg {
                        // a gossip by storage providers
                        FromSwarm::WouldStore(peer) => {
                            pipeline.storage_provider_hints.insert(peer, Instant::now());
                        }
                        FromSwarm::Request { .. } => {}
                        FromSwarm::Response { peer, response, .. } => {
                            match response {
                                peyk::protocol::Response::IssuedStoragePermit { valid_for } => {
                                    let now = Instant::now();
                                    pipeline.storage_permits.insert(
                                        peer,
                                        now.checked_add(Duration::from_secs(valid_for))
                                        .unwrap_or(now)
                                    );
                                    // todo: storage permit is already invalid in case of overflow
                                    pipeline.assign_chunks();
                                }
                            }
                        }
                        FromSwarm::PublishManifestResult { blob_id, query_id, succeeded } => {
                            pipeline.handle_publish_manifest_result(
                                blob_id, query_id, succeeded
                            ).await;
                        }
                        FromSwarm::ManifestIsReady { id, manifest } => {
                            pipeline.handle_manifest(id, manifest).await;
                            pipeline.maintain_remote_blob().await;
                        }
                    }
                    None => {
                        warn!("Swarm channel is closed.");
                        break
                    }
                },
                // messages
                r = rx_admission.recv() => match r {
                    Some(m) => match m {
                        // remote blob: gather chunks
                        ToPipeline::GatherBlob(id) => {
                            pipeline.add_remote_blob(id).await;
                        }
                        // local blob: chunk and distribute
                        ToPipeline::DistributeBlob {
                            id, root_hash, merkle_leaves, admission_permit, ..
                        } => {
                            // todo: request storage permits
                            pipeline.add_local_blob(
                                id, root_hash, merkle_leaves, admission_permit
                            ).await;
                        }
                    }
                    None => {
                        warn!("Message channel is closed.");
                        break
                    }
                },
                // <blob transfer>
                // outcomes
                o = rx_transfer_outcome.recv() => match o {
                    Some(outcome) => match outcome {
                        TransferOutcome::Outgoing(
                            Outcome { peer, hash, direction, succeeded }
                        ) => {
                            pipeline.handle_transfer_outcome(
                                peer, hash, direction, succeeded
                            ).await;
                        }
                        _ => ()
                    }
                    None => {
                        break
                    }
                },
            }
        }
    });
    Ok(jh)
}
