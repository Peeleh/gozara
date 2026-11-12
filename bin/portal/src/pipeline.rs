use std::{
    collections::HashMap,
    sync::Arc,
    path::Path
};
use eyre::Result;
use tracing::{info, warn, debug};
use tokio::{
    sync::{mpsc, oneshot, Semaphore, OwnedSemaphorePermit},
    time::{Instant, Duration, timeout, interval},
};
use tokio_util::sync::CancellationToken;
use blake3::Hash;
use libp2p::{
    PeerId,
    request_response::OutboundRequestId,
    kad::QueryId
};
use sqlx::PgPool;
use crate::admission::{
    BASE_BLOB_PATH, ToPipeline, CHUNK_SIZE
};
use crate::blob::{
    LocalBlob, LocalChunk, PushContext, RemoteBlob, RemoteChunk, PullContext
};
use peyk::{
    blob_transfer, FromSwarm, ToSwarm,
    protocol::Manifest,
    request_pull
};

// one hour
const MANIFEST_RECORD_TTL: u64 = 3600;

// provider hints are valid for 5 minutes
const STORAGE_PROVIDER_HINT_DECAY: u64 = 5 * 60;

// 60 seconds as a general timeout
const TIMEOUT_WINDOW: u64 = 60;

const MAX_RETRIES: usize = 5;

// 30 minutes before a blob is marked as failed
const BLOB_STALE_DEADLINE: u64 = 30 * 60;

// 5 minutes before a chunk is marked as failed
const CHUNK_STALE_DEADLINE: u64 = 5 * 60;

enum InternalMessage {
    ChunkUploadResult {
        id: Hash,
        to: PeerId,
        succeeded: bool, // todo: replace with io::Result
    }
}

struct Pipeline {
    db_pool: PgPool,
    remote_blob: Option<(String, RemoteBlob)>,
    local_blob: Option<(String, LocalBlob)>,

    // record decaying WouldStore gossips
    storage_provider_hints: HashMap<PeerId, Instant>,
    storage_permits: HashMap<PeerId, Instant>,

    tx_internal: mpsc::UnboundedSender<InternalMessage>,

    tx_swarm: mpsc::Sender<ToSwarm>,

    blob_transfer_control: peyk::Control,
    tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>,

    // to gate i/o usage
    download_permit: Arc<Semaphore>,
    upload_permit: Arc<Semaphore>
}

impl Pipeline {
    fn new(
        db_pool: PgPool,
        tx_internal: mpsc::UnboundedSender<InternalMessage>,
        tx_swarm: mpsc::Sender<ToSwarm>,
        blob_transfer_control: peyk::Control,
        tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>
    ) -> Self {
        Pipeline {
            db_pool,
            remote_blob: None,
            local_blob: None,
            storage_provider_hints: HashMap::new(),
            storage_permits: HashMap::new(),
            tx_internal,
            tx_swarm,
            blob_transfer_control,
            tx_blob_transfer_events,
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
        self.remote_blob = Some((id, RemoteBlob::AwaitingManifest(vec![])));
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
        let path = Path::new(BASE_BLOB_PATH).join(&id);
        if let Err(e) = tokio::fs::create_dir_all(&path).await {
            warn!(blob = %id, error = %e, "Failed to create the local blob folder.");
            return
        }
        let chunks: HashMap<Hash, LocalChunk> = merkle_leaves
            .iter()
            .map(|hash| (*hash, LocalChunk::AwaitingAssignment))
            .collect();
        self.local_blob = Some((
            id,
            LocalBlob::Distributing {
                root_hash, merkle_leaves, chunks, path, _admission_permit
            }
        ));
        self.assign_chunks();
    }

    async fn handle_publish_manifest_result(
        &mut self,
        id: String,
        query_id: QueryId,
        succeeded: bool
    ) {
        let Some((local_id, local_blob)) = &mut self.local_blob else {
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
            self.archive_local_blob().await;
        } else {
            *failed += 1;
            if *failed >= MAX_RETRIES {
                *local_blob = LocalBlob::Failed;
                debug!(blob = %local_id,
                    "Max manifest publish attempts retries attempted, so blob has failed."
                );
                self.archive_local_blob().await;
            }
        }
    }

    async fn publish_manifest(&mut self) {
        let Some((id, LocalBlob::AwaitingPublishManifest { manifest, attempts, .. })) =
            &mut self.local_blob
        else {
            debug!("The local blob is not `publishing manifest`.");
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

    async fn archive_local_blob(&mut self) {
        let Some((_id, _local_blob)) = self.local_blob.take() else {
            return
        };
        // todo: archival
    }

    fn assign_chunks(&mut self) {
        let num_available_permits = self.upload_permit.available_permits();
        if num_available_permits == 0 {
            // nothing to be done
            return
        }
        let Some((local_id, LocalBlob::Distributing { chunks, path, .. })) = &mut self.local_blob else {
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
            LocalChunk::Assigned { context, failed, .. }  => {
                context.is_none() &&
                peers.iter().any(|peer| match failed.get(peer) {
                    Some(attempts) => {
                        attempts.len() < MAX_RETRIES
                        && now.duration_since(*attempts.first().unwrap()).as_secs()
                            < CHUNK_STALE_DEADLINE
                    }
                    None => true,
                })
            },
            _ => false
        };
        let mut rng = rand::rng();
        for (&hash, chunk) in chunks
            .iter_mut()
            .filter(|(_, c)| is_eligible(c, &peers))
            .take(num_available_permits)
        {
            let mut eligible_peers = peers.clone();
            eligible_peers.retain(|peer| match chunk {
                LocalChunk::AwaitingAssignment => true,
                LocalChunk::Assigned { failed, .. } => {
                    if let Some(attempts) = failed.get(peer) {
                        let first_try = *attempts.first().unwrap();
                        attempts.len() < MAX_RETRIES
                        && now.duration_since(first_try).as_secs() < CHUNK_STALE_DEADLINE
                    } else {
                        true
                    }
                }
                _ => false
            });
            // todo: peer selection strategy(fastest, most reliable, ...)
            let peer = {
                use rand::seq::IndexedRandom;
                let Some(peer) = eligible_peers.choose(&mut rng) else {
                    continue
                };
                *peer
            };
            let hash_str = hash.to_string();
            let Ok(permit) = self.upload_permit.clone().try_acquire_owned() else {
                warn!(to = %peer, hash = %hash_str,
                    "Could not get an upload permit for transfer."
                );
                break
            };
            let now = Instant::now();
            let failed: HashMap<PeerId, Vec<Instant>> = match chunk {
                LocalChunk::AwaitingAssignment => HashMap::new(),
                LocalChunk::Assigned { failed, .. } => failed.clone(),
                _ => continue
            };
            let chunk_path = path.as_path().join(&hash_str);
            let context = Some(PushContext { to: peer, created_at: now });
            *chunk = LocalChunk::Assigned { context, failed };
            let tx_internal = self.tx_internal.clone();
            let control = self.blob_transfer_control.clone();
            let tx_events = self.tx_blob_transfer_events.clone();
            tokio::spawn(async move {
                debug!(chunk = %hash_str, to = %peer, "Upload is initiated.");
                let upload_result = timeout(
                    Duration::from_secs(TIMEOUT_WINDOW),
                    blob_transfer::push(control, peer, hash, &chunk_path, tx_events)
                ).await;
                drop(permit);
                match upload_result {
                    Ok(Ok(_)) => {
                        debug!(chunk = %hash_str, to = %peer, dur = now.elapsed().as_secs_f32(),
                            "Upload finished successfully."
                        );
                        let _ = tx_internal.send(InternalMessage::ChunkUploadResult {
                            id: hash, to: peer, succeeded: true
                        });
                    }
                    Ok(Err(e)) => {
                        warn!(chunk = %hash_str, to = %peer, error = %e,
                            dur = now.elapsed().as_secs_f32(), "Upload failed."
                        );
                        let _ = tx_internal.send(InternalMessage::ChunkUploadResult {
                            id: hash, to: peer, succeeded: false
                        });
                    }
                    Err(_) => {
                        let _ = tx_internal.send(InternalMessage::ChunkUploadResult {
                            id: hash, to: peer, succeeded: false
                        });
                        warn!(chunk = %hash_str, to = %peer, dur = now.elapsed().as_secs_f32(),
                            "Upload has timed out."
                        );
                    }
                };
            });
        }
    }

    async fn handle_manifest(
        &mut self,
        id: String,
        manifest: Manifest
    ) {
        let Manifest::Blob { root_hash, chunks: chunk_owners } = manifest;
        let Some((remote_id, remote_blob)) = self.remote_blob.as_mut() else {
            debug!(blob = %id, "Unsolicited blob manifest.");
            return
        };
        if *remote_id != id {
            debug!(expected = %remote_id, given = %id, "Blob manifest Ids mismatch.");
            return
        }
        let now = Instant::now();
        match remote_blob {
            RemoteBlob::AwaitingManifest(_) => {
                // todo: validate root hash
                let path = Path::new(BASE_BLOB_PATH).join(&id);
                if let Err(e) = tokio::fs::create_dir_all(&path).await {
                    warn!(blob = %id, error = %e, path = %path.display(),
                        "Failed to create the remote blob folder."
                    );
                    *remote_blob = RemoteBlob::Failed;
                    return
                }
                let mut chunks = HashMap::new();
                for (plain_hash, peer) in chunk_owners.iter() {
                    let hash = Hash::from(*plain_hash);
                    let hash_str = hash.to_string();
                    let mut requests = vec![];
                    if let Some(req_id) = request_pull(
                        &hash_str,
                        *peer,
                        self.tx_swarm.clone()
                    ).await {
                        requests.push((req_id, now));
                    }
                    chunks.insert(
                        hash,
                        RemoteChunk::Pulling {
                            requests: [(*peer, requests)].into(),
                            context: None,
                            failed: HashMap::new()
                        }
                    );
                }
                *remote_blob = RemoteBlob::Gathering {
                    root_hash: root_hash.into(),
                    merkle_leaves: chunk_owners
                        .into_iter()
                        .map(|(h, _)| h.into())
                        .collect(),
                    path,
                    chunks
                };
            }
            RemoteBlob::Failed => {
                // todo
            }
            _ => ()
        };
    }

    async fn handle_pull_chunk_ack(
        &mut self,
        id: String,
        peer: PeerId,
        req_id: OutboundRequestId
    ) {
        let Some((_, RemoteBlob::Gathering { chunks, .. })) = &mut self.remote_blob else {
            debug!(chunk = %id, "Unsolicited pull chunk ACK.");
            return
        };
        let Ok(hash) = Hash::from_hex(id.as_bytes()) else {
            debug!(chunk = %id, "Pull chunk ACK hash decode failed.");
            return
        };
        let Some(chunk) = chunks.get_mut(&hash) else {
            debug!(chunk = %id, "Unsolicited pull chunk ACK.");
            return
        };
        match chunk {
            RemoteChunk::Pulling { requests, context, failed } => {
                if failed.contains_key(&peer) {
                    // todo: retry
                    return
                }
                if let Some(PullContext { from: ex_peer, .. }) = context {
                    if *ex_peer == peer {
                        debug!(chunk = %id, from = %peer, "Pull already underway.");
                        return
                    }
                }
                let Some(reqs) = requests.get(&peer) else {
                    debug!(chunk = %id, from = %peer, "Unsolicited pull chunk ACK.");
                    return
                };
                if !reqs.iter().any(|(ex_req_id, _)| *ex_req_id == req_id) {
                    debug!(chunk = %id, from = %peer, request_id = %req_id,
                        "Pull chunk ACK request Ids mismatch."
                    );
                    return
                }
                let Ok(_permit) = self.download_permit.clone().try_acquire_owned() else {
                    debug!(chunk = %id, from = %peer, "Could not acquire a download permit.");
                    // todo: retry mechanism
                    return
                };
                *context = Some(PullContext { from: peer, created_at: Instant::now(), _permit });
            }
            RemoteChunk::Failed => {
                // todo
            }
            _ => ()
        }
    }

    async fn maintain_local_blob(&mut self) {
        let Some((_, local_blob)) = &mut self.local_blob else {
            return
        };
        let now = Instant::now();
        match local_blob {
            LocalBlob::Distributing { chunks, .. } => {
                for (_, chunk) in chunks.iter_mut() {
                    match chunk {
                        LocalChunk::Assigned { context, failed, .. } => {
                            if let Some(PushContext { to, created_at }) = context {
                                if now.duration_since(*created_at).as_secs() > TIMEOUT_WINDOW {
                                    failed.entry(*to).or_default().push(now);
                                    *context = None;
                                }
                            }
                        }
                        _ => ()
                    };
                }
                // todo: need blob's entry time to decide if it should be marked as failed
                if self.upload_permit.available_permits() == 0usize {
                    return
                }
                if chunks.iter().any(|(_, chunk)|
                    matches!(chunk, LocalChunk::AwaitingAssignment | LocalChunk::Assigned { .. })
                ) {
                    self.assign_chunks();
                }
            }
            LocalBlob::AwaitingPublishManifest { attempts, .. } => {
                if !attempts.is_empty()
                    && now
                        .duration_since(attempts.first().unwrap().1)
                        .as_secs()
                        > BLOB_STALE_DEADLINE
                {
                    *local_blob = LocalBlob::Failed;
                    self.archive_local_blob().await;
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
        let Some((id, remote_blob)) = self.remote_blob.as_mut() else {
            return
        };
        let now = Instant::now();
        match remote_blob {
            RemoteBlob::AwaitingManifest(attempts) => {
                if !attempts.is_empty() {
                    if now
                        .duration_since(*attempts.first().unwrap())
                        .as_secs()
                        > BLOB_STALE_DEADLINE
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
                        RemoteChunk::Pulling { requests, context, failed } => {
                            // the active pull may fail so advance other pull requests as well
                            let failed_reqs = requests.extract_if(|_, reqs| {
                                if reqs.is_empty() {
                                    return false
                                }
                                let (_, first_try) = reqs.first().unwrap();
                                reqs.len() >= MAX_RETRIES
                                || now.duration_since(*first_try).as_secs() > CHUNK_STALE_DEADLINE
                            });
                            for (peer, reqs) in failed_reqs.into_iter() {
                                let attempts: Vec<_> = reqs.into_iter().map(|(_, t)| t).collect();
                                failed
                                    .entry(peer)
                                    .or_default()
                                    .extend_from_slice(attempts.as_slice());
                            }
                            for (peer, reqs) in requests.iter_mut() {
                                if !reqs.is_empty() {
                                    let (_, last_try) = reqs.last().unwrap();
                                    if now.duration_since(*last_try).as_secs() < TIMEOUT_WINDOW {
                                        // nothing to be done
                                        continue
                                    }
                                }
                                let Some(req_id) = request_pull(
                                    &hash_str, *peer, self.tx_swarm.clone()
                                ).await else {
                                    continue
                                };
                                reqs.push((req_id, now));
                                debug!(chunk = %hash_str, remaining = MAX_RETRIES - reqs.len(),
                                    "Sent another push request."
                                );
                            }
                            if let Some(PullContext { from, created_at, .. }) = context {
                                if now.duration_since(*created_at).as_secs() > TIMEOUT_WINDOW {
                                    failed.entry(*from).or_default().push(Instant::now());
                                    *context = None;
                                }
                            }
                            if requests.is_empty() && !failed.is_empty() {
                                if failed
                                    .iter()
                                    .all(|(_, attempts)| {
                                        let first_try = *attempts.first().unwrap();
                                        attempts.len() >= MAX_RETRIES
                                        || now.duration_since(first_try).as_secs()
                                            > CHUNK_STALE_DEADLINE
                                    })
                                {
                                    *chunk = RemoteChunk::Failed;
                                    // todo: retry
                                }
                            }
                        }
                        RemoteChunk::Failed => {
                            // todo
                        }
                        _ => (),
                    };
                }
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

    async fn archive_remote_blob(&mut self) {
        let Some((id, remote_blob)) = self.remote_blob.take() else {
            return
        };
        // todo: archival
    }

    async fn handle_incoming_push(
        &mut self,
        in_push: blob_transfer::IncomingPush
    ) {
        let Some((_, RemoteBlob::Gathering { chunks, path, .. })) = self.remote_blob.as_mut() else {
            return
        };
        match in_push {
            blob_transfer::IncomingPush::Request { peer, hash, len, reply_tx } => {
                let chunk_path = chunks
                    .get(&hash)
                    .and_then(|chunk| match chunk {
                        RemoteChunk::Pulling { context, .. } => {
                            context
                                .as_ref()
                                .and_then(|pull_context|
                                    if pull_context.from == peer {
                                        Some(path.as_path().join(hash.to_string()))
                                    } else {
                                        None
                                    }
                                )
                        }
                        _ => None
                    });
                    if chunk_path.is_none() {
                        debug!(hash = %hash.to_hex(), from = %peer, len = %len,
                            "Unsolicited incoming push."
                        );
                    }
                    let _ = reply_tx.send(chunk_path);
            }
            blob_transfer::IncomingPush::Finished { hash, succeeded } => {
                let Some(chunk) = chunks.get_mut(&hash) else {
                    debug!(hash = %hash.to_hex(), "Unsolicited incoming push result.");
                    return
                };
                let RemoteChunk::Pulling { requests, context, failed } = chunk else {
                    debug!(hash = %hash.to_hex(), "Unsolicited incoming push result: not pulling.");
                    return
                };
                let Some(PullContext { from, .. }) = context else {
                    debug!(hash = %hash.to_hex(), "Unsolicited incoming push result: no context.");
                    return
                };
                if succeeded {
                    *chunk = RemoteChunk::Pulled(*from);
                    if chunks.values().all(|c| matches!(c, RemoteChunk::Pulled(_))) {
                        self.archive_remote_blob().await;
                    }
                } else {
                    let _ = requests.remove(from);
                    failed.entry(*from).or_default().push(Instant::now());
                    *context = None;
                }
            }
        };
    }

    fn handle_incoming_pull(
        &self,
        in_pull: blob_transfer::IncomingPull
    ) {
        // todo: check the db too
        let Some((id, LocalBlob::Distributing { chunks, path, .. })) = self.local_blob.as_ref() else {
            return
        };
        let pull_permit = if chunks.contains_key(&in_pull.hash) {
            self
                .upload_permit
                .clone()
                .try_acquire_owned()
                .ok()
                .map(|permit| {
                    let chunk_path = path.as_path().join(&in_pull.hash.to_string());
                    blob_transfer::PullPermit { path: chunk_path, _permit: permit }
                })
        } else {
            debug!(hash = %in_pull.hash.to_hex(), from = %in_pull.peer,
                "Unsolicited incoming pull."
            );
            None
        };
        in_pull.reply(pull_permit);
    }
}

pub async fn run(
    db_pool: PgPool,
    mut rx_admission: mpsc::Receiver<ToPipeline>,
    mut rx_handler: mpsc::Receiver<FromSwarm>,
    tx_swarm: mpsc::Sender<ToSwarm>,
    mut blob_transfer_control: peyk::Control,
    shutdown: CancellationToken
) -> Result<tokio::task::JoinHandle<()>> {
    //  setup blob transfer
    let mut incoming_pushes = blob_transfer::accept_pushes(
        blob_transfer_control.accept(blob_transfer::PUSH_PROTOCOL)?,
        CHUNK_SIZE,
    );
    let mut incoming_pulls = blob_transfer::accept_pulls(
        blob_transfer_control.accept(blob_transfer::PULL_PROTOCOL)?
    );
    let (tx_blob_transfer_events, mut rx_blob_transfer_event) =
        mpsc::unbounded_channel::<blob_transfer::TransferEvent>();
    let (tx_internal, mut rx_internal) = mpsc::unbounded_channel::<InternalMessage>();
    let mut pipeline = Pipeline::new(
        db_pool,
        tx_internal,
        tx_swarm,
        blob_transfer_control,
        tx_blob_transfer_events
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
                        FromSwarm::Response { peer, request_id, response } => {
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
                                peyk::protocol::Response::PullChunkAck(id) => {
                                    pipeline.handle_pull_chunk_ack(id, peer, request_id).await;
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
                // internal messages
                im = rx_internal.recv() => match im {
                    Some(i_msg) => match i_msg {
                        InternalMessage::ChunkUploadResult { id, to: peer, succeeded } => {
                            let hash_str = id.to_string();
                            debug!(chunk = %hash_str, to = %peer, succeeded = %succeeded,
                                "New upload result."
                            );
                            let Some(
                                (
                                    blob_id,
                                    LocalBlob::Distributing { root_hash, merkle_leaves, chunks, .. }
                                )
                            ) = &mut pipeline.local_blob else {
                                debug!("The local blob is not `distributing`.");
                                continue
                            };
                            let Some(chunk) = chunks.get_mut(&id) else {
                                warn!("Missing chunk.");
                                continue
                            };
                            match chunk {
                                LocalChunk::Assigned { context, failed, .. } => {
                                    if succeeded {
                                        *chunk = LocalChunk::Pushed(peer);
                                        if chunks
                                            .values()
                                            .all(|chunk| matches!(chunk, LocalChunk::Pushed(_)))
                                        {
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
                                            pipeline.local_blob = Some((
                                                blob_id.clone(),
                                                LocalBlob::AwaitingPublishManifest {
                                                    manifest,
                                                    attempts: vec![],
                                                    failed: 0
                                                }
                                            ));
                                            pipeline.publish_manifest().await;
                                        }
                                    } else {
                                        failed.entry(peer).or_default().push(Instant::now());
                                        *context = None;
                                    }
                                }
                                _ => {
                                    debug!(chunk = %hash_str, "Unsolicited upload result.");
                                }
                            }
                        }
                    }
                    None => {
                        warn!("Internal channel is closed.");
                        break
                    }
                },
                // <blob transfer>
                // pushes
                p = incoming_pushes.recv() => match p {
                    Some(push_req) => {
                        pipeline.handle_incoming_push(push_req).await;
                    },
                    None => {
                        break
                    }
                },
                // pulls
                p = incoming_pulls.recv() => match p {
                    Some(pull_req) => {
                        pipeline.handle_incoming_pull(pull_req);
                    },
                    None => {
                        break
                    }
                },
                // events
                e = rx_blob_transfer_event.recv() => match e {
                    Some(a) => {
                        debug!(event = ?a);
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
