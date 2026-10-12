use std::{
    collections::{VecDeque, HashMap},
    sync::Arc,
};
use eyre::Result;
use tracing::{info, warn, debug};
use tokio::{
    sync::{mpsc, Semaphore, OwnedSemaphorePermit},
    time::{Instant, Duration, timeout, interval},
};
use tokio_util::sync::CancellationToken;
use bytes::Bytes;
use blake3::Hash;
use libp2p::PeerId;
use rs_merkle::MerkleTree;
use crate::admission::{
    Status as BridgeStatus,
    ToPipeline, ToBridge, AdmittedBlob, CHUNK_SIZE
};
use crate::blake3_wrapper::Blake3;
use peyk::{blob_transfer, HandlerMessage, SwarmMessage};

// 30 seconds
const CHUNK_UPLOAD_WINDOW: u64 = 30;
// provider hints are valid for 5 minutes
const STORAGE_PROVIDER_DECAY: u64 = 5 * 60;

enum InternalMessage {
    UpdateChunkStatus {
        hash: Hash,
        status: ChunkUploadStatus,
        at: Instant
    }
}

#[derive(Debug, Clone)]
enum ChunkUploadStatus {
        Pending,
    Inflight {
        to: PeerId,
    },
    Finalized {
        owner: PeerId
    }
}

struct TimestampedStatus {
    at: Instant,
    status: ChunkUploadStatus
}

struct Chunk {
    data: Bytes,
    status: TimestampedStatus
}

enum Blob {
    LocalBlob {
        id: String,
        root_hash: Hash,
        merkle_tree: MerkleTree<Blake3>,
        chunks: HashMap<Hash, Chunk>,
        allowance: OwnedSemaphorePermit,
        created_at: Instant,
    },
    RemoteBlob {
        id: String,
        // pull_status: Vec<(DownloadStatus, Instant)>,
        expected_chunk_hashes: Vec<Hash>,
        chunks: HashMap<Hash, Bytes>,
    }
}

struct Pipeline {
    pending_blobs: VecDeque<Blob>,
    current_blob: Option<Blob>,

    // record decaying WouldStore gossips
    storage_provider_hints: HashMap<PeerId, Instant>,
    storage_permits: HashMap<PeerId, Instant>,

    tx_internal: mpsc::UnboundedSender<InternalMessage>,

    tx_swarm: mpsc::Sender<SwarmMessage>,

    blob_transfer_control: peyk::Control,
    tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>,

    // to gate i/o usage
    download_allowance: Arc<Semaphore>,
    upload_allowance: Arc<Semaphore>
}

impl Pipeline {
    fn new(
        tx_internal: mpsc::UnboundedSender<InternalMessage>,
        tx_swarm: mpsc::Sender<SwarmMessage>,
        blob_transfer_control: peyk::Control,
        tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>
    ) -> Self {
        Pipeline {
            pending_blobs: VecDeque::new(),
            current_blob: None,
            storage_provider_hints: HashMap::new(),
            storage_permits: HashMap::new(),
            tx_internal,
            tx_swarm,
            blob_transfer_control,
            tx_blob_transfer_events,
            download_allowance: Arc::new(Semaphore::new(32)), // 32 * 4 = 128 MiB of downloads
            upload_allowance: Arc::new(Semaphore::new(20)),   // 20 * 4 =  80 MiB of uploads
        }
    }

    fn add_local_blob(
        &mut self,
        admitted_blob: AdmittedBlob
    ) {
        let chunks_map: HashMap<Hash, Chunk> = admitted_blob.chunks
            .into_iter()
            .map(|(hash, data)| (hash, Chunk {
                data,
                status: TimestampedStatus {
                    status: ChunkUploadStatus::Pending,
                    at: Instant::now()
                }
            }))
            .collect();
        self.pending_blobs.push_back(
            Blob::LocalBlob {
                id: admitted_blob.id,
                root_hash: admitted_blob.root_hash,
                merkle_tree: admitted_blob.merkle_tree,
                chunks: chunks_map,
                allowance: admitted_blob.allowance,
                created_at: Instant::now()
            }
        );
    }

    fn archive_cur_blob(&mut self) -> Result<()> {
        let _blob = self.current_blob.take().unwrap();
        // todo: archive the blob
        Ok(())
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
        let _ = self.tx_swarm.send(SwarmMessage::RequestStoragePermits {
            peers: self.storage_provider_hints.keys().cloned().collect()
        }).await;
    }

    async fn begin_next_blob(&mut self) {
        if self.current_blob.is_some() {
            return
        }
        self.current_blob = self.pending_blobs.pop_front();
        if let Some(blob) = self.current_blob.as_ref() {
            if let Blob::LocalBlob { id, .. } = blob {
                info!("Started to distribute a new local blob(`{}`).", id);
                self.request_storage_permits().await;
            }
        } else {
            info!("All blobs are caught up. Waiting for the next...");
        }
    }

    fn assign_chunks(&mut self) {
        let num_available_permits = self.upload_allowance.available_permits();
        if num_available_permits == 0 {
            debug!("No upload allowance for now, cannot proceed with chunk assignment.");
            return
        }
        let Some(blob) = &mut self.current_blob else {
            debug!("The current blob is invalid so chunk assignment won't proceed.");
            return
        };
        let Blob::LocalBlob { chunks, .. } = blob else {
            debug!("Chunk assignment does not apply here as the current blob is of `remote` kind.");
            return
        };
        let upload_onset = Instant::now();
        let peers: Vec<PeerId> = self.storage_permits
            .iter()
            .filter_map(|(peer, expires_at)| if *expires_at > upload_onset { Some(peer) } else { None })
            .cloned()
            .collect();
        if peers.is_empty() {
            warn!("No eligible peers to assign.");
            return
        }
        let eligible = |c: &Chunk| match c.status.status {
            ChunkUploadStatus::Pending => true,
            ChunkUploadStatus::Inflight { .. }  => upload_onset.duration_since(c.status.at).as_secs() > CHUNK_UPLOAD_WINDOW,
            ChunkUploadStatus::Finalized { .. } => false
        };
        let num_peers = peers.len();
        let mut peer_index = 0;
        for (&hash, chunk) in chunks.iter_mut().filter(|(_, c)| eligible(c)).take(num_available_permits) {
            let peer = peers[peer_index];
            let Ok(permit) = self.upload_allowance.clone().try_acquire_owned() else {
                warn!(%peer, hash = %hash.to_hex(), "Could not get an upload permit for transfer.");
                break
            };
            chunk.status = TimestampedStatus {
                status: ChunkUploadStatus::Inflight { to: peer },
                at: upload_onset
            };
            let control = self.blob_transfer_control.clone();
            let tx_events = self.tx_blob_transfer_events.clone();
            // self send for maintenance and state propagation
            let tx_internal = self.tx_internal.clone();
            let payload = chunk.data.clone();
            tokio::spawn(async move {
                let hash_str = hash.to_hex();
                debug!(chunk = %hash_str, %peer, payload_mib = payload.len() as f32 / 1_048_576f32, "upload is initiated.");
                let upload_result = timeout(
                    Duration::from_secs(CHUNK_UPLOAD_WINDOW),
                    blob_transfer::push(control, peer, hash, payload, tx_events)
                ).await;
                drop(permit);
                // mark the stalled peer as faulty and do not match again
                match upload_result {
                    Ok(Ok(_)) => {
                        debug!(chunk = %hash_str, %peer, dur = upload_onset.elapsed().as_secs_f32(), "upload finished successfully.");
                        let _ = tx_internal.send(InternalMessage::UpdateChunkStatus {
                            hash,
                            status: ChunkUploadStatus::Finalized { owner: peer },
                            at: Instant::now(),
                        });
                    }
                    Ok(Err(e)) => {
                        warn!(chunk = %hash_str, %peer,
                            error = %e, dur = upload_onset.elapsed().as_secs_f32(),
                            "upload failed."
                        );
                        let _ = tx_internal.send(InternalMessage::UpdateChunkStatus {
                            hash,
                            status: ChunkUploadStatus::Pending,
                            at: Instant::now(),
                        });
                    }
                    Err(_) => {
                        warn!(chunk = %hash_str, %peer,
                            dur = upload_onset.elapsed().as_secs_f32(),
                            "upload has timed out."
                        );
                    }
                };
            });
            peer_index = (peer_index + 1) % num_peers;
        }
    }
}

pub async fn run(
    mut rx_admission: mpsc::Receiver<ToPipeline>,
    mut rx_handler: mpsc::Receiver<HandlerMessage>,
    tx_swarm: mpsc::Sender<SwarmMessage>,
    tx_bridge: mpsc::Sender<ToBridge>,
    mut blob_transfer_control: peyk::Control,
    shutdown: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    //  setup blob transfer
    let mut incoming_pushes = blob_transfer::accept_pushes(
        blob_transfer_control.accept(blob_transfer::PUSH_PROTOCOL)?,
        CHUNK_SIZE
    );
    let mut incoming_pulls = blob_transfer::accept_pulls(
        blob_transfer_control.accept(blob_transfer::PULL_PROTOCOL)?
    );
    let (tx_blob_transfer_events, mut rx_blob_transfer_event) = 
        mpsc::unbounded_channel::<blob_transfer::TransferEvent>();
    let (tx_internal, mut rx_internal) = mpsc::unbounded_channel::<InternalMessage>();
    let mut pipeline = Pipeline::new(
        tx_internal,
        tx_swarm,
        blob_transfer_control,
        tx_blob_transfer_events
    );
    let mut timer_stale = interval(Duration::from_secs(60));
    let mut timer_assign = interval(Duration::from_secs(30));
    let jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Received shutdown request.");
                    break
                },

                _i = timer_stale.tick() => {
                    pipeline
                        .storage_provider_hints
                        .retain(|_, created_at| {
                            Instant::now().duration_since(*created_at).as_secs() < STORAGE_PROVIDER_DECAY
                        });
                    if pipeline.current_blob.is_some() {
                        pipeline.request_storage_permits().await;
                    }
                },

                _i = timer_assign.tick() => {
                    pipeline.assign_chunks();
                },
                // swarm handlers
                hm = rx_handler.recv() => match hm {
                    Some(h_msg) => {
                        match h_msg {
                            // a gossip by storage providers
                            HandlerMessage::WouldStore {
                                peer_id,
                            } => {
                                pipeline
                                    .storage_provider_hints
                                    .insert(peer_id, Instant::now());
                            }
                            HandlerMessage::Request {
                                peer_id,
                                request_id,
                                request,
                                channel
                            } => {
                            }
                            HandlerMessage::Response {
                                peer_id,
                                request_id,
                                response
                            } => {
                                match response {
                                    peyk::protocol::Response::IssuedStoragePermit { valid_for } => {
                                        let now = Instant::now();
                                        pipeline.storage_permits.insert(
                                            peer_id,
                                            now.checked_add(Duration::from_secs(valid_for))
                                            .unwrap_or(now)
                                        );
                                        // todo: storage permit is already invalid in case of overflow
                                        pipeline.assign_chunks();
                                    }
                                    peyk::protocol::Response::BlobMeta {..} => {}
                                }
                            }
                        }
                    }
                    None => {
                        warn!("Swarm handler channel is closed.");
                        break
                    }
                },
                // messages
                m = rx_admission.recv() => match m {
                    Some(msg) => match msg {
                        // remote blob: download chunks and assemble
                        ToPipeline::GetBlob { id } => {
                            if let Some(Blob::LocalBlob { .. }) = pipeline.current_blob {
                                // todo
                            } else {
                                info!("Blob(`{}`) is missing, scheduled for gathering from remote nodes.", id);
                                pipeline.pending_blobs.push_back(
                                    Blob::RemoteBlob {
                                        id,
                                        // pull_status: vec![(Status::AwaitingMetadata, Instant::now())],
                                        expected_chunk_hashes: vec![],
                                        chunks: HashMap::new(),
                                    }
                                );
                            }
                        }
                        // local blob: chunk and distribute
                        ToPipeline::NewBlob(blob) => {
                            pipeline.add_local_blob(blob);
                            pipeline.begin_next_blob().await;
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
                        InternalMessage::UpdateChunkStatus {
                            hash,
                            status,
                            ..
                        } => {
                            let hash_str = hash.to_hex().to_string();
                            debug!("A new chunk(`{}`) status update(`{:?}`)", hash_str, status);
                            let Some(blob) = &mut pipeline.current_blob else {
                                debug!("No current blob.");
                                continue
                            };
                            let Blob::LocalBlob { id, chunks, .. } = blob else {
                                debug!("The current blob is of `remote` kind.");
                                continue
                            };
                            let Some(chunk) = chunks.get_mut(&hash) else {
                                warn!("Chunk is missing.");
                                continue
                            };
                            // todo: keep track of previous updates and test for healthy transition
                            chunk.status = TimestampedStatus {
                                at: Instant::now(),
                                status: status.clone()
                            };
                            match status {
                                ChunkUploadStatus::Pending => {},
                                ChunkUploadStatus::Inflight { to } => {
                                    debug!("Chunk is being sent to Peer(`{}`).", to);
                                },
                                ChunkUploadStatus::Finalized { owner } => {
                                    debug!("Chunk has been successfully uploaded to peer(`{}`).", owner);
                                    let is_finalized = chunks
                                        .values()
                                        .all(|chunk| matches!(chunk.status.status, ChunkUploadStatus::Finalized { .. }));
                                    if is_finalized {
                                        info!("Blob(`{}`) is now stored globally.", id);
                                        if let Err(e) = tx_bridge.send(ToBridge::UpdateStatus {
                                            id: id.to_string(),
                                            status: BridgeStatus::Finalized
                                        }).await {
                                            warn!("Failed to send the Finalized status to the bridge channel: {e:?}");
                                        }
                                        let _ = pipeline.archive_cur_blob();
                                        pipeline.begin_next_blob().await;
                                    }
                                    pipeline.assign_chunks();
                                }
                                // todo: when to notify it about failure?
                            };

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
                    Some(push) => {
                        // push.peer push.hash, push.data
                    },
                    None => {
                        break
                    }
                },
                // pulls
                p = incoming_pulls.recv() => match p {
                    Some(push) => {
                        // g.respond(store.get(&g.hash).await);
                    },
                    None => {
                        break
                    }
                },
                // events
                e = rx_blob_transfer_event.recv() => match e {
                    Some(a) => {
                        info!("{:?}", a);
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

#[allow(unused)]
async fn dump_blob_to_disk(
    id: &str,
    data: Bytes
) -> Result<()> {
    const BASE_PATH: &str = "./blobs";
    let blob_path = format!("{BASE_PATH}/{id}");
    info!("Archiving blob(`{}`) to `{}`", id, blob_path);
    tokio::fs::create_dir_all(BASE_PATH).await?;
    tokio::fs::write(&blob_path, data.as_ref()).await?;
    Ok(())
}