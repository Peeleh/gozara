use std::{
    collections::{VecDeque, HashMap},
    sync::Arc,
};
use eyre::{eyre, Result};
use tracing::{info, warn, trace};
use tokio::{
    sync::{mpsc, oneshot, Semaphore, OwnedSemaphorePermit},
    time::{Instant, Duration, timeout, interval},
    task,
};
use tokio_util::sync::CancellationToken;
use bytes::Bytes;
use libp2p::PeerId;
use rs_merkle::MerkleTree;
use crate::bridge::{
    Message as BridgeMessage,
    Status as BridgeStatus
};
use crate::blake3_wrapper::Blake3Hash;
use peyk::{blob_transfer, HandlerMessage, SwarmMessage};

pub type Hash = [u8; 32];

// 4 MB
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// blob lifetime: 4 hours
const BLOB_LIFETIME: u64 = 4 * 60 * 60;

// max incoming(network) blob size 4 MiB
const MAX_NETWORK_BLOB_SIZE: usize = 4 * 1024 * 1024;
// 30 seconds
const CHUNK_UPLOAD_WINDOW: u64 = 30;
// provider hints are valid for 5 minutes
const STORAGE_PROVIDER_DECAY: u64 = 5 * 60;

pub enum Message {
    // src: the bridge
    // to gather blob from remote nodes
    GetBlob {
        id: String,
    },
    // src: the bridge
    NewBlob {
        id: String,
        data: Bytes,
    }
}

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
        data: Bytes,
        chunks: HashMap<Hash, Chunk>,
        merkle_tree: MerkleTree::<Blake3Hash>,
        created_at: Instant,
    },
    RemoteBlob {
        id: String,
        // pull_status: Vec<(DownloadStatus, Instant)>,
        expected_chunk_hashes: Vec<Hash>,
        chunks: HashMap<Hash, Bytes>,
    }
}

impl Blob {
    async fn archive(&self) -> Result<()> {
        // todo
        Ok(())
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

    blob_transfer_control: libp2p_stream::Control,
    tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>,

    // to gate i/o usage
    download_allowance: Arc<Semaphore>,
    upload_allowance: Arc<Semaphore>
}

impl Pipeline {
    pub fn new(
        tx_internal: mpsc::UnboundedSender<InternalMessage>,
        tx_swarm: mpsc::Sender<SwarmMessage>,
        blob_transfer_control: libp2p_stream::Control,
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

    async fn add_local_blob(
        &mut self,
        id: String,
        data: Bytes,
    ) -> Result<()> {
        if data.is_empty() {
            return Err(eyre!("Empty blob."))
        }
        // todo: also check with the archived blobs
        if self.pending_blobs
            .iter()
            .any(|blob| match blob {
                Blob::LocalBlob { id: ex_id, .. } => *ex_id == id,
                Blob::RemoteBlob { id: ex_id, .. } => *ex_id == id,
            })
        {
            return Err(eyre!("Duplicate blob: {}", id))
        }
        let is_duplicate = match self.current_blob.as_ref() {
            Some(blob) => match blob {
                Blob::LocalBlob { id: ex_id, .. } => *ex_id == id,
                Blob::RemoteBlob { id: ex_id, .. } => *ex_id == id,
            }
            None => false
        };
        if is_duplicate {
            return Err(eyre!("Duplicate blob: {}", id))
        }

        let cloned_data = data.clone();
        let (chunks, merkle_tree) = task::spawn_blocking(move || {
            let chunks: Vec<(Hash, Chunk)> = cloned_data
                .chunks(CHUNK_SIZE)
                .map(|c| (
                    blake3::hash(c).into(),
                    Chunk {
                        data: cloned_data.slice_ref(c),
                        status: TimestampedStatus {
                            at: Instant::now(),
                            status: ChunkUploadStatus::Pending,
                        }
                    }
                )).collect();            
            let chunk_hashes = chunks
                .iter()
                .map(|(hash, _)| *hash)
                .collect::<Vec<Hash>>();
            let merkle_tree = MerkleTree::<Blake3Hash>::from_leaves(&chunk_hashes);

            (chunks, merkle_tree)
        }).await?;
        let root_hash = merkle_tree
            .root()
            .ok_or_else(|| eyre!("Couldn't get the merkle root."))?;
        info!(
            "Blob(`{}`) is chunked and Merklized with root hash(`{}`). We'll now try to distribute it.",
            id, hex::encode(root_hash)
        );
        self.pending_blobs.push_back(
            Blob::LocalBlob {
                id,
                root_hash,
                data,
                chunks: chunks.into_iter().collect(),
                merkle_tree,
                created_at: Instant::now()
            }
        );
        Ok(())
    }

    fn archive_cur_blob(&mut self) -> Result<()> {
        let _blob = self.current_blob.take().unwrap();
        // todo: archive the blob
        Ok(())
    }

    // periodic cleanup
    fn remove_stale_blobs(
        &mut self
    ) {
        // let now = Instant::now();
        // self.blobs.retain(|_, blob| {
        //     match blob {
        //         Blob::LocalBlob { created_at, .. } => {
        //             now.duration_since(*created_at).as_secs() < BLOB_LIFETIME
        //         }
        //         Blob::RemoteBlob { .. } => true
        //     }
        // });
        // todo: inform the bridge?
    }

    pub async fn request_storage_permits(&mut self) {
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

    pub async fn begin_next_blob(&mut self) {
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

    pub async fn assign_chunks(&mut self) {
        let num_available_permits = self.upload_allowance.available_permits();
        if num_available_permits == 0 {
            trace!("No upload allowance for now, cannot proceed with chunk assignment.");
            return
        }
        let Some(blob) = &mut self.current_blob else {
            trace!("The current blob is invalid so chunk assignment won't proceed.");
            return
        };
        let Blob::LocalBlob { chunks, .. } = blob else {
            trace!("Chunk assignment does not apply here as the current blob is of `remote` kind.");
            return
        };
        let now = Instant::now();
        let peers: Vec<PeerId> = self.storage_permits
            .iter()
            .filter_map(|(peer, expires_at)| if *expires_at > now { Some(peer) } else { None })
            .cloned()
            .collect();
        if peers.is_empty() {
            warn!("No eligible peers to assign.");
            return
        }
        let chosen_chunks: Vec<Hash> = chunks
            .iter()
            .filter_map(|(hash, chunk)| {
                match chunk.status.status {
                    ChunkUploadStatus::Pending => Some(hash),
                    ChunkUploadStatus::Inflight { .. }  => {
                        // timed out
                        if now.duration_since(chunk.status.at).as_secs() > CHUNK_UPLOAD_WINDOW {
                            Some(hash)
                        } else {
                            None
                        }
                    },
                    ChunkUploadStatus::Finalized { .. } => None
                }
            })
            .take(num_available_permits)
            .cloned()
            .collect();
        let num_peers = peers.len();
        let mut assignments = HashMap::<PeerId, Vec<(Hash, Bytes, OwnedSemaphorePermit, Instant)>>::new();
        let mut peer_index = 0;
        for hash in chosen_chunks.into_iter() {
            let peer = peers[peer_index];
            let Ok(permit) = self.upload_allowance.clone().try_acquire_owned() else {
                warn!(%peer, hash = %hex::encode(hash), "Could not get an upload permit for transfer.");
                break
            };
            assignments.entry(peer).or_default().push((
                hash,
                chunks.get(&hash).unwrap().data.clone(),
                permit,
                now
            ));
            chunks.get_mut(&hash).unwrap().status = TimestampedStatus {
                status: ChunkUploadStatus::Inflight { to: peer },
                at: now
            };
            peer_index = (peer_index + 1) % num_peers;
        }
        // upload chunks
        for (peer, batch) in assignments.into_iter() {
            for (hash, data, permit, upload_onset) in batch.into_iter() {
                let control = self.blob_transfer_control.clone();
                let tx_events = self.tx_blob_transfer_events.clone();
                // self send for maintenance and state propagation
                let tx_internal = self.tx_internal.clone();
                tokio::spawn(async move {
                    let hash_str = hex::encode(hash);
                    info!(chunk = %hash_str, %peer, length = data.len() as f32 / 1_048_576f32, "upload is initiated");
                    let upload_result = timeout(
                        Duration::from_secs(CHUNK_UPLOAD_WINDOW),
                        blob_transfer::push(control, peer, hash, data, tx_events)
                    ).await;
                    drop(permit);
                    match upload_result {
                        Ok(Ok(_)) => {
                            info!(chunk = %hash_str, %peer,
                                dur = upload_onset.elapsed().as_secs_f32(),
                                "upload finished successfully."
                            );
                            let _ = tx_internal.send(InternalMessage::UpdateChunkStatus {
                                hash,
                                status: ChunkUploadStatus::Finalized { owner: peer },
                                at: Instant::now(),
                            });
                        }
                        Ok(Err(e)) => {
                            info!(chunk = %hash_str, %peer,
                                dur = upload_onset.elapsed().as_secs_f32(),
                                "upload failed."
                            );
                            let _ = tx_internal.send(InternalMessage::UpdateChunkStatus {
                                hash,
                                status: ChunkUploadStatus::Pending,
                                at: Instant::now(),
                            });
                            // mark the stalled peer as faulty and do not match again
                        }
                        Err(_) => {
                            info!(chunk = %hash_str, %peer,
                                dur = upload_onset.elapsed().as_secs_f32(),
                                "upload has timed out."
                            );
                        }
                    };
                });
            }
        }
    }
}

pub async fn run(
    mut rx_coord: mpsc::Receiver<Message>,
    mut rx_handler: mpsc::Receiver<HandlerMessage>,
    tx_swarm: mpsc::Sender<SwarmMessage>,
    tx_bridge: mpsc::Sender<BridgeMessage>,
    mut blob_transfer_control: libp2p_stream::Control,
    shutdown: CancellationToken,
) -> Result<task::JoinHandle<()>> {
    //  setup blob transfer
    let mut incoming_pushes = blob_transfer::accept_pushes(
        blob_transfer_control.accept(blob_transfer::PUSH_PROTOCOL)?,
        MAX_NETWORK_BLOB_SIZE
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
                    warn!("Received shutdown request.");
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
                    // pipeline.remove_stale_blobs();
                },

                _i = timer_assign.tick() => {
                    pipeline.assign_chunks().await;                    
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
                                            now.checked_add(
                                                Duration::from_secs(valid_for as u64)
                                            ).unwrap_or_else(|| now)
                                        );
                                        // todo: storage permit is already invalid in case of overflow
                                        pipeline.assign_chunks().await;
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
                m = rx_coord.recv() => match m {
                    Some(msg) => match msg {
                        // remote blob: download chunks and assemble 
                        Message::GetBlob { id } => {
                            if let Some(Blob::LocalBlob { .. }) = pipeline.current_blob {
                                // todo
                            } else {
                                info!("Blob(`{}`) is missing, scheduled for gathering from remote nodes.", id);
                                pipeline.pending_blobs.push_back(
                                    Blob::RemoteBlob {
                                        id: id.clone(),
                                        // pull_status: vec![(Status::AwaitingMetadata, Instant::now())],
                                        expected_chunk_hashes: vec![],
                                        chunks: HashMap::new(),
                                    }
                                );
                            }
                        }
                        // local blob: chunk and distribute
                        Message::NewBlob { id, data } => {
                            match pipeline.add_local_blob(id.clone(), data).await {
                                Ok(_) => {
                                    pipeline.begin_next_blob().await;
                                }
                                Err(add_err) => {
                                    warn!("Add local blob error: {:?}", add_err);
                                    let _ = tx_bridge.send(BridgeMessage::UpdateStatus {
                                        id,
                                        status: BridgeStatus::Failed { reason: Some(add_err.to_string()) }
                                    }).await;
                                    // todo: retry
                                    continue
                                }
                            }
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
                            let hash_str = hex::encode(hash);
                            info!("A new chunk(`{}`) status update(`{:?}`)", hash_str, status);
                            let Some(blob) = &mut pipeline.current_blob else {
                                warn!("The current blob is invalid.");
                                continue
                            };
                            let Blob::LocalBlob { id, chunks, .. } = blob else {
                                trace!("The current blob is of `remote` kind.");
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
                                    info!("Chunk is being sent to Peer(`{}`).", to);
                                },
                                ChunkUploadStatus::Finalized { owner } => {
                                    info!("Chunk has been successfully uploaded to peer(`{}`).", owner);
                                    let is_finalized = chunks
                                        .values()
                                        .all(|chunk| matches!(chunk.status.status, ChunkUploadStatus::Finalized { .. }));
                                    if is_finalized {
                                        info!("Blob(`{}`) is now stored globally.", id);
                                        let _ = tx_bridge.send(BridgeMessage::UpdateStatus {
                                            id: id.to_string(),
                                            status: BridgeStatus::Finalized
                                        }).await;
                                        let _ = pipeline.archive_cur_blob();
                                        pipeline.begin_next_blob().await;
                                    }
                                    pipeline.assign_chunks().await;
                                    // todo: when to notify it about failure?
                                }
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