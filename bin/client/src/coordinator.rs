use std::{
    time::{Instant, Duration},
    collections::{VecDeque, HashMap},
    sync::Arc,
};
use eyre::{eyre, Result};
use tracing::{info, warn};
use tokio::{
    sync::{mpsc, oneshot, Semaphore},
    time::interval
};
use bytes::Bytes;
use libp2p::PeerId;
use rand::seq::IndexedRandom;
use crate::blob_store::{Hash, BlobMessage} ;
use crate::blob_transfer;
use peyk::{HandlerMessage, SwarmMessage};

// max incoming(network) blob size 8 MiB
const MAX_BLOB_SIZE: usize = 8 * 1024 * 1024;
// 30 seconds
const CHUNK_UPLOAD_WINDOW: u64 = 30;
// provider are valid for 5 minutes
const STORAGE_PROVIDER_DECAY: u64 = 5 * 60;

pub enum CoordMessage {
    DistributeBlob {
        id: String,
        root_hash: Hash,
        chunk_hashes: Vec<Hash>,
    }, 
}

enum InternalMessage {
    UpdateChunkStatus {
        hash: Hash,
        new_status: ChunkUploadStatus
    }
}

#[derive(Debug, Clone)]
enum ChunkUploadStatus {
    Pending,
    Inflight {
        created_at: Instant,
        to: PeerId,
    },
    Finalized {
        on: u64,
        owner: PeerId
    }
}

struct StorageDeal {
    id: String,
    root_hash: Hash,
    chunks: HashMap<Hash, ChunkUploadStatus>,
}

impl StorageDeal {
    pub fn is_finalized(&self) -> bool {
        self.chunks
            .values()
            .all(|status| matches!(*status, ChunkUploadStatus::Finalized { .. }))
    }
}

struct Pipeline {
    // record WouldStore gossips
    storage_provider_hints: HashMap<PeerId, Instant>,
    storage_permits: HashMap<PeerId, Instant>,
    pending_storage_deals: VecDeque<StorageDeal>,
    active_storage_deal: Option<StorageDeal>,
    tx_internal: mpsc::UnboundedSender<InternalMessage>,
    tx_swarm: mpsc::Sender<SwarmMessage>,
    tx_blob: mpsc::Sender<BlobMessage>,
    blob_transfer_control: libp2p_stream::Control,
    tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>,
    // to gate i/o usage
    download_allowance: Arc<Semaphore>,
    upload_allowance: Arc<Semaphore>,
}

impl Pipeline {
    pub fn new(
        tx_internal: mpsc::UnboundedSender<InternalMessage>,
        tx_swarm: mpsc::Sender<SwarmMessage>,
        tx_blob: mpsc::Sender<BlobMessage>,
        blob_transfer_control: libp2p_stream::Control,
        tx_blob_transfer_events: mpsc::UnboundedSender<blob_transfer::TransferEvent>
    ) -> Self {
        Pipeline {
            storage_provider_hints: HashMap::new(),
            storage_permits: HashMap::new(),
            pending_storage_deals: VecDeque::new(),
            active_storage_deal: None,
            tx_internal,
            tx_swarm,
            tx_blob,
            blob_transfer_control,
            tx_blob_transfer_events,
            download_allowance: Arc::new(Semaphore::new(32)), // 32 * 4 = 128 MiB of downloads
            upload_allowance: Arc::new(Semaphore::new(20)),   // 20 * 4 =  80 MiB of uploads
        }
    }

    pub fn new_deal(
        &mut self,
        id: String,
        root_hash: Hash,
        chunk_hashes: Vec<Hash>
    ) {
        self.pending_storage_deals.push_back(StorageDeal {
            id,
            root_hash,
            chunks: chunk_hashes
                .into_iter()
                .map(|h| (h, ChunkUploadStatus::Pending))
                .collect()
        });
        self.begin_next_deal();
    }

    pub async fn request_storage_permits(&self)-> Result<()> {
        if self.storage_provider_hints.is_empty() {
            return Err(eyre!("No storage providers to request permits from."))
        }
        if self.active_storage_deal.is_some() {
            // todo: peers size should not be large
            self.tx_swarm.send(SwarmMessage::RequestStoragePermits {
                peers: self.storage_provider_hints.keys().cloned().collect()
            }).await?;
        }
        Ok(())
    }

    pub fn begin_next_deal(&mut self) {
        if self.active_storage_deal.is_none() {
            self.active_storage_deal = self.pending_storage_deals.pop_front();
            if let Some(deal) = self.active_storage_deal.as_ref() {
                info!(
                    "A new deal(`{}`) has begun.",
                    deal.id
                );
            } else {
                info!("All deals are caught up. Waiting for the next...");
            }
        }
    }

    pub async fn assign_chunks(&mut self) {
        if self.active_storage_deal.is_none() || 0 == self.upload_allowance.available_permits() {
            return
        }        
        let now = Instant::now();
        self.storage_permits.retain(|_, expires_at| *expires_at > now);
        if self.storage_permits.is_empty() {
            return
        }
        let peers: Vec<PeerId> = self.storage_permits.keys().cloned().collect();
        // skip already finalized chunks
        let active_storage_deal = self.active_storage_deal.as_mut().unwrap();
        let pending_chunks: Vec<_> = active_storage_deal
            .chunks
            .iter()
            .filter_map(|(h, upload_status)| {
                match upload_status {
                    ChunkUploadStatus::Pending => Some(*h),
                    ChunkUploadStatus::Inflight {
                        created_at,
                        ..
                    } => {
                        // timed out
                        if now.duration_since(*created_at).as_secs() > CHUNK_UPLOAD_WINDOW {
                            Some(*h)
                        } else {
                            None
                        }
                    },
                    ChunkUploadStatus::Finalized { .. } => None
                }
            })
            .collect();
        let chosen_chunks: Vec<Hash> = pending_chunks
            .into_iter()
            .take(self.upload_allowance.available_permits())
            .collect();
        let (tx, rx) = oneshot::channel::<Option<HashMap<Hash, Option<Bytes>>>>();
        if let Err(e) = self.tx_blob.send(BlobMessage::FetchChunks {
            id: active_storage_deal.id.clone(),
            chunks: chosen_chunks,
            tx_reply: tx,
        }).await {
            warn!(
                "Failed to ask for chunks of blob(`{}`) from blob store: {}",
                active_storage_deal.id,
                e
            );
            return
        }
        info!(
            "Requested some chunks of blob (`{}`).",
            active_storage_deal.id
        );
        match rx.await {
            Ok(r) => {
                let Some(chunks) = r else {
                    warn!("No chunks are available.");
                    return
                };
                // todo: cry about `none` chunks
                let chunks: Vec<(Hash, Bytes)> = chunks
                    .into_iter()
                    .filter_map(|(h, b)| if b.is_some() { Some((h, b.unwrap())) } else { None })
                    .collect();
                if chunks.is_empty() {
                    warn!("Critical to assignment: requested chunks are missing.");
                    return
                }
                if peers.is_empty() {
                    warn!("Critical to assignment: no peers to assign.");
                    return
                }
                let num_peers = peers.len();
                let mut assignments: HashMap<PeerId, Vec<(Hash, Bytes)>> = HashMap::new();
                let mut peer_index = 0;
                let mut chunks_iter = chunks.into_iter();
                while let Some(chunk) = chunks_iter.next() {
                    assignments
                        .entry(peers[peer_index])
                        .or_default()
                        .push(chunk);
                    peer_index = (peer_index + 1) % num_peers;
                }
                // upload chunks                 
                for (peer, asses) in assignments.into_iter() {
                    for (hash, data) in asses.into_iter() {
                        let control = self.blob_transfer_control.clone();
                        let upload_allowance = self.upload_allowance.clone();
                        let tx_events = self.tx_blob_transfer_events.clone();
                        // self send for maintenance and state propagation
                        let tx_internal = self.tx_internal.clone();
                        tokio::spawn(async move {
                            if let Err(e) = upload_allowance.acquire_owned().await {
                                warn!(
                                    "Could not get allowance from the semaphore to begin upload: {:?}",
                                    e
                                );
                                return
                            }
                            let hash_str = hex::encode(hash);
                            // upload initiated
                            if let Err(e) = tx_internal.send(InternalMessage::UpdateChunkStatus {
                                hash,
                                new_status: ChunkUploadStatus::Inflight {
                                    created_at: Instant::now(),
                                    to: peer.clone()
                                }
                            }) {
                                warn!(
                                    "Could not notify the coordinator about an inflight upload for chunk(`{}`): {:?}",
                                    hash_str,
                                    e
                                );
                            }
                            if let Err(e) = blob_transfer::push(
                                control,
                                peer.clone(),
                                hash_str.clone(),
                                data,
                                tx_events
                            ).await {
                                warn!(
                                    "Push blob(`{}`) to Peer(`{}`) failed: {}",
                                    hash_str,
                                    peer.clone(),
                                    e
                                );
                                // keep it at inflight to simulate backoff
                                return
                            }
                            // upload succeeded
                            info!(
                                "Blob(`{}`) has been successfully transferred to Peer(`{}`).",
                                hash_str,
                                peer
                            );
                            if let Err(e) = tx_internal.send(InternalMessage::UpdateChunkStatus {
                                hash,
                                new_status: ChunkUploadStatus::Finalized {
                                    on: Instant::now().elapsed().as_secs(),
                                    owner: peer.clone()
                                }
                            }) {
                                warn!(
                                    "Could not notify the coordinator about an completed upload for chunk(`{}`): {:?}",
                                    hash_str,
                                    e
                                );
                            }                            
                        });
                    }
                }
                
            },
            Err(_) => {
                warn!("Reply channel for chunks is closed.");
            }
        };
    }
}

pub async fn run(
    mut rx_coord: mpsc::Receiver<CoordMessage>,
    mut rx_handler: mpsc::Receiver<HandlerMessage>,
    tx_swarm: mpsc::Sender<SwarmMessage>,
    tx_blob: mpsc::Sender<BlobMessage>,
    mut blob_transfer_control: libp2p_stream::Control,
) -> Result<()> {
    //  setup blob transfer
    let mut incoming_pushes = blob_transfer::accept_pushes(
        blob_transfer_control.accept(blob_transfer::PUSH_PROTOCOL)?,
        MAX_BLOB_SIZE
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
        tx_blob.clone(),
        blob_transfer_control,
        tx_blob_transfer_events
    );
    let mut timer_stale_providers = interval(Duration::from_secs(60));
    let mut timer_assign = interval(Duration::from_secs(30));
    let jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                _i = timer_stale_providers.tick() => {
                    pipeline
                        .storage_provider_hints
                        .retain(|_, created_at| {
                            Instant::now().duration_since(*created_at).as_secs() < STORAGE_PROVIDER_DECAY
                        });                    
                },
                _i = timer_assign.tick() => {
                    pipeline.assign_chunks().await;                    
                },
                // swarm handlers
                hm = rx_handler.recv() => match hm {
                    Some(h_msg) => {
                        match h_msg {
                            // a gossip by storer nodes
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
                                request_id: _,
                                response
                            } => {
                                match response {
                                    peyk::protocol::Response::AckStoragePermit { valid_for } => {
                                        let now = Instant::now();
                                        pipeline.storage_permits.insert(
                                            peer_id.clone(),
                                            Instant::now().checked_add(
                                                Duration::from_secs(valid_for as u64)
                                            ).unwrap_or_else(|| now)
                                        );
                                        // todo: storage permit is already invalid
                                    }
                                }
                            }    
                        }
                    }
                    None => {
                        warn!("Swarm handler channel is closed.");
                        break;
                    }
                },
                // coordination messages
                cm = rx_coord.recv() => match cm {
                    Some(c_msg) => {
                        match c_msg {
                            CoordMessage::DistributeBlob {
                                id,
                                root_hash,
                                chunk_hashes
                            } => {              
                                pipeline.new_deal(id, root_hash, chunk_hashes);
                                if let Err(e) = pipeline.request_storage_permits().await {
                                    warn!(
                                        "Request storage permit failed: {}",
                                        e
                                    );
                                    // todo: retry with backoff
                                }
                            }
                        }
                    }
                    None => {
                        warn!("Coordination channel is closed.");
                        break;
                    }
                },
                // internal message
                im = rx_internal.recv() => match im {
                    Some(i_msg) => {
                        match i_msg {
                            InternalMessage::UpdateChunkStatus {
                                hash,
                                new_status
                            } => {
                                let hash_str = hex::encode(hash);
                                let Some(active_storage_deal) = pipeline.active_storage_deal.as_mut() else {
                                    warn!(
                                        "New chunk status update(`{}`) for chunk(`{:?}`) but the active storage deal is invalid.",
                                        hash_str,
                                        new_status
                                    );
                                    continue
                                };
                                let Some(chunk_status) = active_storage_deal.chunks.get_mut(&hash) else {
                                    warn!(
                                        "Unsolicited new status update(`{}`) for Chunk(`{:?}`).",
                                        hash_str,
                                        new_status                                        
                                    );
                                    continue
                                };
                                // todo: check if the new status is > the older
                                *chunk_status = new_status.clone();
                                match new_status {
                                    ChunkUploadStatus::Pending | ChunkUploadStatus::Inflight { .. } => {},
                                    ChunkUploadStatus::Finalized {
                                        on,
                                        owner
                                    } => {
                                        info!(
                                            "Chunk(`{}`) is successfully upload to peer(`{}`).",
                                            hash_str,
                                            owner
                                        );
                                        if active_storage_deal.is_finalized() {
                                            match tx_blob.send(BlobMessage::StoreResult {
                                                id: active_storage_deal.id.clone(),
                                                success: true,
                                                failure_reason: None
                                            }).await {
                                                Ok(_) => {
                                                    pipeline.begin_next_deal();
                                                }
                                                Err(e) => 
                                                    warn!(
                                                        "Failed to notify blob store about global storage finalization: {:?}.",
                                                        e
                                                    )
                                            }
                                        }
                                        // todo: when to notify it about failure?
                                    }
                                };

                            }
                        }
                    }
                    None => {
                        warn!("Internal channel is closed.");
                        break;
                    }
                },
                // <blob transfer>
                // pushes
                p = incoming_pushes.recv() => match p {
                    Some(push) => {
                        // push.peer, push.data
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
    jh.await.map_err(|e| eyre!(e.to_string()))
}