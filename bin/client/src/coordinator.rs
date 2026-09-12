use std::{
    time::{Instant, Duration},
    collections::{VecDeque, HashMap},
};
use eyre::{eyre, Result};
use tracing::{info, warn};
use futures::StreamExt;
use tokio::{
    sync::{mpsc, oneshot},
    time::interval
};
use tokio_stream::wrappers::IntervalStream;
use bytes::Bytes;
use libp2p::{
    // identity,
    // gossipsub,
    // swarm::Swarm,
    PeerId,
};
use rand::seq::IndexedRandom;
use crate::blob_store::{Hash, BlobMessage} ;
use peyk::{HandlerMessage, SwarmMessage};

// 30 seconds
const UploadTimeout: u64 = 30;
// provider are valid for 5 minutes
const STORAGE_PROVIDER_DECAY: u64 = 5 * 60;

enum ChunkUploadStatus {
    Pending,
    Inflight {
        created_at: u64,
        to: PeerId,
        data: Bytes
    },
    Finalized {
        owner: PeerId
    }
}

struct StorageDeal {
    id: String,
    pub root_hash: Hash,
    pub chunks: HashMap<Hash, ChunkUploadStatus>,
}

struct Pipeline {
    // record WouldStore gossips
    pub storage_provider_hints: HashMap<PeerId, u64>,
    pub storage_permits: HashMap<PeerId, u64>,
    pub pending_storage_deals: VecDeque<StorageDeal>,
    pub active_storage_deal: Option<StorageDeal>,
    pub tx_swarm: mpsc::Sender<SwarmMessage>,
    pub tx_blob: mpsc::Sender<BlobMessage>,
}

impl Pipeline {
    pub fn new(
        tx_swarm: mpsc::Sender<SwarmMessage>,
        tx_blob: mpsc::Sender<BlobMessage>
        ) -> Self {
        Pipeline {
            storage_provider_hints: HashMap::new(),
            storage_permits: HashMap::new(),
            pending_storage_deals: VecDeque::new(),
            active_storage_deal: None,
            tx_swarm: tx_swarm,
            tx_blob: tx_blob
        }
    }

    pub fn new_deal(
        &mut self,
        id: String,
        root_hash: Hash,
        chunk_hashes: Vec<Hash>
    ) {
        let now = Instant::now().elapsed().as_secs();
        self.pending_storage_deals.push_back(StorageDeal {
            id,
            root_hash: root_hash,
            chunks: chunk_hashes
                .into_iter()
                .map(|h| (h, ChunkUploadStatus::Pending))
                .collect()
        });
        if self.active_storage_deal.is_none() {
            self.active_storage_deal = self.pending_storage_deals.pop_front();
        }        
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

    pub async fn assign_chunks(&mut self) {
        if self.active_storage_deal.is_none() {
            return 
        }
        let now = Instant::now().elapsed().as_secs();
        self.storage_permits
            .retain(|_, valid_until| {
                *valid_until < now
            });
        if self.storage_permits.is_empty() {
            return
        }
        let peers: Vec<PeerId> = self.storage_permits.keys().cloned().collect();
        // each chunk is 4mb so with 1gbps(100mb/s) link speed, each permit
        // translates to 20 chunks/~100mb worth of storage space        
        const CHUNKS_PER_PERMIT: usize = 20;        
        // skip already finalized chunks
        let now = Instant::now().elapsed().as_secs();
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
                        if created_at + UploadTimeout > now {
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
            .chunks(CHUNKS_PER_PERMIT)
            .next()
            .unwrap()
            .into_iter()
            .cloned()            
            .collect();
        let (tx, rx) = oneshot::channel::<Option<HashMap<Hash, Option<Bytes>>>>();
        if let Err(e) = self.tx_blob.send(BlobMessage::FetchChunks {
            id: active_storage_deal.id.clone(),
            chunks: chosen_chunks,
            tx: tx,
        }).await {
            warn!(
                "Failed to ask for chunks of blob(`{}`) from blob store: {}",
                active_storage_deal.id,
                e
            );
        }
        info!(
            "Requested some chunks of blob (`{}`).",
            active_storage_deal.id
        );
        match rx.await {
            Ok(r) => {
                let Some(chunks) = r else {
                    warn!("Chunks are empty!");
                    return
                };
                // todo: cry about `none` chunks
                let chunks: Vec<(Hash, Bytes)> = chunks
                    .into_iter()
                    .filter_map(|(h, b)| if b.is_some() { Some((h, b.unwrap())) } else { None })
                    .collect();
                if chunks.is_empty() {
                    warn!("Chunks are empty!");
                    return
                }
                let num_chunks = chunks.len();
                let num_peers = peers.len();
                let assignments: HashMap<PeerId, Vec<(Hash, Bytes)>> = if num_chunks >= num_peers {
                    let bucket_size = num_chunks / num_peers;
                    let buckets = chunks
                        .chunks(bucket_size as usize)
                        .into_iter()
                        .map(|t| t.into())
                        .collect::<Vec<Vec<(Hash, Bytes)>>>();
                    peers                        
                        .into_iter()
                        .zip(buckets)
                        .collect()
                } else {
                    let mut rng = &mut rand::rng();
                    let chunks: Vec<Vec<(Hash, Bytes)>> = chunks
                        .into_iter()
                        .map(|c| [c].into())
                        .collect();
                    peers
                        .sample(&mut rng, num_chunks)
                        .into_iter()
                        .cloned()
                        .zip(chunks.into_iter().collect::<Vec<_>>())
                        .collect()
                }; 
                if let Err(e) = self.tx_swarm.send(SwarmMessage::Store {
                    chunks: assignments,
                }).await {
                    warn!(
                        "Failed to send chunks to swarm channel: {}",
                        e
                    );
                }
            },
            Err(e) => {
                warn!("Reply channel for chunks is closed.");
            }
        };
    }
}

pub enum CoordMessage {
    DistributeBlob {
        id: String,
        root_hash: Hash,
        chunk_hashes: Vec<Hash>,
    }
}

pub async fn run(
    mut rx_coord: mpsc::Receiver<CoordMessage>,
    mut rx_handler: mpsc::Receiver<HandlerMessage>,
    tx_swarm: mpsc::Sender<SwarmMessage>,
    tx_blob: mpsc::Sender<BlobMessage>
) -> Result<()> {
    let mut pipeline = Pipeline::new(tx_swarm, tx_blob);
    let mut timer_stale_providers = IntervalStream::new(
        interval(Duration::from_secs(60))
    ).fuse();
    let mut timer_assign = IntervalStream::new(
        interval(Duration::from_secs(30))
    ).fuse();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _i = timer_stale_providers.select_next_some() => {
                    let now = Instant::now().elapsed().as_secs();
                    pipeline
                        .storage_provider_hints
                        .retain(|_, created_at| {
                            *created_at + STORAGE_PROVIDER_DECAY < now
                        });                    
                },
                _i = timer_assign.select_next_some() => {
                    pipeline.assign_chunks().await;
                },
                // swarm handlers
                hm = rx_handler.recv() =>  match hm {
                    Some(h_msg) => {
                        match h_msg {
                            // a gossip by storer nodes
                            HandlerMessage::WouldStore {
                                peer_id,
                            } => {
                                pipeline
                                    .storage_provider_hints
                                    .insert(peer_id, Instant::now().elapsed().as_secs());
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
                                    peyk::protocol::Response::AckStoragePermit { valid_until } => {
                                        pipeline.storage_permits.insert(peer_id.clone(), valid_until);
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
            }
        }
    });
    Ok(())
}