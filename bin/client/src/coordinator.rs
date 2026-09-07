use std::{
    time::{Instant, Duration},
    collections::HashMap,
};
use eyre::{eyre, Result};
use tracing::{info, warn};
use futures::StreamExt;
use tokio::{
    sync::mpsc,
    time::interval
};
use tokio_stream::wrappers::IntervalStream;
use libp2p::{
    // identity,
    // gossipsub,
    // swarm::Swarm,
    PeerId,
};
use crate::blob_store::Hash;
use peyk::{HandlerMessage, SwarmMessage};

struct Chunk {
    pub created_at: u64,
    pub last_update: u64,
    pub owner: Option<PeerId>,
}

struct StorageDeal {
    root_hash: Hash,
    chunk_hashes: HashMap<Hash, Chunk>,
}

struct Pipeline {
    // u64 is the time of creation
    pub storage_provider_hints: HashMap<PeerId, u64>,
    pub storage_permits: HashMap<PeerId, u64>,
    pub storage_deals: HashMap<String, StorageDeal>,
    pub tx_swarm: mpsc::Sender<SwarmMessage>,
}

impl Pipeline {
    pub fn new(tx_swarm: mpsc::Sender<SwarmMessage>) -> Self {
        Pipeline {
            storage_provider_hints: HashMap::new(),
            storage_permits: HashMap::new(),
            storage_deals: HashMap::new(),
            tx_swarm: tx_swarm
        }
    }

    pub fn new_deal(
        &mut self,
        id: String,
        root_hash: Hash,
        chunk_hashes: Vec<Hash>
    ) {
        let now = Instant::now().elapsed().as_secs();
        self.storage_deals.insert(id, StorageDeal {
            root_hash: root_hash,
            chunk_hashes: chunk_hashes
                .into_iter()
                .map(|h| (h, Chunk {
                    created_at: now,
                    last_update: now,
                    owner: None
                }))
                .collect()
        });
    }

    pub async fn request_storage_permits(&self)-> Result<()> {
        if self.storage_provider_hints.is_empty() {
            return Err(eyre!("No storage providers to request permits from."))
        }
        // todo: peers size should not be large
        self.tx_swarm.send(SwarmMessage::RequestStoragePermits {
            peers: self.storage_provider_hints.keys().cloned().collect()
        }).await?;
        Ok(())
    }

    pub fn assign_chunks_to(
        &mut self,
        peer_id: PeerId,
        permit_valid_until: u64
    ) {
        // fifo & greedy strategy in chunk assignment

        // each chunk is 4mb so with 1gbps(100mb/s) link speed, each permit
        // translates to 20 chunks/~100mb worth of storage space
        const CHUNKS_PER_PERMIT: usize = 20;

    }
}

pub enum CoordMessage {
    DiffuseBlob {
        id: String,
        root_hash: Hash,
        chunk_hashes: Vec<Hash>,
    }
}

pub async fn run(
    mut rx_coord: mpsc::Receiver<CoordMessage>,
    mut rx_handler: mpsc::Receiver<HandlerMessage>,
    tx_swarm: mpsc::Sender<SwarmMessage>
) -> Result<()> {
    let mut pipeline = Pipeline::new(tx_swarm);
    // remove stale storage providers every ~5 minutes
    const STORAGE_PROVIDER_DECAY: u64 = 5 * 60;
    let mut timer_stale_providers = IntervalStream::new(
        interval(Duration::from_secs(30))
    ).fuse();
    // let mut timer_reassign = IntervalStream::new(
    //     interval(Duration::from_secs(20))
    // ).fuse();
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
                    pipeline
                        .storage_permits
                        .retain(|_, valid_until| {
                            *valid_until < now
                        });
                },
                // _i = timer_reassign.select_next_some() => {
                //     if pipeline.storage_deal.is_some() {
                //         pipeline.assign_chunks();
                //     }
                // },
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
                                        pipeline.assign_chunks_to(peer_id, valid_until);
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
                            CoordMessage::DiffuseBlob {
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