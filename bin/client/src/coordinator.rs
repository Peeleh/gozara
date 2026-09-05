use std::{
    time::{Instant, Duration},
    collections::HashMap
};
use eyre::Result;
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

struct StorageProviderHints {
    pub capacity: u32,
    pub created_at: u64,
}

struct Chunk {
    pub created_at: u64,
    pub is_finalized: bool,
    pub owner: Option<PeerId>,
}

impl Chunk {
    pub fn new() -> Self {
        let now = Instant::now().elapsed().as_secs();
        Self {
            created_at: now,
            is_finalized: false,
            owner: None
        }
    }
}

struct StorageDeal {
    pub root_hash: Hash,
    pub chunks: HashMap<Hash, Chunk>,
}

impl StorageDeal {
    pub fn new(root_hash: Hash, chunk_hashes: Vec<Hash>) -> Self {        
        Self {
            root_hash: root_hash,
            chunks: chunk_hashes
                .into_iter()
                .map(|h| (h, Chunk::new()))
                .collect()
        }
    }

    // pub fn is_finalized(&self) -> bool {
    //     self.chunks.values().all(|v| *v == ChunkStatus::Finalized)
    // }
}

struct State {
    pub active_storage_providers: HashMap<PeerId, StorageProviderHints>,
    pub storage_deals: HashMap<String, StorageDeal>,
}

impl State {
    pub fn new() -> Self {
        State {
            active_storage_providers: HashMap::new(),
            storage_deals: HashMap::new()
        }
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
    let mut state = State::new();
    // remove stale storage providers every ~5 minutes
    const STORAGE_PROVIDER_DECAY: u64 = 5 * 60;
    let mut timer_stale_providers = IntervalStream::new(
        interval(Duration::from_secs(30))
    ).fuse();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _i = timer_stale_providers.select_next_some() => {
                    let now = Instant::now().elapsed().as_secs();
                    state
                        .active_storage_providers                                        
                        .retain(|_, v| {
                            v.created_at + STORAGE_PROVIDER_DECAY < now
                        });
                },
                // swarm handlers
                hm = rx_handler.recv() =>  match hm {
                    Some(h_msg) => {
                        match h_msg {
                            // a gossip by storer nodes
                            HandlerMessage::WouldStore {
                                peer_id,
                                capacity,
                            } => {
                                state.active_storage_providers.insert(peer_id, StorageProviderHints {
                                    capacity: capacity,
                                    created_at: Instant::now().elapsed().as_secs()
                                });
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
                                if state.storage_deals.contains_key(&id) {
                                    warn!(
                                        "Ignored duplicate diffuse blob message for blob: `{}`",
                                        id
                                    );
                                    continue;
                                }
                                state.storage_deals.insert(id, StorageDeal::new(root_hash, chunk_hashes));
                                // prepare for assignments
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