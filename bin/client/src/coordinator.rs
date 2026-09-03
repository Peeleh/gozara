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

struct StorageProviderSpecs {
    pub capacity: u32,
    pub created_at: u64,
}

struct State {
    pub active_storage_providers: HashMap<PeerId, StorageProviderSpecs>,
}

impl State {
    pub fn new() -> Self {
        State {
            active_storage_providers: HashMap::new(),
        }
    }
}

pub enum CoordMessage {
    DiffuseBlob {
        id: String,
        root_hash: Hash,
        num_chunks: usize,
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
                                state.active_storage_providers.insert(peer_id, StorageProviderSpecs {
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
                                num_chunks
                            } => {
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