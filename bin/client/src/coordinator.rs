use std::collections::HashMap;
use eyre::Result;
use tracing::{info, warn};
use libp2p::{
    identity,
    gossipsub,
    PeerId,
};
use tokio::sync::mpsc;
use libp2p::swarm::Swarm;
use crate::blob_store::Hash;
use peyk::{HandlerMessage, SwarmMessage};

struct State {
    pub active_storage_providers: HashMap<PeerId, u32>,
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
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // swarm handlers
                hm = rx_handler.recv() =>  match hm {
                    Some(h_msg) => {
                        match h_msg {
                            // a gossip by storer nodes
                            HandlerMessage::WouldStore {
                                peer_id,
                                capacity,
                            } => {
                                state.active_storage_providers.insert(peer_id, capacity);
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