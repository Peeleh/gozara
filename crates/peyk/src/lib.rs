pub mod p2p;
pub mod protocol;
pub mod blob_transfer;

use std::{
    time::Duration,
    collections::HashMap
};
use futures::stream::StreamExt;
use eyre::Result;
use tracing::{info, warn};
use libp2p::{
    identify,  
    gossipsub,
    kad,
    request_response,
    swarm::{
        Swarm,
        SwarmEvent
    },
    PeerId,
    multiaddr::Protocol
};
use tokio::{
    sync::{mpsc},
    time::interval
};
use tokio_stream::wrappers::IntervalStream;
use bytes::Bytes;
use p2p::{GlobalBehaviour, GlobalBehaviourEvent};

pub enum SwarmMessage {
    RequestStoragePermits { 
        peers: Vec<PeerId>,
    },
    Store {
        chunks: HashMap<PeerId, Vec<([u8; 32], Bytes)>>,
    }    
}

// consumers handle inbound messages
pub enum HandlerMessage {
    WouldStore {
        peer_id: PeerId, 
    },
    Request {
        peer_id: PeerId,
        request_id: request_response::InboundRequestId,
        request: protocol::Request,
        channel: request_response::ResponseChannel<protocol::Response>
    },
    Response {
        peer_id: PeerId,
        request_id: request_response::OutboundRequestId,
        response: protocol::Response
    }    
}

pub async fn process_swarm(
    mut swarm: Swarm<GlobalBehaviour>,
    mut rx: mpsc::Receiver<SwarmMessage>,
    tx_handler: mpsc::Sender<HandlerMessage>
) -> Result<()> {
    tokio::spawn(async move {
        // to update kademlia tables
        let mut timer_peer_discovery = IntervalStream::new(
            interval(Duration::from_secs(60))
        ).fuse();

        loop {
            tokio::select! {
                // try to discover new peers
                _i = timer_peer_discovery.select_next_some() => {                
                    let random_peer_id = PeerId::random();
                    // info!("Searching for the closest peers to `{random_peer_id}`");
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .get_closest_peers(random_peer_id);
                },

                // blob events
                r = rx.recv() =>  match r {
                    Some(sw_req) => {
                        match sw_req {
                            SwarmMessage::RequestStoragePermits { 
                                peers
                            } => {
                                for peer in peers.iter() {
                                    let _ = swarm
                                        .behaviour_mut()
                                        .req_resp
                                        .send_request(
                                            peer,
                                            protocol::Request::RequestStoragePermit
                                        );
                                }
                            }
                            SwarmMessage::Store {
                                chunks,
                            } => {
                                // chunks: HashMap<PeerId, Vec<(Hash, Bytes)>>,                                
                                
                            }
                        }
                    }
                    None => {
                        warn!("Swarm channel is closed.");
                        continue
                    }
                },                
                
                // libp2p events
                event = swarm.select_next_some() => match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Local node is listening on {address}");
                    }
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        endpoint,
                        ..
                    } => {
                        info!(
                            "A connection has been established to {} via {:?}",
                            peer_id,
                            endpoint
                        );                    
                    }
                    // <identify>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Identify(identify::Event::Received {
                        // peer_id,
                        // info,
                        ..
                    })) => {
                        // info!(
                        //     "Received identify from {}: {:#?}`",
                        //     peer_id,
                        //     info
                        // );                        
                    }
                    SwarmEvent::NewExternalAddrOfPeer {
                        peer_id,
                        address
                    } => {
                        let is_public = address.iter()
                            .filter_map(|c| 
                                if let Protocol::Ip4(ip4_addr) = c {
                                    Some(ip4_addr)
                                } else {
                                    None
                                }
                            )
                            .all(|a| !a.is_private() && !a.is_loopback());
                        if is_public {                        
                            info!(
                                "Added public address of the peer to the DHT: {}",
                                address
                            );
                            swarm.behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, address);
                        }                      
                    }
                    // <gossipsub>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message,
                        ..
                    })) => {
                        match bincode::deserialize::<u8>(&message.data) {
                            Ok(_) => {
                                if let Err(e) = tx_handler.send(HandlerMessage::WouldStore {
                                    peer_id: peer_id,
                                }).await {
                                    warn!(
                                        "Gossip notify error: `{:?}`",
                                        e
                                    );                                    
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "Gossip message decode error: `{:?}`",
                                    e
                                );
                            }
                        }
                    }
                    // <kademlia>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        result: kad::QueryResult::GetClosestPeers(Ok(_ok)),
                        ..
                    })) => {
                        // info!("Query finished with closest peers: {:#?}", ok.peers);
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        result:
                            kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                                ..
                            })),
                        ..
                    })) => {
                        // warn!("Query for closest peers timed out");
                    }
                    // SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    //     result: kad::QueryResult::GetProviders(
                    //         Ok(
                    //             kad::GetProvidersOk::FoundProviders{ mut providers, .. }
                    //         )
                    //     ),
                    //     ..
                    // })) => {
                    //     providers.remove(&my_peer_id);
                    //     info!("providers: {:?}", providers);
                    //     for peer_id in providers {
                    //         let res = swarm.dial(peer_id);
                    //         info!("dial result: {:?}", res);
                    //     }
                    // },

                    // <protocol>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                        peer: peer_id,
                        message: request_response::Message::Request {
                            request,
                            channel,
                            request_id,
                            ..
                        },
                        ..
                    })) => {                 
                        // let _ = swarm
                        //     .behaviour_mut()
                        //     .req_resp
                        //     .send_response(
                        //         channel,
                        //         protocol::Response::Accept
                        //     );
                        if let Err(e) = tx_handler.send(HandlerMessage::Request {
                            peer_id: peer_id,
                            request_id: request_id,
                            request: request,
                            channel: channel
                        }).await {
                            warn!(
                                "Request relay error: `{:?}`",
                                e
                            );                                    
                        }
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                        peer: peer_id,
                        message: request_response::Message::Response {
                            response,
                            request_id,
                            ..
                        },
                        ..
                    })) => {                
                        if let Err(e) = tx_handler.send(HandlerMessage::Response {
                            peer_id: peer_id,
                            request_id: request_id,
                            response: response
                        }).await {
                            warn!(
                                "Response relay error: `{:?}`",
                                e
                            );                                    
                        }
                    }
                    // <blob transfer>
                    // SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    //     peer: peer_id,
                    //     message: request_response::Message::Request {
                    //         request: blob_transfer::Request(blob_hash),
                    //         channel,
                    //         //request_id,
                    //         ..
                    //     },
                    //     ..
                    // })) => {                
                    //     let blob_hash = blob_hash.parse::<u128>().unwrap();
                    //     if let Some(blob) = pipeline.get_blob(&blob_hash) {
                    //         if let Err(e) = swarm
                    //             .behaviour_mut()
                    //             .blob_transfer
                    //                 .send_response(
                    //                     channel,
                    //                     blob_transfer::Response(blob.clone())
                    //                 )
                    //         {
                    //             warn!(
                    //                 "Failed to initiate the requested blob(`{}`)'s transmission: `{:?}`.",
                    //                 blob_hash,
                    //                 e
                    //             );
                    //         } else {
                    //             info!(
                    //                 "The requested blob(`{}`)'s transmission to `{}` is initiated: {:.2} KB",
                    //                 blob_hash,
                    //                 peer_id,
                    //                 blob.len() as f64 / 1024.0f64
                    //             );
                    //         }
                    //     } else {
                    //         warn!(
                    //             "The requested blob(`{}`) does not exist.",
                    //             blob_hash,
                    //         );
                    //     }
                    // },

                    // SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    //     peer: peer_id,
                    //     message: request_response::Message::Response {
                    //         response: blob_transfer::Response(blob),
                    //         //response_id,
                    //         ..
                    //     },
                    //     ..
                    // })) => {
                    //     pipeline.verify_agg_proof(blob, peer_id);
                    // },

                    // SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::InboundFailure {
                    //     peer: peer_id,
                    //     connection_id,
                    //     request_id,
                    //     error,
                    // })) => {
                    //     warn!(
                    //         "Blob transfer `inbound failure`: peer `{}`, con_id: {:?}, req_id: {:?}, e: {:?} ",
                    //         peer_id,
                    //         connection_id,
                    //         request_id,
                    //         error
                    //     );
                    // },

                    // SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::OutboundFailure {
                    //     peer: peer_id,
                    //     connection_id,
                    //     request_id,
                    //     error,
                    // })) => {
                    //     warn!(
                    //         "Blob transfer `outbound failure`: peer `{}`, con_id: {:?}, req_id: {:?}, e: {:?} ",
                    //         peer_id,
                    //         connection_id,
                    //         request_id,
                    //         error
                    //     );
                    // },
                    _ => {
                        // info!("{:#?}", event);
                    }
                },
            }
        }
    });
    Ok(())
}
