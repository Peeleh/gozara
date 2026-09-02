pub mod p2p;
pub mod protocol;
pub mod blob_transfer;

use std::time::Duration;
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
use p2p::{GlobalBehaviour, GlobalBehaviourEvent};

pub enum SwarmMessage {
    PersistBlob { id: String }
}

// consumers handle inbound messages
pub enum HandlerMessage {
    Gossip {
        peer_id: PeerId, 
        data: protocol::WouldStore,
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
                            SwarmMessage::PersistBlob { id: _id } => {
                            }
                        }
                    }
                    None => {
                        warn!("Swarm channel is closed.");
                        break;
                    }
                },                
                
                // libp2p events
                event = swarm.select_next_some() => match event {
                    // general events
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
                    // identify events
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
                    // gossipsub events
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message,
                        ..
                    })) => {
                        match bincode::deserialize::<protocol::WouldStore>(&message.data) {
                            Ok(_) => {
                                if let Err(e) = tx_handler.send(HandlerMessage::Gossip {
                                    peer_id: peer_id,
                                    data: protocol::WouldStore
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
                    // kademlia events
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

                    // requests
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
                                "Request notify error: `{:?}`",
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
                                "Response notify error: `{:?}`",
                                e
                            );                                    
                        }
                    }
                    _ => {
                        // info!("{:#?}", event);
                    }
                },
            }
        }
    });
    Ok(())
}
