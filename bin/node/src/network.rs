use crate::NodeConfig;
use std::{
    fs,
    time::Duration
};
use futures::stream::StreamExt;
use eyre::Result;
use tracing::{info, warn};
use libp2p::{
    identity,
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
use peyk::{
    p2p::{
        GlobalBehaviourEvent
    },
    // blob_transfer,
    protocol
};
use crate::blob_store::Hash;

pub enum SwarmRequest {
    NeedBlob(Hash),
    PersistBlob(Hash)
}

pub async fn process_swarm(
    mut swarm: Swarm<peyk::p2p::GlobalBehaviour>,
    mut rx: mpsc::UnboundedReceiver<SwarmRequest>
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
                            SwarmRequest::NeedBlob(_hash) => {}
                            SwarmRequest::PersistBlob(_hash) => {
                                
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
                        // propagation_source: peer_id,
                        message,
                        ..
                    })) => {
                        
                        match bincode::deserialize::<protocol::WouldProve>(&message.data) {
                            Ok(_) => {
                                
                            },

                            Err(e) => {
                                warn!(
                                    "Gossip message decode error: `{:?}`",
                                    e
                                );
                            },

                        };
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
                        // peer: prover,
                        message: request_response::Message::Request {
                            request: protocol::Request::ProofIsReady(_token),
                            channel,
                            //request_id,
                            ..
                        },
                        ..
                    })) => {                 
                        let _ = swarm
                            .behaviour_mut()
                            .req_resp
                            .send_response(
                                channel,
                                protocol::Response::Accept
                            );
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                        peer: _peer_id,
                        message: request_response::Message::Response {
                            response,
                            //response_id,
                            ..
                        },
                        ..
                    })) => {                
                        match response {
                            _ => {},
                        };
                    },
                    _ => {
                        // info!("{:#?}", event);
                    }
                },
            }
        }
    });
    Ok(())
}

pub async fn go_public(
    config: NodeConfig,
    mut rx: mpsc::UnboundedReceiver<SwarmRequest>
) -> Result<()> {
    // derive peer id
    let local_key = {
        if let Some(key_file) = config.key_file {
            let bytes = fs::read(key_file)?;
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = fs::write("./key.secret", bytes);
            warn!("No keys were supplied, so one is generated for you and saved to `./key.secret` file.");
            new_key
        }
    };
    let my_peer_id = PeerId::from_public_key(&local_key.public());    
    info!(
        "My peer id: `{}`",
        my_peer_id
    );  
    let mut swarm = peyk::p2p::setup_global_swarm(&local_key)?;
    // listen on all interfaces
    // ipv4
    swarm.listen_on(
        "/ip4/0.0.0.0/udp/20201/quic-v1".parse()?
    )?;
    swarm.listen_on(
        "/ip4/0.0.0.0/tcp/20201".parse()?
    )?;
    // ipv6
    // swarm.listen_on(
    //     "/ip6/::/udp/20201/quic-v1".parse()?
    // )?;
    // swarm.listen_on(
    //     "/ip6/::/tcp/20201".parse()?
    // )?;
    // gossip
    // topic example: "gozara-me" for the middle east zone
    let topic = gossipsub::IdentTopic::new(format!("gozara-{}-zone", config.zone));
    let _ = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic);
    
    // init kademlia: get to know bootnode(s)    
    swarm.behaviour_mut()
        .kademlia
        .add_address(
            &config.bootnode_peer_id.parse()?,
            format!(
                "/ip4/{}/tcp/20201",
                config.bootnode_ip_addr
            )
            .parse()?
        );
    // initiate bootstrapping
    match swarm.behaviour_mut().kademlia.bootstrap() {
        Ok(query_id) => {            
            info!(
                "Bootstrap is initiated, query id: {:?}",
                query_id
            );
        },
        Err(e) => {
            info!(
                "Bootstrap failed: {:?}",
                e
            );
        }
    };
    // specify the external address    
    swarm.add_external_address(
        format!(
            "/ip4/{}/tcp/{}",
            config.external_ip_addr,
            config.external_port
        )
        .parse()?
    );
    swarm.add_external_address(
        format!(
            "/ip4/{}/udp/{}/quic-v1",
            config.external_ip_addr,
            config.external_port
        )
        .parse()?
    );    
    process_swarm(swarm, rx).await?;
    Ok(())
}