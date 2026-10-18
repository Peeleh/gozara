pub mod p2p;
pub mod protocol;
pub mod blob_transfer;

use std::time::Duration;
use futures::stream::StreamExt;
use eyre::Result;
use tracing::{info, warn, debug};
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
    time::{Instant, interval},
    task::JoinHandle
};
use tokio_util::sync::CancellationToken;
use tokio_stream::wrappers::IntervalStream;
use common::types::BlobManifest;
use p2p::{GlobalBehaviour, GlobalBehaviourEvent};
pub use libp2p_stream::Control;

pub enum ToSwarm {
    RequestStoragePermits { 
        peers: Vec<PeerId>,
    },
    PublishManifest {
        blob_id: String,
        manifest: BlobManifest,
        expires_at: Instant
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
    },
    PublishManifestResult {
        blob_id: String,
        succeeded: bool,
    }
}

const BLOB_MANIFEST_NAMESPACE: &[u8] = b"/gozara/manifest/blob/";

pub async fn process_swarm(
    mut swarm: Swarm<GlobalBehaviour>,
    mut rx: mpsc::Receiver<ToSwarm>,
    tx_handler: mpsc::Sender<HandlerMessage>,
    shutdown: CancellationToken,
) -> Result<JoinHandle<()>> {
    let jh = tokio::spawn(async move {
        // to update kademlia tables
        let mut timer_peer_discovery = IntervalStream::new(
            interval(Duration::from_secs(60))
        ).fuse();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Received shutdown request.");
                    break
                },
                // try to discover new peers
                _i = timer_peer_discovery.select_next_some() => {                
                    let random_peer_id = PeerId::random();
                    debug!("Searching for the closest peers to `{random_peer_id}`");
                    swarm
                        .behaviour_mut()
                        .kademlia
                        .get_closest_peers(random_peer_id);
                },
                // ToSwarm messages
                r = rx.recv() =>  match r {
                    Some(m) => match m {
                        ToSwarm::RequestStoragePermits { peers } => {
                            for peer in peers.iter() {
                                let _ = swarm
                                    .behaviour_mut()
                                    .req_resp
                                    .send_request(
                                        peer,
                                        protocol::Request::StoragePermit
                                    );
                            }
                        }
                        ToSwarm::PublishManifest { blob_id, manifest, expires_at } => {
                            let value = match bincode::serialize(&manifest) {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(blob = %blob_id, error = %e, "Failed to serialize the given manifest.");
                                    continue
                                }
                            };
                            let key = kad::RecordKey::new(&[BLOB_MANIFEST_NAMESPACE, blob_id.as_bytes()].concat());
                            let record = kad::Record {
                                key: key,
                                value: value,
                                publisher: Some(*swarm.local_peer_id()),
                                expires: Some(expires_at.into())
                            };
                            let query_id = match swarm
                                .behaviour_mut()
                                .kademlia
                                .put_record(record, kad::Quorum::One)
                            {
                                Ok(qid) => qid,
                                Err(e) => {
                                    warn!(blob = %blob_id, error = %e, "Failed to put the manifest record on DHT.");
                                    continue
                                }
                            };
                            debug!(blob = %blob_id, query = %query_id, "Publish manifest is initiated.");
                            // todo: follow up
                        }
                    }
                    None => {
                        warn!("Swarm channel is closed.");
                        break
                    }
                },                
                
                // libp2p events
                event = swarm.select_next_some() => match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        debug!("Local node is listening on {address}");
                    }
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        endpoint,
                        ..
                    } => {
                        debug!(
                            "A connection has been established to {} via {:?}",
                            peer_id,
                            endpoint
                        );                    
                    }
                    // <identify>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Identify(identify::Event::Received {
                        peer_id,
                        info,
                        ..
                    })) => {
                        debug!("Received identify from `{peer_id}`: {info:#?}`");
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
                            debug!("Added public address of the peer to the DHT: {}", address);
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, address);
                        }                      
                    }
                    // <gossipsub>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        message,
                        ..
                    })) => {
                        let Some(peer_id) = message.source else { continue };
                        match bincode::deserialize::<u8>(&message.data) {
                            Ok(_) => {
                                let _ = tx_handler.try_send(HandlerMessage::WouldStore { peer_id });
                            }
                            Err(e) => {
                                debug!("Gossip message decode error: `{e:?}`");
                            }
                        }
                    }
                    // <kademlia>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                        ..
                    })) => {
                        debug!("Query for the closest peers lookup is finished: {:?}", ok.peers);
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        result: kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout { .. })),
                        ..
                    })) => {
                        debug!("Query for the closest peers timed out.");
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        id: query_id,
                        result: kad::QueryResult::PutRecord(Ok(kad::PutRecordOk{ key })),
                        stats,
                        ..
                    })) => {
                        let Some(id) = key.as_ref().strip_prefix(BLOB_MANIFEST_NAMESPACE) else {
                            continue
                        };
                        debug!(key = ?key, query = %query_id, stats = ?stats, "PutRecord was successful.");
                        let Ok(blob_id) = String::from_utf8(id.to_owned()) else {
                            continue
                        };
                        let _ = tx_handler.send(HandlerMessage::PublishManifestResult {
                            blob_id,
                            succeeded: true
                        });
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        id: query_id,
                        result: kad::QueryResult::PutRecord(Err(e)),
                        stats,
                        ..
                    })) => {
                        let key = e.key();
                        let Some(id) = key.as_ref().strip_prefix(BLOB_MANIFEST_NAMESPACE) else {
                            continue
                        };
                        debug!(key = ?key, query = %query_id, stats = ?stats, "PutRecord failed.");
                        let Ok(blob_id) = String::from_utf8(id.to_owned()) else {
                            continue
                        };
                        let _ = tx_handler.send(HandlerMessage::PublishManifestResult {
                            blob_id,
                            succeeded: false
                        });
                    }
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
                        let _ = tx_handler.try_send(HandlerMessage::Request {
                            peer_id: peer_id,
                            request_id: request_id,
                            request: request,
                            channel: channel
                        });
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
                        let _ = tx_handler.send(HandlerMessage::Response {
                            peer_id: peer_id,
                            request_id: request_id,
                            response: response
                        });
                    },
                    _ => {
                        debug!("{:#?}", event);
                    }
                },
            }
        }
    });
    Ok(jh)
}
