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
    request_response::OutboundRequestId,
    swarm::{
        Swarm,
        SwarmEvent
    },
    PeerId,
    multiaddr::Protocol
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, interval},
    task::JoinHandle
};
use tokio_util::sync::CancellationToken;
use tokio_stream::wrappers::IntervalStream;
use p2p::{GlobalBehaviour, GlobalBehaviourEvent};
pub use libp2p_stream::Control;

pub enum ToSwarm {
    StoragePermit {
        peers: Vec<PeerId>,
    },
    PublishManifest {
        blob_id: String,
        manifest: protocol::Manifest,
        expires_at: Instant,
        tx_reply: oneshot::Sender<kad::QueryId>
    },
    LookupManifest {
        blobs: Vec<String>
    },
    PullChunk {
        id: String,
        to: PeerId,
        tx_reply: oneshot::Sender<OutboundRequestId>
    }
}

// consumers handle inbound messages
pub enum FromSwarm {
    WouldStore(PeerId),
    Request {
        peer: PeerId,
        request_id: request_response::InboundRequestId,
        request: protocol::Request,
        channel: request_response::ResponseChannel<protocol::Response>
    },
    Response {
        peer: PeerId,
        request_id: OutboundRequestId,
        response: protocol::Response
    },
    PublishManifestResult {
        blob_id: String,
        query_id: kad::QueryId,
        succeeded: bool,
    },
    ManifestIsReady {
        id: String,
        manifest: protocol::Manifest
    }
}

const BLOB_MANIFEST_NAMESPACE: &[u8] = b"/gozara/manifest/blob/1.0.0/";

pub async fn process_swarm(
    mut swarm: Swarm<GlobalBehaviour>,
    mut rx: mpsc::Receiver<ToSwarm>,
    tx_handler: mpsc::Sender<FromSwarm>,
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
                        ToSwarm::StoragePermit { peers } => {
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
                        ToSwarm::PublishManifest { blob_id, manifest, expires_at, tx_reply } => {
                            let value = match bincode::serialize(&manifest) {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(blob = %blob_id, error = %e,
                                        "Failed to serialize the given manifest."
                                    );
                                    continue
                                }
                            };
                            let key = kad::RecordKey::new(
                                &[BLOB_MANIFEST_NAMESPACE,
                                blob_id.as_bytes()].concat()
                            );
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
                                    warn!(blob = %blob_id, error = %e,
                                        "Failed to put the manifest record on DHT."
                                    );
                                    continue
                                }
                            };
                            debug!(blob = %blob_id, query = %query_id,
                                "Publish manifest is initiated."
                            );
                            let _ = tx_reply.send(query_id);
                        }
                        ToSwarm::LookupManifest { blobs } => {
                            for blob_id in blobs.into_iter() {
                                let key = kad::RecordKey::new(
                                    &[BLOB_MANIFEST_NAMESPACE,
                                    blob_id.as_bytes()].concat()
                                );
                                let query_id = swarm
                                    .behaviour_mut()
                                    .kademlia
                                    .get_record(key);
                                debug!(blob = %blob_id, query = %query_id,
                                    "Initiating manifest lookup."
                                );
                            }
                        }
                        ToSwarm::PullChunk { id, to, tx_reply } => {
                            let req_id = swarm
                                .behaviour_mut()
                                .req_resp
                                .send_request(&to, protocol::Request::PullChunk(id.clone()));
                            let _ = tx_reply.send(req_id);
                            debug!(chunk = %id, from = %to, req_id = %req_id,
                                "Requested chunk pull."
                            );
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
                        debug!(to = %peer_id, endpoint = ?endpoint, "A connection has been established.");
                    }
                    // <identify>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Identify(identify::Event::Received {
                        peer_id,
                        info,
                        ..
                    })) => {
                        debug!("Received identify from `{peer_id}`: {info:#?}");
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
                                let _ = tx_handler.try_send(FromSwarm::WouldStore(peer_id));
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
                    // put record
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        id: query_id,
                        result: kad::QueryResult::PutRecord(result),
                        stats,
                        ..
                    })) => {
                        let (key, succeeded) = match result {
                            Ok(kad::PutRecordOk { key }) => (key, true),
                            Err(e) => (e.key().clone(), false)
                        };
                        let Some(id) = key
                            .as_ref()
                            .strip_prefix(BLOB_MANIFEST_NAMESPACE)
                            .and_then(|b| str::from_utf8(b).ok())
                        else {
                            continue
                        };
                        debug!(key = ?key, query = %query_id, stats = ?stats,
                            succeeded = %succeeded, "Received PutRecord result."
                        );

                        if let Err(e) = tx_handler.try_send(FromSwarm::PublishManifestResult {
                            blob_id: id.to_owned(), query_id, succeeded
                        }) {
                            debug!(blob = id, error = %e,
                                "Failed to notify the publish manifest result."
                            );
                        }
                    }
                    // get record
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                        id: query_id,
                        result: kad::QueryResult::GetRecord(
                            Ok(kad::GetRecordOk::FoundRecord(peer_record))
                        ),
                        ..
                    })) => {
                        let Some(tmp) = peer_record
                            .record
                            .key
                            .as_ref()
                            .strip_prefix(BLOB_MANIFEST_NAMESPACE)
                        else {
                            continue
                        };
                        let Ok(id) = String::from_utf8(tmp.to_owned()) else {
                            continue
                        };
                        debug!(blob = %id, query = %query_id, "GetRecord succeeded.");
                        let manifest: protocol::Manifest = match bincode::deserialize(&peer_record.record.value) {
                            Ok(m) => m,
                            Err(e) => {
                                debug!(error = %e, "Manifest decode failed.");
                                continue
                            }
                        };
                        // do not notify failures and let them timeout
                        if let Err(e) = tx_handler.try_send(
                            FromSwarm::ManifestIsReady { id: id.clone(), manifest }
                        ) {
                            debug!(blob = %id, error = %e,
                                "Failed to notify about a ready manifest."
                            );
                        }
                    }
                    // request-response: <protocol>
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                        peer,
                        message: request_response::Message::Request { request, channel, request_id },
                        ..
                    })) => {
                        if let Err(e) = tx_handler.try_send(
                            FromSwarm::Request { peer, request_id, request, channel }
                        ) {
                            debug!(request = %request_id, error = %e,
                                "Failed to notify about a new request."
                            );
                        }
                    }
                    SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                        peer,
                        message: request_response::Message::Response { response, request_id },
                        ..
                    })) => {
                        if let Err(e) = tx_handler.try_send(
                            FromSwarm::Response { peer, request_id, response }
                        ) {
                            debug!(request = %request_id, error = %e,
                                "Failed to notify about a new response."
                            );
                        }
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

pub async fn request_pull(
    id: &str,
    to: PeerId,
    tx: mpsc::Sender<ToSwarm>
) -> Option<OutboundRequestId> {
    let (tx_reply, rx) = oneshot::channel::<OutboundRequestId>();
    if let Err(e) = tx.send(ToSwarm::PullChunk {
        id: id.to_owned(), to, tx_reply
    }).await {
        warn!(chunk = %id, error = %e,
            "Failed to send the `pull chunk` to the Swarm channel."
        );
        return None
    }
    let req_id = match rx.await {
        Ok(r) => r,
        Err(_) => {
            warn!("Request pull reply channel is closed.");
            return None
        }
    };
    Some(req_id)
}