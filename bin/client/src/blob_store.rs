use std::{
    time::{Instant, Duration},
    collections::HashMap,
};
use eyre::{eyre, Result};
use tracing::{info, warn};
use tokio::{
    task::JoinHandle,
    sync::{mpsc, oneshot},
    time::interval
};
use tokio_util::sync::CancellationToken;
use rs_merkle::MerkleTree;
use bytes::Bytes;
use crate::coordinator::CoordMessage;
use crate::bridge::{
    BridgeMessage, UploadStatus
};
use crate::blake3_wrapper::Blake3Hash;

pub type Hash = [u8; 32];


// 4 MB
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// blob lifetime: 4 hours
const BLOB_LIFETIME: u64 = 4 * 60 * 60;

pub enum BlobMessage {
    // src: the bridge
    NewBlob {
        id: String,
        data: Bytes,
    },
    // src: the coordinator
    // to transfer chunk bytes
    FetchChunks {
        id: String,
        chunks: Vec<Hash>,
        tx_reply: oneshot::Sender<Option<HashMap<Hash, Option<Bytes>>>>,
    },
    // src: the coordinator
    // to notify about blob distribution result
    StoreResult {
        id: String,
        success: bool,
        failure_reason: Option<String>,
    }
}

struct Blob {
    root_hash: Hash,
    data: Bytes,
    chunks: HashMap<Hash, Bytes>,
    merkle_tree: MerkleTree::<Blake3Hash>,
    created_at: Instant,
}

struct BlobStore {
    blobs: HashMap<String, Blob>,    
}

impl BlobStore {
    pub fn new() -> Self {
        BlobStore {
            blobs: HashMap::new(),
        }
    }

    pub fn add_blob(
        &mut self,
        id: String,
        data: Bytes,
    ) -> Result<()> {
        if data.is_empty() {
            return Err(eyre!("Empty blob."));
        }
        if self.blobs.contains_key(&id) {
            return Err(eyre!("Duplicate blob: {}", id));
        }
        let chunks: Vec<(Hash, Bytes)> = data
            .chunks(CHUNK_SIZE)
            .map(|c| (blake3::hash(c).into(), data.slice_ref(c)))
            .collect();        
        let merkle_tree = MerkleTree::<Blake3Hash>::from_leaves(
            &chunks
                .iter()
                .map(|(hash, _)| hash.clone())
                .collect::<Vec<Hash>>()
        );
        let root_hash = merkle_tree
            .root()
            .ok_or_else(|| eyre!("Couldn't get the merkle root."))?;        
        info!(
            "Blob `{}` is chunked and now stored locally with root hash(`{}`). We'll now try to persist it globally.",
            id,
            hex::encode(root_hash)
        );
        self.blobs.insert(
            id.clone(), 
            Blob {
                root_hash,
                data,
                chunks: chunks.into_iter().collect(),
                merkle_tree,
                created_at: Instant::now()
            }
        );

        Ok(())
    }

    pub fn remove_blob(&mut self, id: &str) {
        let _b = self.blobs.remove(id);
    }

    // periodic cleanup
    pub fn remove_stale_blobs(
        &mut self
    ) {
        let now = Instant::now();
        self.blobs.retain(|_, blob| {
            now.duration_since(blob.created_at).as_secs() < BLOB_LIFETIME
        });        
        // todo: inform the bridge?
    }
}

pub async fn run(
    mut rx_blob: mpsc::Receiver<BlobMessage>,
    tx_bridge: mpsc::Sender<BridgeMessage>,
    tx_coord: mpsc::Sender<CoordMessage>,
    shutdown: CancellationToken
) -> Result<JoinHandle<()>> {
    let mut blob_store = BlobStore::new();
    // to remove stale blobs
    let mut timer_stale_blobs = interval(Duration::from_secs(60));
    let jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    warn!("Received shutdown request.");
                    break
                },

                _i = timer_stale_blobs.tick() => {
                    blob_store.remove_stale_blobs();
                },

                m = rx_blob.recv() =>  match m {
                    Some(bm) => match bm {
                        BlobMessage::NewBlob { id, data } => {
                            match blob_store.add_blob(id.clone(), data) {
                                Ok(_) => {
                                    let blob = blob_store.blobs.get(&id).unwrap();
                                    if let Err(e) = tx_coord.send(CoordMessage::DistributeBlob {
                                        id: id.clone(),
                                        root_hash: blob.root_hash,
                                        chunk_hashes: blob.chunks.keys().cloned().collect()
                                    }).await {
                                        warn!("Failed to notify the Coordinator about the new blob: {:?}", e);
                                        // todo: retry or it'll stay pending forever
                                        continue
                                    }
                                }
                                Err(add_err) => {
                                    warn!("Add blob error: {:?}", add_err);
                                    if let Err(e) = tx_bridge.send(BridgeMessage::UpdateStatus {
                                        id,
                                        status: UploadStatus::Failed { reason: Some(add_err.to_string()) }
                                    }).await {
                                        warn!("Failed to notify the Bridge about this error: {:?}", e);
                                    }
                                    // todo: retry
                                    continue
                                }
                            }
                        }
                        BlobMessage::FetchChunks {
                            id,
                            chunks: requested_chunks,
                            tx_reply
                        } => {
                            let Some(blob) = blob_store.blobs.get(&id) else {
                                if let Err(e) = tx_reply.send(None) {
                                    warn!(
                                        "Failed to notify(reply) the Coordinator about the missing blob(`{}`): {:?}",
                                        id,
                                        e
                                    );
                                }
                                continue
                            };
                            let chunks: HashMap<Hash, Option<Bytes>> = requested_chunks
                                .into_iter()
                                .map(|hash| (hash, blob.chunks.get(&hash).cloned()))
                                .collect();
                            if let Err(e) = tx_reply.send(Some(chunks)) {
                                warn!(
                                    "Failed to reply blob(`{}`) chunks to the Coordinator: {:?}",
                                    id,
                                    e
                                );
                            }
                        }
                        BlobMessage::StoreResult {
                            id,
                            success,
                            failure_reason
                        } => {
                            if !blob_store.blobs.contains_key(&id) {
                                warn!(
                                    "Unsolicited blob(`{}`) store result `{}`.",
                                    id,
                                    success
                                );
                                continue
                            };

                            if success {
                                info!("Blob(`{}`) is now stored globally.", id);
                                // todo: archive blob meta
                                blob_store.remove_blob(&id);
                                if let Err(e) = tx_bridge.send(BridgeMessage::UpdateStatus {
                                    id,
                                    status: UploadStatus::Finalized
                                }).await {
                                    warn!("Failed to notify the Bridge about the finalized state of blob: {:?}", e);
                                }
                                // todo: how to guard against blob retransmission?
                            } else {
                                let reason = failure_reason.or_else(|| Some("not provided".to_string()));
                                info!(
                                    "Failed to store blob(`{}`) globally, reason: {}.",
                                    id,
                                    reason.as_ref().unwrap()
                                );
                                if let Err(e) = tx_bridge.send(BridgeMessage::UpdateStatus {
                                    id,
                                    status: UploadStatus::Failed { reason }
                                }).await {
                                    warn!("Failed to notify the Bridge about the failed state of the blob: {:?}", e);
                                }
                            }
                        }
                    }
                    None => {
                        warn!("Blob channel is closed.");
                        break
                    }
                }
            }
        }
    });
    Ok(jh)
}
