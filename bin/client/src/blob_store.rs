use std::{
    time::{Instant, Duration},
    sync::Arc,
    collections::HashMap,
};
use eyre::{eyre, Result};
use tracing::{info, warn};
use serde::Serialize;
// use futures::stream::StreamExt;
use tokio::{
    task::JoinHandle,
    sync::{mpsc, oneshot},
    time::interval
};
use axum::{
    body::Bytes,
    extract::{Path, DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Router,
    response::{Json, IntoResponse}
};
use dashmap::DashMap;
// use tokio_stream::wrappers::IntervalStream;
use rs_merkle::MerkleTree;
use crate::coordinator::CoordMessage;
use crate::blake3_wrapper::Blake3Hash;

pub type Hash = [u8; 32];

// 4 MB
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// blob lifetime: 4 hours
const BLOB_LIFETIME: u64 = 4 * 60 * 60;

enum InternalMessage {
    // src: the bridge
    NewBlob {
        id: String,
        data: Bytes,
    }
}

pub enum BlobMessage {
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
            .ok_or(eyre!("Couldn't get the merkle root."))?;        
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

fn start_blob_store(
    mut rx_internal: mpsc::Receiver<InternalMessage>,
    mut rx_blob: mpsc::Receiver<BlobMessage>,
    tx_coord: mpsc::Sender<CoordMessage>,
    bridge_state: BridgeState,
) -> JoinHandle<()> {
    let mut blob_store = BlobStore::new();
    // to remove stale blobs
    let mut timer_stale_blobs = interval(Duration::from_secs(60));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _i = timer_stale_blobs.tick() => {
                    blob_store.remove_stale_blobs();
                },

                m = rx_internal.recv() =>  match m {
                    Some(im) => match im {
                        InternalMessage::NewBlob { id, data } => {
                            match blob_store.add_blob(id.clone(), data) {
                                Ok(_) => {
                                    let blob = blob_store.blobs.get(&id).unwrap();
                                    if let Err(e) = tx_coord.send(CoordMessage::DistributeBlob {
                                        id: id.clone(),
                                        root_hash: blob.root_hash,
                                        chunk_hashes: blob.chunks.keys().cloned().collect()
                                    }).await {
                                        warn!(
                                            "Failed to send distribute message to the coordinator's channel: {}",
                                            e
                                        );
                                        // todo: retry or it'll stay pending forever
                                        continue
                                    }
                                    bridge_state.upload_status_map.insert(
                                        id,
                                        UploadStatus::Pending
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "Store blob error: {}",
                                        e
                                    );
                                    bridge_state.upload_status_map.insert(
                                        id,
                                        UploadStatus::Failed{ reason: Some(e.to_string()) }
                                    );
                                    // todo: retry
                                    continue
                                }

                            }
                        }
                    }
                    None => {
                        warn!("Internal channel is closed.");
                        break
                    }
                },
                                
                m = rx_blob.recv() =>  match m {
                    Some(bm) => match bm {
                        BlobMessage::FetchChunks {
                            id,
                            chunks: requested_chunks,
                            tx_reply
                        } => {
                            let Some(blob) = blob_store.blobs.get(&id) else {
                                if let Err(e) = tx_reply.send(None) {
                                    warn!(
                                        "Failed to notify the coordinator about the missing blob(`{}`): {:?}",
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
                                    "Failed to send chunks of the blob(`{}`) to the coordinator: {:?}",
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
                                {
                                    let Some(mut status) = bridge_state.upload_status_map.get_mut(&id) else {
                                        warn!(
                                            "Missing blob(`{}`) to update its status(`success`).",
                                            id
                                        );
                                        continue
                                    };
                                    *status = UploadStatus::Finalized;
                                }
                                info!(
                                    "Blob(`{}`) is now stored globally.",
                                    id
                                );
                                blob_store.remove_blob(&id);
                                // todo: how to guard against blob retransmission?
                            } else {
                                {
                                    let Some(mut status) = bridge_state.upload_status_map.get_mut(&id) else {
                                        warn!(
                                            "Missing blob(`{}`) to update its status(`failed`).",
                                            id
                                        );
                                        continue
                                    };
                                    *status = UploadStatus::Failed { reason: failure_reason.clone() };
                                }
                                info!(
                                    "Failed to store blob(`{}`) globally, reason: {}.",
                                    id,
                                    if let Some(r) = failure_reason { r } else { "not provided".to_string() }
                                );                                    
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
    })
}

// 1 GiB
const MAX_BLOB_SIZE: usize = 1 * 1024 * 1024 * 1024;

#[derive(Clone, Serialize)]
#[serde(tag = "upload_status", rename_all = "lowercase")]
enum UploadStatus {
    Pending,
    Finalized,
    Failed { reason: Option<String> },
}

#[derive(Clone)]
struct BridgeState {
    upload_status_map: Arc<DashMap<String, UploadStatus>>,
    tx_internal: mpsc::Sender<InternalMessage>
}

impl BridgeState {
    pub fn new(tx_internal: mpsc::Sender<InternalMessage>) -> Self {
        BridgeState {
            upload_status_map: Arc::new(DashMap::new()),
            tx_internal
        }
    }
}

async fn get_status(
    State(state): State<BridgeState>,
    Path(id): Path<String>
) -> impl IntoResponse {
    match state.upload_status_map.get(&id) {
        Some(status) => (StatusCode::OK, Json(status.clone())).into_response(),
        None => StatusCode::NOT_FOUND.into_response()
    }
}

async fn new_blob(
    State(state): State<BridgeState>,
    Path(id): Path<String>,
    body: Bytes
) -> impl IntoResponse {
    info!(
        "Received a new blob(`{}`) ~{}MB from the artifact store.",
        id, 
        body.len() as f32 / 1_048_576f32
    );
    if state.upload_status_map.contains_key(&id) {
        warn!(
            "Ignored duplicate blob(`{}`).",
            id
        );
        return StatusCode::CONFLICT
    }

    if let Err(e) = state.tx_internal.send(
        InternalMessage::NewBlob {
            id: id.clone(),
            data: body
    }).await {
        warn!(
            "Failed to send blob to the blob center: `{:?}`",
            e
        );
        return StatusCode::INTERNAL_SERVER_ERROR
    }
    StatusCode::CREATED
}

async fn serve_bridge(
    state: BridgeState,
) -> Result<()> {    
    let app = Router::new()
        .route("/status/{id}", get(get_status))   
        .route("/blob/{id}", post(new_blob))
        .with_state(state)
        .layer(DefaultBodyLimit::max(MAX_BLOB_SIZE));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8709").await?;
    info!("Artifact store bridge is up and listening on port 8709.");        
    axum::serve(listener, app)
        // .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    Ok(())
}

pub async fn run(
    rx_blob: mpsc::Receiver<BlobMessage>,
    tx_coord: mpsc::Sender<CoordMessage>
) -> Result<()> {
    let (tx_internal, rx_internal) = mpsc::channel::<InternalMessage>(32);
    let bridge_state = BridgeState::new(tx_internal);
    let _jh = start_blob_store(rx_internal, rx_blob, tx_coord, bridge_state.clone());
    serve_bridge(bridge_state).await?;
    Ok(())
}
