use std::{
    time::{Instant, Duration},
    sync::Arc,
    collections::HashMap,
};
use eyre::{
    eyre,
    Result
};
use tracing::{
    info,
    warn
};
use serde::Serialize;
use futures::stream::StreamExt;
use tokio::{
    sync::mpsc,
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
use tokio_stream::wrappers::IntervalStream;
use rs_merkle::MerkleTree;
use crate::network::SwarmMessage;
use crate::blake3_wrapper::Blake3Hash;

pub type Hash = [u8; 32];

// 4 MB
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// blob lifetime: 4 hours
const RETENTION_TIME: u64 = 4 * 60 * 60;

pub enum BlobMessage {
    // comes from the bridge
    Store {
        id: String,
        data: Bytes
    },
    // comes from the swarm
    Persist {
        id: String,
        result: bool
    }
}

// 1 GB
const MAX_BLOB_SIZE: usize = 1 * 1024 * 1024 * 1024;

#[derive(Clone, Serialize)]
#[serde(tag = "upload_status", rename_all = "lowercase")]
enum UploadStatus {
    Pending,
    Finalized,
    Failed { reason: String },
}

#[derive(Clone)]
struct BridgeState {
    upload_status: Arc<DashMap<String, UploadStatus>>,
    blob_store_tx: mpsc::Sender<BlobMessage>,
}

impl BridgeState {
    pub fn new(blob_tx: mpsc::Sender<BlobMessage>) -> Self {
        BridgeState {
            upload_status: Arc::new(DashMap::new()),
            blob_store_tx: blob_tx
        }
    }
}

struct Blob {
    pub root_hash: Hash,
    pub bridge_id: String,
    pub data: Bytes,
    // [<start, end>]
    pub chunks: Vec<(usize, usize)>,
    pub created_at: u64,
}

struct BlobStore {
    // <root hash, blob>
    blobs: HashMap<String, Blob>,    
}

impl BlobStore {
    pub fn new() -> Self {
        BlobStore {
            blobs: HashMap::new(),
        }
    }

    pub fn store_blob(
        &mut self,
        id: String,
        data: Bytes,
    ) -> Result<()> {
        if 0 == data.len() {
            return Err(eyre!("Ignored empty blob."));
        }
        let merkle_tree = {
            let leaves: Vec<Hash> = data
                .chunks(CHUNK_SIZE)
                .map(|c| blake3::hash(c).into())
                .collect();
            MerkleTree::<Blake3Hash>::from_leaves(&leaves)
        };
        let root_hash = merkle_tree
            .root()
            .ok_or(eyre!("Couldn't get the merkle root."))?;
        if self.blobs.contains_key(&id) {
            return Err(eyre!("Duplicate blob: {}", hex::encode(root_hash)));
        }        
        let chunks: Vec<(usize, usize)> = (0..data.len())            
            .step_by(CHUNK_SIZE)
            .map(|start| {
                let end = (start + CHUNK_SIZE).min(data.len());
                (start, end)
            })
            .collect();
        info!(
            "Blob `{}` is cuhnked and now stored locally with root hash(`{}`). We'll now try to persist it globally.",
            id,
            hex::encode(root_hash)
        );
        self.blobs.insert(
            id.clone(), 
            Blob {
                root_hash: root_hash,
                bridge_id: id,
                data: data,
                chunks: chunks,
                created_at: Instant::now().elapsed().as_secs()
            }
        );

        Ok(())
    }

    pub fn remove_blob(&mut self, id: &str) {
        let _b = self.blobs.remove(id);
    }

    // periodic cleanup
    pub fn remove_stale_blobs(&mut self) {
        let now = Instant::now().elapsed().as_secs();
        // todo: inform the bridge
        self.blobs.retain(|_, v| {
            v.created_at + RETENTION_TIME < now
        })
    }
}

async fn start_blob_store(
    mut blob_rx: mpsc::Receiver<BlobMessage>,
    swarm_tx: mpsc::Sender<SwarmMessage>,
    bridge_state: BridgeState,
) -> Result<()> {
    let mut blob_store = BlobStore::new();        
    tokio::spawn(async move {
        // to remove stale blobs
        let mut timer_stale_blobs = IntervalStream::new(
            interval(Duration::from_secs(60))
        ).fuse();

        loop {
            tokio::select! {
                _i = timer_stale_blobs.select_next_some() => {                
                    blob_store.remove_stale_blobs();
                },
                                
                m = blob_rx.recv() =>  match m {
                    Some(msg) => {
                        match msg {
                            BlobMessage::Store{id, data} => {
                                match blob_store.store_blob(id.clone(), data) {
                                    Ok(()) => {
                                        if let Err(e) = swarm_tx.send(SwarmMessage::PersistBlob(id.clone())).await {
                                            warn!(
                                                "Failed to send Persist message to the Swarm channel: {}",
                                                e
                                            );
                                            bridge_state.upload_status.insert(
                                                id,
                                                UploadStatus::Failed{ reason: e.to_string() }
                                            );
                                            // todo: retry
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Store blob error: {}", e);
                                        bridge_state.upload_status.insert(
                                            id,
                                            UploadStatus::Failed{ reason: e.to_string() }
                                        );
                                        // todo: retry
                                        continue;                                        
                                    }

                                }                                
                            }
                            BlobMessage::Persist{id: _, result: _} => {}
                        }
                    }
                    None => {
                        warn!("Store blob channel is closed.");
                        break;
                    }
                }
            }
        }
    });
    Ok(())
}

async fn get_status(
    State(state): State<BridgeState>,
    Path(id): Path<String>
) -> impl IntoResponse {
    match state.upload_status.get(&id) {
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
    if state.upload_status.contains_key(&id) {
        warn!(
            "Ignored duplicate blob(`{}`).",
            id
        );
        return StatusCode::INTERNAL_SERVER_ERROR
    }
    if let Err(e) = state.blob_store_tx.send(
        BlobMessage::Store{id: id.clone(), data: body}
    ).await {
        warn!(
            "Failed to send blob to the blob center: `{:?}`",
            e
        );
        return StatusCode::INTERNAL_SERVER_ERROR
    }
    state.upload_status.insert(id, UploadStatus::Pending);
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8709").await.unwrap();
    info!("Artifact store bridge is up and listening on port 8709.");        
    axum::serve(listener, app)
        // .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    Ok(())
}

pub async fn run(
    tx: mpsc::Sender<BlobMessage>,
    rx: mpsc::Receiver<BlobMessage>,
    swarm_tx: mpsc::Sender<SwarmMessage>
) -> Result<()> {
    let bridge_state = BridgeState::new(tx);
    start_blob_store(rx, swarm_tx, bridge_state.clone()).await?;
    serve_bridge(bridge_state).await?;
    Ok(())
}
