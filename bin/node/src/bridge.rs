use std::{
    // time::{Instant, Duration},
    sync::Arc
};
use eyre::{
    // eyre,
    Result
};
use serde::Serialize;
use tracing::{
    info,
    warn
};
// use futures::stream::StreamExt;
use tokio::sync::mpsc;
use axum::{
    body::Bytes,
    extract::{Path, DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Router,
    response::{Json, IntoResponse}
};
use dashmap::DashMap;
use crate::blob_store::BlobRequest;

type Hash = [u8; 32];

// 1 GB
const MAX_BLOB_SIZE: usize = 1 * 1024 * 1024 * 1024;

pub enum UploadRequest {
    Success { id: String },
    Failed { 
        id: String,
        reason: String
    },
}

#[derive(Clone)]
struct AppState {
    blobs: Arc<DashMap<String, UploadStatus>>,
    blob_store_tx: mpsc::UnboundedSender<BlobRequest>,
}

impl AppState {
    pub fn new(bs_tx: mpsc::UnboundedSender<BlobRequest>) -> Self {
        AppState { 
            blobs: Arc::new(DashMap::new()),
            blob_store_tx: bs_tx
        }   
    }
}

#[derive(Clone, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum UploadStatus {
    Pending,
    Finalized,
    Failed { reason: String },
}

async fn get_status(
    State(state): State<AppState>,
    Path(id): Path<String>
) -> impl IntoResponse {
    match state.blobs.get(&id) {
        Some(status) => (StatusCode::OK, Json(status.clone())).into_response(),
        None => StatusCode::NOT_FOUND.into_response()
    }
}

async fn new_blob(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Bytes
) -> impl IntoResponse {
    info!(
        "Received a new blob(`{}`) ~{}MB from the artifact store.",
        id, 
        body.len() as f32 / 1_048_576f32
    );
    if state.blobs.contains_key(&id) {
        warn!(
            "Ignored duplicate blob(`{}`).",
            id
        );
        return StatusCode::INTERNAL_SERVER_ERROR
    }
    if let Err(e) = state.blob_store_tx.send(
        BlobRequest::Store(id.clone(), body.into())
    ) {
        warn!(
            "Failed to send blob to the blob center: `{:?}`",
            e
        );
        return StatusCode::INTERNAL_SERVER_ERROR
    }
    state.blobs.insert(id, UploadStatus::Pending);
    StatusCode::CREATED
}

pub async fn run(
    mut bridge_rx: mpsc::UnboundedReceiver<UploadRequest>,
    blob_tx: mpsc::UnboundedSender<BlobRequest>
) -> Result<()> {
    let state = AppState::new(blob_tx);
    let my_state = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                r = bridge_rx.recv() =>  match r {
                    Some(req) => {
                        match req {
                            UploadRequest::Success { id } => {
                                if !my_state.blobs.contains_key(&id) {
                                    warn!(
                                        "Upload success for a missing blob: `{}`",
                                        id
                                    );
                                }
                                my_state.blobs.insert(id, UploadStatus::Finalized);
                            }
                            UploadRequest::Failed{ id, reason } => {
                                warn!(
                                    "Upload failed for a missing blob: `{}`",
                                    id
                                );
                                my_state.blobs.insert(id, UploadStatus::Failed { reason });
                            }
                        }
                    }
                    None => {
                        warn!("Bridge channel is closed.");
                        break;
                    }
                }                
            }
        }
    });

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