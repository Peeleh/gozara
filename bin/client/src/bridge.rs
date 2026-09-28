use std::sync::Arc;
use eyre::Result;
use tracing::{info, warn};
use serde::Serialize;
use tokio::{
    task::JoinHandle,
    sync::mpsc,
};
use axum::{
    body::Bytes,
    extract::{Path, DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Router,
    response::{Json, IntoResponse}
};
use tower::limit::ConcurrencyLimitLayer;
use dashmap::{
    DashMap,
    mapref::entry::Entry
};
use crate::blob_store::BlobMessage;

// 1 GiB
const MAX_BLOB_SIZE: usize = 1 * 1024 * 1024 * 1024;
// 8 GiB or max 8 new blob requests to the http bridge
const TOTAL_INBOUND_BLOB_PRESSURE: usize = 8;

#[derive(Clone, Serialize, Debug)]
#[serde(tag = "upload_status", rename_all = "lowercase")]
pub enum UploadStatus {
    Pending,
    Finalized,
    Failed { reason: Option<String> },
}

pub enum BridgeMessage {
    UpdateStatus {
        id: String,
        status: UploadStatus
    }
}

#[derive(Clone)]
pub struct BridgeState {
    upload_status_map: Arc<DashMap<String, UploadStatus>>,
    tx_blob: mpsc::Sender<BlobMessage>
}

impl BridgeState {
    pub fn new(tx_blob: mpsc::Sender<BlobMessage>) -> Self {
        BridgeState {
            upload_status_map: Arc::new(DashMap::new()),
            tx_blob
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
    // todo: this check should be a match and the pending, ... flow should be strictly scrutinized
    match state.upload_status_map.entry(id.clone()) {
        Entry::Occupied(_) => {
            warn!("Ignored duplicate blob(`{}`).", id);
            return StatusCode::CONFLICT
        }
        Entry::Vacant(entry) => {
            entry.insert(UploadStatus::Pending);
        }
    }

    if let Err(e) = state.tx_blob.send(
        BlobMessage::NewBlob {
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

pub async fn run(
    mut rx_bridge: mpsc::Receiver<BridgeMessage>,
    tx_blob: mpsc::Sender<BlobMessage>
) -> Result<JoinHandle<()>> {
    let state = BridgeState::new(tx_blob);
    let state_server = state.clone();
    let mut server_jh = tokio::spawn(async move {
        let app = Router::new()
            .route("/status/{id}", get(get_status))   
            .route("/blob/{id}", post(new_blob)
                .layer(ConcurrencyLimitLayer::new(TOTAL_INBOUND_BLOB_PRESSURE))
            )
            .with_state(state_server)
            .layer(DefaultBodyLimit::max(MAX_BLOB_SIZE));

        let listener = match tokio::net::TcpListener::bind("127.0.0.1:8709").await {
            Ok(l) => l,
            Err(e) => {
                warn!("Listener bind error: {:?}", e);
                return
            }
        };
        info!("Artifact store HTTP bridge is up and listening on port 8709.");
        if let Err(e) = axum::serve(listener, app)
            // .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
        {
            warn!("Failed to start HTTP bridge server: {:?}", e);
        }
    });
    let mut msg_jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                m = rx_bridge.recv() => match m {
                    Some(b_msg) => {
                        match b_msg {
                            BridgeMessage::UpdateStatus {
                                id,
                                status
                            } => {
                                match state.upload_status_map.entry(id.clone()) {
                                    Entry::Occupied(mut entry) => {
                                        entry.insert(status);
                                    }
                                    Entry::Vacant(_) => {
                                        warn!(
                                            "Unsolicited status update(`{:?}`) for blob(`{}`).",
                                            status,
                                            id
                                        );
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        warn!("Bridge message channel is closed.");
                        break
                    }
                }
            }
        }
    });
    Ok(tokio::spawn(async move {
        tokio::select! {
            r = &mut server_jh => {
                msg_jh.abort();
                if let Err(e) = r {
                    warn!("The Axum server task panicked: {e:?}");
                }
            },

            r = &mut msg_jh => {
                server_jh.abort();
                if let Err(e) = r {
                    warn!("The bridge channel task panicked: {e:?}");
                }
            },
        }
    }))
}
