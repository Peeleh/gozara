use std::{
    time::Instant,
    sync::Arc,
};
use eyre::Result;
use tracing::{info, warn};
use serde::Serialize;
use tokio::{
    task::JoinHandle,
    sync::mpsc,
};
use tokio_util::{
    sync::CancellationToken,
    io::ReaderStream
};
use axum::{
    body::Bytes,
    extract::{Path, DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Router,
    response::{Json, IntoResponse}
};
use tower::{
    ServiceExt,
    limit::ConcurrencyLimitLayer
};
use tower_http::services::ServeFile;
use dashmap::{
    DashMap,
    mapref::entry::Entry
};
use crate::coordinator::Message as CoordMessage;

// 1 GiB
const MAX_BLOB_SIZE: usize = 1 * 1024 * 1024 * 1024;
// 8 GiB or max 8 new blob requests to the http bridge
const TOTAL_INBOUND_BLOB_PRESSURE: usize = 8;

#[derive(Clone, Serialize, Debug)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum Status {
    // outbound blobs
    Pending,
    // inbound blobs
    Missing,
    Finalized,
    Failed { reason: Option<String> },
}

#[derive(Clone, Debug)]
struct TimestampedStatus {
    at: Instant,
    status: Status,
}

pub enum Message {
    UpdateStatus {
        id: String,
        status: Status
    }
}

#[derive(Clone)]
pub struct BridgeState {
    status_map: Arc<DashMap<String, Vec<TimestampedStatus>>>,
    tx_coord: mpsc::Sender<CoordMessage>,
}

impl BridgeState {
    pub fn new(tx_coord: mpsc::Sender<CoordMessage>) -> Self {
        BridgeState {
            status_map: Arc::new(DashMap::new()),
            tx_coord
        }
    }
}

async fn get_status(
    State(state): State<BridgeState>,
    Path(id): Path<String>
) -> impl IntoResponse {
    match state.status_map.get(&id) {
        Some(status_list) => {
            let most_recent_status = &status_list.last().unwrap().status;
            (StatusCode::OK, Json(most_recent_status)).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response()
    }
}

async fn get_blob(
    State(state): State<BridgeState>,
    Path(id): Path<String>,
    req: axum::extract::Request
) -> impl IntoResponse {
    let status = state.status_map
        .get(&id)
        .map(|status_list| status_list.last().unwrap().status.clone())
        .unwrap_or(Status::Missing);
    match status {
        Status::Finalized  => {
            // ServeFile::new(path)
            //     .oneshot(req)
            //     .await
            //     .into_response()
            StatusCode::ACCEPTED.into_response()
        }
        Status::Missing => {
            if let Err(e) = state.tx_coord.send(CoordMessage::GetBlob {
                id: id.clone(),
            }).await {
                warn!("Failed to send the get blob message to the blob store: {e:?}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }

            StatusCode::ACCEPTED.into_response()
        }
        _  => {
            (StatusCode::ACCEPTED, Json(status.clone())).into_response()
        }
    }
}

async fn new_blob(
    State(state): State<BridgeState>,
    Path(id): Path<String>,
    body: Bytes
) -> impl IntoResponse {
    info!(
        "Received a new blob(`{}`) ~{:.1}MiB from the artifact store.",
        id, 
        body.len() as f32 / 1_048_576f32
    );
    // todo: this check should be a match and the pending, ... flow should be strictly scrutinized
    match state.status_map.entry(id.clone()) {
        Entry::Occupied(_) => {
            warn!("Ignored duplicate blob(`{}`).", id);
            return StatusCode::CONFLICT
        }
        Entry::Vacant(entry) => {
            entry.insert(vec![TimestampedStatus {
                at: Instant::now(),
                status: Status::Pending
            }]);
        }
    }

    if let Err(e) = state.tx_coord.send(
        CoordMessage::NewBlob {
            id: id.clone(),
            data: body
    }).await {
        warn!("Failed to send the new blob to the blob store: {e:?}");
        let _ = state.status_map.remove(&id);
        return StatusCode::INTERNAL_SERVER_ERROR
    }
    StatusCode::CREATED
}

pub async fn run(
    mut rx_bridge: mpsc::Receiver<Message>,
    tx_coord: mpsc::Sender<CoordMessage>,
    shutdown: CancellationToken
) -> Result<JoinHandle<()>> {
    let state = BridgeState::new(tx_coord);
    let state_server = state.clone();
    let shutdown_bridge = shutdown.clone();
    let app = Router::new()
        .route("/status/{id}", get(get_status))
        .route("/blob/{id}", post(new_blob)
            .layer(ConcurrencyLimitLayer::new(TOTAL_INBOUND_BLOB_PRESSURE))
        )
        .with_state(state_server)
        .layer(DefaultBodyLimit::max(MAX_BLOB_SIZE));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8709").await?;
    let mut server_jh = tokio::spawn(async move {
        info!("Artifact store HTTP bridge is up and listening on port 8709.");
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_bridge.cancelled_owned())
            .await
        {
            warn!("Failed to start HTTP bridge server: {:?}", e);
        }
    });
    let shutdown_msg = shutdown.clone();
    let mut msg_jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_msg.cancelled() => {
                    warn!("Received shutdown request.");
                    break
                },

                m = rx_bridge.recv() => match m {
                    Some(b_msg) => {
                        match b_msg {
                            Message::UpdateStatus {
                                id,
                                status
                            } => {
                                match state.status_map.entry(id.clone()) {
                                    Entry::Occupied(mut entry) => {
                                        entry.get_mut().push(TimestampedStatus {
                                            at: Instant::now(),
                                            status: status
                                        });
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
                shutdown.cancel();
                if let Err(e) = r {
                    warn!("The Axum server task panicked: {e:?}");
                }
            },

            r = &mut msg_jh => {
                shutdown.cancel();
                if let Err(e) = r {
                    warn!("The bridge channel task panicked: {e:?}");
                }
            },
        }
    }))
}
