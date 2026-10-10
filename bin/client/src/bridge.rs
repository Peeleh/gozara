use std::{
    time::{Instant, Duration},
    sync::Arc,
};
use futures::StreamExt;
use eyre::Result;
use tracing::{info, warn};
use serde::Serialize;
use tokio::{
    task::{JoinHandle, spawn_blocking},
    sync::{mpsc, Semaphore, OwnedSemaphorePermit},
    time::timeout
};
use tokio_util::{
    sync::CancellationToken,
    io::ReaderStream
};
use rs_merkle::MerkleTree;
use blake3::Hash;
use bytes::BytesMut;
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, StatusCode},
    routing::{get, post},
    Router,
    response::{Json, Response, IntoResponse}
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
use crate::blake3_wrapper::Blake3;
use crate::coordinator::Message as CoordMessage;

// 1 GiB
const MAX_BLOB_SIZE: usize = 1 * 1024 * 1024 * 1024;

// 4 MB
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// each permit = 4 Mib, in total we want 6 Gib worth of blob upload permits aka memory pressure
const TOTAL_CHUNK_BUDGET: usize = 1536;

// deadline to finish each chunk streaming
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);


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

struct Admission {
    map: Arc<DashMap<String, Vec<TimestampedStatus>>>,
    id: String,
    committed: bool
}

impl Admission {
    fn new(state: &BridgeState, id: &str) -> Result<Self, StatusCode> {
        match state.status_map.entry(id.to_string()) {
            Entry::Occupied(_) => Err(StatusCode::CONFLICT),
            Entry::Vacant(e) => {
                e.insert(vec![TimestampedStatus {
                    at: Instant::now(),
                    status: Status::Pending
                }]);
                Ok(Self {
                    map: state.status_map.clone(),
                    id: id.to_string(),
                    committed: false
                })
            }
        }
    }

    fn commit(mut self) {
        self.committed = true
    }
}

// remove the blob from the map if uploads is cancelled mid-journey
impl Drop for Admission {
    fn drop(&mut self) {
        if !self.committed {
            self.map.remove(&self.id);
        }
    }
}

// Error type for the new_blob: status code + 'Retry-after: 5s' on 503
struct Reject(StatusCode);

impl From<StatusCode> for Reject {
    fn from(code: StatusCode) -> Self { Reject(code) }
}

impl IntoResponse for Reject {
    fn into_response(self) -> Response {
        match self.0 {
            StatusCode::SERVICE_UNAVAILABLE => {
                (self.0, [(header::RETRY_AFTER, "5")]).into_response()
            },
            code => code.into_response()
        }
    }
}

async fn hash_chunk(
    hashing: Arc<Semaphore>,
    chunk: Bytes
) -> Result<(Hash, Bytes), StatusCode> {
    let permit = hashing.acquire_owned().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let c = chunk.clone();
    let hash = spawn_blocking(move || {
        let h = blake3::hash(&c);
        drop(permit);
        h
    }).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((hash, chunk))
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

pub struct AdmittedBlob {
    pub id: String,
    pub root_hash: Hash,
    pub merkle_tree: MerkleTree<Blake3>,
    pub chunks: Vec<(Hash, Bytes)>,
    pub _permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct BridgeState {
    hashing_permit: Arc<Semaphore>,
    blob_budget: Arc<Semaphore>,
    status_map: Arc<DashMap<String, Vec<TimestampedStatus>>>,
    tx_coord: mpsc::Sender<CoordMessage>,
}

impl BridgeState {
    pub fn new(tx_coord: mpsc::Sender<CoordMessage>) -> Self {
        BridgeState {
            hashing_permit: Arc::new(Semaphore::new(2)),
            blob_budget: Arc::new(Semaphore::new(TOTAL_CHUNK_BUDGET)),
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
    headers: header::HeaderMap,
    body: Body
) -> Result<(StatusCode, String), Reject> {
    // check for blob size first and whether we have enough budget to admit it
    let declared_blob_len: usize = headers.get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse().ok())
        .ok_or(StatusCode::LENGTH_REQUIRED)?;
    if declared_blob_len == 0 {
        return Err(StatusCode::BAD_REQUEST.into())
    }
    let num_chunks = declared_blob_len.div_ceil(CHUNK_SIZE);
    if declared_blob_len > MAX_BLOB_SIZE || num_chunks > TOTAL_CHUNK_BUDGET {
        return Err(StatusCode::PAYLOAD_TOO_LARGE.into())
    }
    let admission = Admission::new(&state, &id)?;
    let permit = state.blob_budget.clone()
        .try_acquire_many_owned(num_chunks as u32)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    // stream in chunks
    let mut stream = body.into_data_stream();
    let mut buf = BytesMut::with_capacity(CHUNK_SIZE);
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut received = 0usize;
    while let Some(frame) = timeout(BODY_IDLE_TIMEOUT, stream.next())
        .await
        .map_err(|_| StatusCode::REQUEST_TIMEOUT)?
    {
        let mut data = frame.map_err(|_| StatusCode::BAD_REQUEST)?;
        received += data.len();
        if received > declared_blob_len {
            return Err(StatusCode::BAD_REQUEST.into())
        }
        while !data.is_empty() {
            let take = (CHUNK_SIZE - buf.len()).min(data.len());
            buf.extend_from_slice(&data.split_to(take));
            if buf.len() == CHUNK_SIZE {
                let full = std::mem::replace(&mut buf, BytesMut::with_capacity(CHUNK_SIZE)).freeze();
                chunks.push(hash_chunk(state.hashing_permit.clone(), full).await?);
            }
        }
    }
    if received != declared_blob_len {
        return Err(StatusCode::BAD_REQUEST.into())
    }
    if !buf.is_empty() {
        chunks.push(hash_chunk(state.hashing_permit.clone(), buf.freeze()).await?);
    }
    // merklize it
    let leaves: Vec<[u8; 32]> = chunks.iter().map(|(h, _)| *h.as_bytes()).collect();
    let merkle_tree = MerkleTree::<Blake3>::from_leaves(leaves.as_slice());
    let root_hash = merkle_tree.root().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    state.tx_coord
        .send(CoordMessage::NewBlob(AdmittedBlob { id, root_hash: root_hash.into(), merkle_tree, chunks, _permit: permit }))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    admission.commit();
    Ok((StatusCode::CREATED, hex::encode(root_hash)))
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
        .route("/blob/{id}", post(new_blob))
        .with_state(state_server);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8709").await?;
    let mut server_jh = tokio::spawn(async move {
        info!("Artifact store HTTP bridge is up and listening on port 8709.");
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_bridge.cancelled_owned())
            .await
        {
            warn!("Failed to start the HTTP bridge server: {e:?}");
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
