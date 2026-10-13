use std::{
    time::{Instant, Duration},
    sync::Arc,
};
use futures::StreamExt;
use eyre::Result;
use tracing::{info, warn, debug};
use tokio::{
    sync::{mpsc, Semaphore},
    time::timeout,
    task,
};
use tokio_util::sync::CancellationToken;
use dashmap::{
    DashMap,
    mapref::entry::Entry
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
use mti::prelude::*;
// use std::str::FromStr;
use crate::blake3_wrapper::Blake3;
use crate::admission::{Status, ToPipeline, ToBridge, AdmittedBlob, CHUNK_SIZE};

// 1 GiB
const MAX_BLOB_SIZE: usize = 1 << 30;

// each permit = 4 MiB, in total we want 6 GiB worth of blob upload permits aka memory pressure
const TOTAL_CHUNK_BUDGET: usize = 1536;

// max silence between frames
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct TimestampedStatus {
    at: Instant,
    status: Status,
}

struct Admission {
    map: Arc<DashMap<String, Vec<TimestampedStatus>>>,
    id: String,
    committed: bool
}

impl Admission {
    fn new(state: &BridgeState, id: String) -> Result<Self, StatusCode> {
        let validated_id = MagicTypeId::from_str(&id)
            .map_err(|e| {
                debug!(error = %e, "Bad Id.");
                StatusCode::BAD_REQUEST
            })?
            .to_string();
        match state.status_map.entry(validated_id.clone()) {
            Entry::Occupied(_) => Err(StatusCode::CONFLICT),
            Entry::Vacant(e) => {
                e.insert(vec![TimestampedStatus {
                    at: Instant::now(),
                    status: Status::Pending
                }]);
                Ok(Self {
                    map: state.status_map.clone(),
                    id: validated_id,
                    committed: false
                })
            }
        }
    }

    fn commit(mut self) {
        self.committed = true
    }
}

// remove the blob from the map if the upload is cancelled mid-journey
impl Drop for Admission {
    fn drop(&mut self) {
        if !self.committed {
            self.map.remove(&self.id);
        }
    }
}

// Error type for the new_blob: status code + 'Retry-After: 5' on 503
struct Reject(StatusCode);

impl From<StatusCode> for Reject {
    fn from(code: StatusCode) -> Self { Reject(code) }
}

impl IntoResponse for Reject {
    fn into_response(self) -> Response {
        debug!(status = %self.0, "Request is rejected.");
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
    let hash = task::spawn_blocking(move || {
        let h = blake3::hash(&c);
        drop(permit);
        h
    }).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok((hash, chunk))
}


#[derive(Clone)]
struct BridgeState {
    hashing: Arc<Semaphore>,
    admission_budget: Arc<Semaphore>,
    status_map: Arc<DashMap<String, Vec<TimestampedStatus>>>,
    tx_admission: mpsc::Sender<ToPipeline>,
}

impl BridgeState {
    fn new(tx_admission: mpsc::Sender<ToPipeline>) -> Self {
        BridgeState {
            hashing: Arc::new(Semaphore::new(2)),
            admission_budget: Arc::new(Semaphore::new(TOTAL_CHUNK_BUDGET)),
            status_map: Arc::new(DashMap::new()),
            tx_admission
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

// to assemble from remote nodes
async fn get_blob(
    State(state): State<BridgeState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, String), Reject> {
    let validated_id = MagicTypeId::from_str(&id)
        .map_err(|e| {
            debug!(error = %e, "Bad Id.");
            StatusCode::BAD_REQUEST
        })?
        .to_string();
    let status = match state.status_map.entry(validated_id.clone()) {
        Entry::Occupied(e) => {
            e.get().last().unwrap().status.clone()
        },
        Entry::Vacant(e) => {
            e.insert(vec![TimestampedStatus {
                at: Instant::now(),
                status: Status::Pending
            }]);
            Status::Pending
        }
    };

    match status {
        Status::Pending => {
            state.tx_admission
                .send(ToPipeline::GetBlob { id: validated_id.clone() })
                .await
                .map_err(|e| {
                    debug!(error = %e, "Failed to notify the coordinator about a missing blob.");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            return Ok((StatusCode::ACCEPTED, validated_id))
        }
        Status::Finalized => {
            // todo: gather blob info
            return Ok((StatusCode::OK, validated_id))
        }
        Status::Failed { reason } => {
            // todo: failed blobs should be evicted from the statu map after a while to allow retry
            return Ok((StatusCode::OK, reason.unwrap_or_else(|| "".to_string())))
        }
    }
}

// to distribute 
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
    let admission = Admission::new(&state, id.clone())?;
    let allowance = state.admission_budget.clone()
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
                chunks.push(hash_chunk(state.hashing.clone(), full).await?);
            }
        }
    }
    if received != declared_blob_len {
        return Err(StatusCode::BAD_REQUEST.into())
    }
    if !buf.is_empty() {
        chunks.push(hash_chunk(state.hashing.clone(), buf.freeze()).await?);
    }
    // merklize it
    let leaves: Vec<[u8; 32]> = chunks.iter().map(|(h, _)| *h.as_bytes()).collect();
    let merkle_tree = MerkleTree::<Blake3>::from_leaves(leaves.as_slice());
    let root_hash: Hash = merkle_tree.root()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .into();
    let root_hash_str = root_hash.to_hex().to_string();
    state.tx_admission
        .send(ToPipeline::NewBlob(AdmittedBlob { id: id.clone(), root_hash, merkle_tree, chunks, allowance }))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    admission.commit();
    info!("Admitted a new blob with id(`{id}`), root hash(`{root_hash_str}`), #chunks(`{num_chunks}`), and total size of `{:.1}` MiB",
        declared_blob_len as f32 / 1_048_576f32
    );
    Ok((StatusCode::CREATED, id))
}

pub async fn run(
    mut rx_bridge: mpsc::Receiver<ToBridge>,
    tx_admission: mpsc::Sender<ToPipeline>,
    shutdown: CancellationToken
) -> Result<task::JoinHandle<()>> {
    let state = BridgeState::new(tx_admission);
    let state_server = state.clone();
    let shutdown_bridge = shutdown.clone();
    let app = Router::new()
        .route("/status/{id}", get(get_status))
        .route("/blob/{id}", get(get_blob))
        .route("/blob/{id}", post(new_blob))
        .with_state(state_server);
    const ADDR: &str = "127.0.0.1:8709";
    let listener = tokio::net::TcpListener::bind(ADDR).await?;
    let mut server_jh = tokio::spawn(async move {
        info!("Artifact store HTTP bridge is up and listening on `{ADDR}`.");
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_bridge.cancelled_owned())
            .await
        {
            warn!("Failed to serve the HTTP bridge: {e:?}");
        }
    });
    let shutdown_msg = shutdown.clone();
    let mut msg_jh = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_msg.cancelled() => {
                    info!("Received shutdown request.");
                    break
                },

                m = rx_bridge.recv() => match m {
                    Some(b_msg) => {
                        match b_msg {
                            ToBridge::UpdateStatus {
                                id,
                                status
                            } => {
                                match state.status_map.entry(id.clone()) {
                                    Entry::Occupied(mut entry) => {
                                        entry.get_mut().push(
                                            TimestampedStatus { at: Instant::now(), status }
                                        );
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
