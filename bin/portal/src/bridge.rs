use std::{
    time::Duration,
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
use rs_merkle::MerkleTree;
use blake3::Hash;
use bytes::BytesMut;
use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{header, StatusCode},
    routing::get,
    Router,
    response::{Response, IntoResponse}
};
use mti::prelude::{V7, MagicTypeId, MagicTypeIdExt};
use std::str::FromStr;
use sqlx::postgres::PgPool;
use crate::blake3_wrapper::Blake3;
use crate::admission::{
    BASE_BLOB_PATH, BlobStatus, ToPipeline, CHUNK_SIZE
};

// 1 GiB
const MAX_BLOB_SIZE: usize = 1<<30;

// each permit = 4 MiB, in total 16 GiB worth of "upload permits/memory pressure"
const TOTAL_CHUNK_BUDGET: usize = 1<<12;

// max silence between frames
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

fn is_id_valid(
    id: &str
) -> bool {
    MagicTypeId::from_str(&id).is_ok()
}

async fn fetch_status_from_db(
    id: &str,
    db_pool: &PgPool
) -> Option<BlobStatus> {
    match sqlx::query!(
        r#"SELECT status AS "status: BlobStatus" from blobs WHERE id = $1"#,
        id
    )
    .fetch_optional(db_pool)
    .await
    {
        Ok(Some(row)) => Some(row.status),
        Ok(None) => None,
        Err(e) => {
            warn!(blob = %id, error = %e, "Failed to fetch status from the DB.");
            None
        }
    }
}

struct Admission {
    id: String,
    committed: bool,
    db_pool: PgPool
}

impl Admission {
    async fn new(
        id: &str,
        db_pool: PgPool
    ) -> Result<Self, StatusCode> {
        if let Some(_) = fetch_status_from_db(id, &db_pool).await {
            return Err(StatusCode::CONFLICT.into())
        }
        let new_status = BlobStatus::Distributing;
        let _ = sqlx::query!(
            r#"INSERT INTO blobs(id, status) VALUES($1, $2)"#,
            id,
            new_status as BlobStatus
        )
        .execute(&db_pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self { id: id.to_owned(), committed: false, db_pool })
    }

    fn commit(mut self) {
        self.committed = true
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if !self.committed {
            // todo: remove the directory containing chunks
            // if let Err(e) = sqlx::query!("DELETE FROM blobs WHERE id = $1", self.id.clone())
            //     .execute(self.db_pool)
            //     .await
            // {
            //     warn!(blob = %self.id, "Could not delete from the DB.");
            // }
        }
    }
}

// Error type: status code + 'Retry-After: 5' on 503
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
    chunk: &Bytes
) -> Result<Hash, StatusCode> {
    let permit = hashing.acquire_owned().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let c = chunk.clone();
    let hash = task::spawn_blocking(move || {
        let h = blake3::hash(&c);
        drop(permit);
        h
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(hash)
}

async fn write_chunks_to_disk(
    id: &str,
    leaves: &[Hash],
    chunks: &[Bytes])
-> Result<()> {
    use tokio::fs;
    let blob_path = std::path::Path::new(BASE_BLOB_PATH).join(id);
    fs::create_dir_all(&blob_path).await?;
    let random_name = blob_path.join("tmp_".create_type_id::<V7>().as_str());
    for (chunk_id, chunk) in leaves.iter().zip(chunks.iter()) {
        fs::write(&random_name, chunk).await?;
        let hex_id = chunk_id.to_hex().to_string();
        fs::rename(&random_name, &blob_path.join(&hex_id)).await?;
    }
    Ok(())
}


#[derive(Clone)]
struct BridgeState {
    hashing: Arc<Semaphore>,
    admission_budget: Arc<Semaphore>,
    db_pool: PgPool,
    tx_admission: mpsc::Sender<ToPipeline>,
}

impl BridgeState {
    fn new(
        db_pool: PgPool,
        tx_admission: mpsc::Sender<ToPipeline>
    ) -> Self {
        Self {
            hashing: Arc::new(Semaphore::new(2)),
            admission_budget: Arc::new(Semaphore::new(TOTAL_CHUNK_BUDGET)),
            db_pool,
            tx_admission
        }
    }
}

async fn get_status(
    State(state): State<BridgeState>,
    Path(id): Path<String>
) -> Result<(StatusCode, String), Reject> {
    if !is_id_valid(&id) {
        debug!(id = %id, "Invalid Id.");
        return Err(Reject(StatusCode::BAD_REQUEST))
    }
    if let Some(status) = fetch_status_from_db(&id, &state.db_pool).await {
        match status {
            BlobStatus::Gathering | BlobStatus::Distributing =>
                Ok((StatusCode::ACCEPTED, format!("{status:?}"))),
            BlobStatus::Finished => Ok((StatusCode::OK, format!("{status:?}"))),
            BlobStatus::Failed => Err(StatusCode::BAD_GATEWAY.into())
        }
    } else {
        Err(Reject(StatusCode::NOT_FOUND))
    }
}

// to gather from remote nodes
async fn gather_blob(
    State(state): State<BridgeState>,
    Path(id): Path<String>,
) -> Result<StatusCode, Reject> {
    if !is_id_valid(&id) {
        debug!(id = %id, "Invalid Id.");
        return Err(Reject(StatusCode::BAD_REQUEST))
    }
    match fetch_status_from_db(&id, &state.db_pool).await {
        Some(s) => match s {
            BlobStatus::Gathering => Ok(StatusCode::ACCEPTED),
            BlobStatus::Distributing => Err(StatusCode::CONFLICT.into()),
            BlobStatus::Finished => Ok(StatusCode::OK),
            BlobStatus::Failed => Err(StatusCode::BAD_GATEWAY.into())
        }
        None => {
            // register blob on the db
            let new_status = BlobStatus::Gathering;
            let _ = sqlx::query!(
                r#"INSERT INTO blobs(id, status) VALUES($1, $2)"#,
                id.clone(),
                new_status as BlobStatus
            )
            .execute(&state.db_pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            // notify the pipeline
            state
                .tx_admission
                .send(ToPipeline::GatherBlob(id.clone()))
                .await
                .map_err(|e| {
                    debug!(blob = %id, error = %e,
                        "Failed to notify the pipeline about a new gather."
                    );
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            Ok(StatusCode::ACCEPTED)
        }
    }
}

// to distribute
async fn distribute_blob(
    State(state): State<BridgeState>,
    Path(id): Path<String>,
    headers: header::HeaderMap,
    body: Body
) -> Result<StatusCode, Reject> {
    if !is_id_valid(&id) {
        debug!(id = %id, "Invalid Id.");
        return Err(Reject(StatusCode::BAD_REQUEST))
    }
    // check for blob size first and whether we have enough budget to admit it
    let declared_blob_len: usize = headers.get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse().ok())
        .ok_or(StatusCode::LENGTH_REQUIRED)?;
    if declared_blob_len == 0 {
        return Err(StatusCode::BAD_REQUEST.into())
    }
    let num_chunks = declared_blob_len.div_ceil(CHUNK_SIZE);
    if declared_blob_len > MAX_BLOB_SIZE {
        return Err(StatusCode::PAYLOAD_TOO_LARGE.into())
    }
    let admission_permit = state.admission_budget.clone()
        .try_acquire_many_owned(num_chunks as u32)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let admission = Admission::new(&id, state.db_pool.clone()).await?;
    // stream in chunks
    let mut stream = body.into_data_stream();
    let mut buf = BytesMut::with_capacity(CHUNK_SIZE);
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut leaves = Vec::with_capacity(num_chunks);
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
                leaves.push(hash_chunk(state.hashing.clone(), &full).await?);
                chunks.push(full);
            }
        }
    }
    if received != declared_blob_len {
        return Err(StatusCode::BAD_REQUEST.into())
    }
    if !buf.is_empty() {
        let last_chunk = buf.freeze();
        leaves.push(hash_chunk(state.hashing.clone(), &last_chunk).await?);
        chunks.push(last_chunk);
    }
    // merklize it
    let merkle_tree = {
        let simple_leaves: Vec<[u8; 32]> = leaves.iter().map(|h| *h.as_bytes()).collect();
        MerkleTree::<Blake3>::from_leaves(&simple_leaves)
    };
    let root_hash: Hash = merkle_tree.root()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .into();
    // dump chunks to the disk
    write_chunks_to_disk(&id, &leaves, &chunks)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.tx_admission
        .send(ToPipeline::DistributeBlob {
            id: id.clone(), root_hash, merkle_leaves: leaves, admission_permit
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    admission.commit();
    info!(id = %id, num_chunks = %num_chunks, size = declared_blob_len as f32 / 1_048_576f32,
        "Admitted a new blob."
    );
    Ok(StatusCode::CREATED)
}

pub async fn run(
    db_pool: PgPool,
    tx_admission: mpsc::Sender<ToPipeline>,
    shutdown: CancellationToken
) -> Result<task::JoinHandle<()>> {
    let state = BridgeState::new(db_pool, tx_admission);
    let shutdown_bridge = shutdown.clone();
    let app = Router::new()
        .route("/status/{id}", get(get_status))
        .route("/blob/{id}", get(gather_blob).post(distribute_blob))
        .with_state(state);
    const ADDR: &str = "127.0.0.1:8709";
    let listener = tokio::net::TcpListener::bind(ADDR).await?;
    let server_jh = tokio::spawn(async move {
        info!("The HTTP bridge is up and listening on port `{ADDR}`.");
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_bridge.cancelled_owned())
            .await
        {
            warn!("Failed to serve the HTTP bridge: {e:?}");
        }
    });
    tokio::spawn(async move {
        let _ = shutdown.cancelled().await;
    });
    Ok(server_jh)
}
