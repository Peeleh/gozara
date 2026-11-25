use blake3::Hash;
use tokio::sync::OwnedSemaphorePermit;

// each chunk is 4 MiB
pub const CHUNK_SIZE: usize = 1<<22;

// each permit = 4 MiB, in total 16 GiB worth of disk pressure
pub const TOTAL_CHUNK_BUDGET: usize = 1<<12;

// blob storage path on disk
pub const BASE_BLOB_PATH: &str = "./blobs";

// 512 MiB
pub const MAX_BLOB_SIZE: usize = 1<<29;

// 128 chunks: 128 * 4 = 512 MiB
pub const MAX_CHUNKS_PER_BLOB: usize = 1<<7;

#[derive(sqlx::Type, Debug, Clone)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
pub enum BlobStatus {
    Gathering,
    Distributing,
    Finished,
    Failed,
}

pub enum ToPipeline {
    // <id>
    GatherBlob(String),
    DistributeBlob {
        id: String,
        root_hash: Hash,
        merkle_leaves: Vec<Hash>,
        admission_permit: OwnedSemaphorePermit,
    }
}
