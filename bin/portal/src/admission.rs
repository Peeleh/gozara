use blake3::Hash;
use tokio::sync::OwnedSemaphorePermit;

// 4 MiB
pub const CHUNK_SIZE: usize = 1<<22;
// blob storage path on disk
pub const BASE_BLOB_PATH: &str = "./blobs";

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
