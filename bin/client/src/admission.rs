use serde::Serialize;
use rs_merkle::MerkleTree;
use blake3::Hash;
use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;
use crate::blake3_wrapper::Blake3;

// 4 MiB
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Serialize, Debug)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum Status {
    Pending,
    Finalized,
    Failed { reason: Option<String> },
}

pub struct AdmittedBlob {
    pub id: String,
    pub root_hash: Hash,
    pub merkle_leaves: Vec<[u8; 32]>,
    pub merkle_tree: MerkleTree<Blake3>,
    pub chunks: Vec<(Hash, Bytes)>,
    pub allowance: OwnedSemaphorePermit,
}

pub enum ToPipeline {
    GetBlob {
        id: String,
    },
    NewBlob(AdmittedBlob)
}

pub enum ToBridge {
    UpdateStatus {
        id: String,
        status: Status
    }
}