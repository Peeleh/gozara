use serde::{Deserialize, Serialize};
use libp2p::PeerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Manifest {
    Blob {
        root_hash: [u8; 32],
        // <hash, owner>
        chunks: Vec<([u8; 32], PeerId)>,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    StoragePermit,
    // <chunk id>
    PullChunk(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    // convention: each permit is worth ~20 chunks of storage space
    IssuedStoragePermit {
        valid_for: u64 // seconds
    },
    // <chunk id>
    PullChunkAck(String),
}
