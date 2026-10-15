use tokio::time::Instant;
use libp2p::PeerId;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
pub struct Timestamped<T> {
    pub value: T,
    pub at: Instant
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub id: String,
    pub op: PeerId,
    pub root_hash: [u8; 32],
    pub chunks: Vec<[u8; 32]>,
}
