use serde::{Deserialize, Serialize};
use libp2p::PeerId;


#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    StoragePermit,
    BlobMeta {
        id: String,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    // convention: each ACK is worth ~20 chunks(up to 100mb) of storage space
     IssuedStoragePermit {
        valid_for: u64
    },
    BlobMeta {
        id: String,
        // Merkle root hash
        root_hash: [u8; 32],
        // (hash, owner)
        chunks: Vec<([u8; 32], PeerId)>,
    }
}
