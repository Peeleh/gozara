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
pub enum Request {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {}
