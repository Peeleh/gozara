use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use libp2p::PeerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobManifest {
    pub root_hash: [u8; 32],
    pub merkle_leaves: Vec<[u8; 32]>,
    // <owner, chunk hash>
    pub chunk_owners: HashMap<PeerId, Vec<[u8; 32]>>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {}
