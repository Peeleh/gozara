use std::collections::{BTreeSet, BTreeMap};
use serde::{Deserialize, Serialize};
use libp2p::PeerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobManifest {
    pub root_hash: [u8; 32],
    pub merkle_leaves: Vec<[u8; 32]>,
    // <owner, [chunks]>
    pub chunk_owners: BTreeMap<PeerId, BTreeSet<[u8; 32]>>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {}
