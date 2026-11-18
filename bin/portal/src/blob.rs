use std::{
    collections::{HashSet, HashMap},
    path::PathBuf
};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::Instant,
};
use libp2p::{
    PeerId,
    kad::QueryId
};
use blake3::Hash;
use peyk::protocol::Manifest;

#[derive(Debug)]
pub struct InflightPull {
    pub from: PeerId,
    pub created_at: Instant
}

#[derive(Debug)]
pub enum RemoteChunk {
    Pulling {
        path: PathBuf,
        owners: HashSet<PeerId>,
        inflight: Option<InflightPull>,
        failed: HashMap<PeerId, usize>,
    },
    Pulled(PeerId),
    Failed
}

#[derive(Debug)]
pub enum RemoteBlob {
    // todo: entry time for timeouts
    AwaitingManifest(Vec<Instant>),
    Gathering {
        root_hash: Hash,
        merkle_leaves: Vec<Hash>,
        base_path: PathBuf,
        chunks: HashMap<Hash, RemoteChunk>,
    },
    Failed
}

pub struct PushContext {
    pub to: PeerId,
    pub created_at: Instant
}

pub enum LocalChunk {
    AwaitingAssignment,
    Assigned {
        context: Option<PushContext>,
        failed: HashMap<PeerId, Vec<Instant>>,
    },
    // <owner>
    Pushed(PeerId)
}

impl LocalChunk {
    pub fn extract_owner(&self) -> Option<PeerId> {
        if let Self::Pushed(owner) = self {
            Some(*owner)
        } else {
            None
        }
    }
}

pub enum LocalBlob {
    // todo: entry time for timeouts
    Distributing {
        root_hash: Hash,
        merkle_leaves: Vec<Hash>,
        chunks: HashMap<Hash, LocalChunk>,
        path: PathBuf,
        _admission_permit: OwnedSemaphorePermit
    },
    AwaitingPublishManifest {
        manifest: Manifest,
        attempts: Vec<(QueryId, Instant)>,
        failed: usize,
    },
    Published,
    Failed
}
