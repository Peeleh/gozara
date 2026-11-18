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

// remote blob
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

pub struct RemoteBlobContext {
    pub id: String,
    pub created_at: Instant,
    pub blob: RemoteBlob
}

// local blob
#[derive(Debug)]
pub struct InflightPush {
    pub to: PeerId,
    pub created_at: Instant
}

pub enum LocalChunk {
    AwaitingAssignment,
    Assigned {
        path: PathBuf,
        inflight: Option<InflightPush>,
        failed: HashMap<PeerId, usize>,
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
    Distributing {
        root_hash: Hash,
        merkle_leaves: Vec<Hash>,
        chunks: HashMap<Hash, LocalChunk>,
        base_path: PathBuf,
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

pub struct LocalBlobContext {
    pub id: String,
    pub created_at: Instant,
    pub blob: LocalBlob
}
