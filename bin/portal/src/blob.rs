use std::{
    collections::HashMap,
    path::PathBuf
};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::Instant,
};
use libp2p::{
    PeerId,
    request_response::OutboundRequestId,
    kad::QueryId
};
use blake3::Hash;
use peyk::protocol::Manifest;

#[derive(Debug)]
pub struct PullContext {
    pub from: PeerId,
    pub created_at: Instant,
    pub _permit: OwnedSemaphorePermit
}

#[derive(Debug)]
pub enum RemoteChunk {
    Pulling {
        requests: HashMap<PeerId, Vec<(OutboundRequestId, Instant)>>,
        context: Option<PullContext>,
        failed: HashMap<PeerId, Vec<Instant>>,
    },
    Pulled(PeerId),
    Failed
}

#[derive(Debug)]
pub enum RemoteBlob {
    AwaitingManifest(Vec<Instant>),
    Gathering {
        root_hash: Hash,
        merkle_leaves: Vec<Hash>,
        path: PathBuf,
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
