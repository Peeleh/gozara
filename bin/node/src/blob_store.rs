use std::{
    time::{Instant, Duration},
    collections::HashMap,
};
use eyre::{
    eyre,
    Result
};
use tracing::{
    info,
    warn
};
use futures::stream::StreamExt;
use tokio::{
    sync::mpsc,
    time::interval
};
use tokio_stream::wrappers::IntervalStream;
use rs_merkle::MerkleTree;
use crate::network::SwarmRequest;
use crate::blake3_wrapper::Blake3Hash;
use crate::bridge::UploadRequest;

pub type Hash = [u8; 32];

// 4 MB
const CHUNK_SIZE: usize = 4 * 1024 * 1024;

// 4 hours
const RETENTION_TIME: u64 = 4 * 60 * 60;

pub enum BlobRequest {
    Store(String, Vec<u8>),
    Remove([u8; 32])
}

struct Blob {
    pub root_hash: [u8; 32],
    pub bridge_id: String,
    pub data: Vec<u8>,
    // [<start, end>]
    pub chunks: Vec<(usize, usize)>,
    pub created_at: u64,
}

struct BlobStore {
    // <root hash, blob>
    blobs: HashMap<[u8; 32], Blob>,    
}

impl BlobStore {
    pub fn new() -> Self {
        BlobStore {
            blobs: HashMap::new(),
        }
    }

    pub fn store_blob(
        &mut self,
        id: String,
        data: Vec<u8>,
    ) -> Result<[u8; 32]> {
        if 0 == data.len() {
            return Err(eyre!("Ignored empty blob."));
        }
        let merkle_tree = {
            let leaves: Vec<[u8; 32]> = data
                .as_slice()
                .chunks(CHUNK_SIZE)
                .map(|c| blake3::hash(c).into())
                .collect();
            MerkleTree::<Blake3Hash>::from_leaves(&leaves)
        };
        let root_hash = merkle_tree
            .root()
            .ok_or(eyre!("Couldn't get the merkle root."))?;
        if self.blobs.contains_key(&root_hash) {
            return Err(eyre!("Duplicate blob: {}", hex::encode(root_hash)));
        }        
        let chunks: Vec<(usize, usize)> = (0..data.len())            
            .step_by(CHUNK_SIZE)
            .map(|start| {
                let end = (start + CHUNK_SIZE).min(data.len());
                (start, end)
            })
            .collect();
        info!(
            "Blob `{}` is cuhnked and now stored locally with root hash(`{}`). We'll now try to persist it globally.",
            id,
            hex::encode(root_hash)
        );
        self.blobs.insert(
            root_hash, 
            Blob {
                root_hash: root_hash.clone(),
                bridge_id: id,
                data: data,
                chunks: chunks,
                created_at: Instant::now().elapsed().as_secs()
            }
        );

        Ok(root_hash)
    }

    pub fn remove_blob(&mut self, root_hash: &[u8; 32]) {
        let _b = self.blobs.remove(root_hash);
    }

    // periodic cleanup
    pub fn remove_stale_blobs(&mut self) {
        let now = Instant::now().elapsed().as_secs();
        // todo: inform the bridge
        self.blobs.retain(|_, v| {
            v.created_at + RETENTION_TIME < now
        })
    }
}

pub async fn run(
    mut blob_rx: mpsc::UnboundedReceiver<BlobRequest>,
    swarm_tx: mpsc::UnboundedSender<SwarmRequest>,
    bridge_tx: mpsc::UnboundedSender<UploadRequest>
) -> Result<()> {
    let mut blob_store = BlobStore::new();    
    tokio::spawn(async move {
        // to remove stale blobs
        let mut timer_stale_blobs = IntervalStream::new(
            interval(Duration::from_secs(60))
        ).fuse();

        loop {
            tokio::select! {
                _i = timer_stale_blobs.select_next_some() => {                
                    blob_store.remove_stale_blobs();
                },
                                
                r = blob_rx.recv() =>  match r {
                    Some(req) => {
                        match req {
                            BlobRequest::Store(id, data) => {
                                match blob_store.store_blob(id, data) {
                                    Ok(root_hash) => {
                                        if let Err(e) = swarm_tx.send(SwarmRequest::PersistBlob(root_hash)) {
                                            warn!(
                                                "Failed to send Persist request to the Swarm channel: {}",
                                                e
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Store blob error: {}", e);
                                        continue;                                        
                                    }

                                }                                
                            }
                            BlobRequest::Remove(root_hash) => {
                                blob_store.remove_blob(&root_hash);
                            }
                        }
                    }
                    None => {
                        warn!("Store blob channel is closed.");
                        break;
                    }
                }
            }
        }
    });
    Ok(())
}
