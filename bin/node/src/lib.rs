pub mod config;
pub use config::NodeConfig;
mod network;
mod blob_store;
mod blake3_wrapper;

// use tracing::info;
use eyre::Result;
use tokio::sync::mpsc;

pub async fn run(
    config: NodeConfig,
) -> Result<()> {
    let (swarm_tx, swarm_rx) = mpsc::unbounded_channel::<network::SwarmMessage>();
    let (blob_tx, blob_rx) = mpsc::unbounded_channel::<blob_store::BlobMessage>();
    network::go_public(config, swarm_rx, blob_tx.clone()).await?;    
    blob_store::run(blob_tx, blob_rx, swarm_tx).await?;
    Ok(())
}
