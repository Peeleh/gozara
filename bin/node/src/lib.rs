pub mod config;
pub use config::NodeConfig;
mod network;
mod blob_store;
mod bridge;
mod blake3_wrapper;

// use tracing::info;
use eyre::Result;
use tokio::sync::mpsc;

pub async fn run(
    config: NodeConfig,
) -> Result<()> {
    let (swarm_tx, mut swarm_rx) = mpsc::unbounded_channel::<network::SwarmRequest>();
    network::go_public(config, swarm_rx).await?;
    let (blob_tx, mut blob_rx) = mpsc::unbounded_channel::<blob_store::BlobRequest>();
    let (bridge_tx, mut bridge_rx) = mpsc::unbounded_channel::<bridge::UploadRequest>();
    blob_store::run(blob_rx, swarm_tx, bridge_tx).await?;
    bridge::run(bridge_rx, blob_tx).await?;
    Ok(())
}
