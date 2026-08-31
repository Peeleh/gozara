use node::NodeConfig;
use std::env;
// use opentelemetry_sdk::Resource;
use tracing::info;
use eyre::{
    Result,
    Context
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {e}");
    }
    info!("Loading environment variables.");
    let cfg = NodeConfig {
        grpc_addr: env::var("NODE_GRPC_ADDR")
            .with_context(|| "`NODE_GRPC_ADDR` is missing")?,
        key_file: env::var("KEY_FILE").ok(),
        bootnode_peer_id: env::var("BOOTNODE_PEER_ID")
            .with_context(|| "`BOOTNODE_PEER_ID` is missing")?,
        bootnode_ip_addr: env::var("BOOTNODE_IP_ADDR")
            .with_context(|| "`BOOTNODE_IP_ADDR` is missing")?,
        bootnode_port: env::var("BOOTNODE_PORT")
            .with_context(|| "`BOOTNODE_PORT` is missing")?,
        external_ip_addr: env::var("EXTERNAL_IP_ADDR")
            .with_context(|| "`EXTERNAL_IP_ADDR` is missing")?,
        external_port: env::var("EXTERNAL_PORT")
            .with_context(|| "`EXTERNAL_PORT` is missing")?,
        zone: env::var("ZONE").unwrap_or("ME".to_string()),
    };
    node::run(cfg).await?;
    Ok(())
}