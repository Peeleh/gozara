mod blob_store;
mod blake3_wrapper;

use eyre::Result;
use tracing::{info, warn};
use libp2p::{
    identity,
    gossipsub,
    PeerId,
};
use tokio::sync::mpsc;
use libp2p::swarm::Swarm;

pub struct Config {
    pub grpc_addr: String,
    pub key_file: Option<String>,
    pub bootnode_peer_id: String,
    pub bootnode_ip_addr: String,
    pub bootnode_port: String, 
    pub external_ip_addr: String,
    pub external_port: String,
    pub zone: String,
}

async fn go_public(
    config: Config
) -> Result<Swarm<peyk::p2p::GlobalBehaviour>> {
    // derive peer id
    let local_key = {
        if let Some(key_file) = config.key_file {
            let bytes = std::fs::read(key_file)?;
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = std::fs::write("./key.secret", bytes);
            warn!("No keys were supplied, so one is generated for you and saved to `./key.secret` file.");
            new_key
        }
    };
    let my_peer_id = PeerId::from_public_key(&local_key.public());    
    info!(
        "My peer id: `{}`",
        my_peer_id
    );  
    let mut swarm = peyk::p2p::setup_global_swarm(&local_key)?;
    // listen on all interfaces
    // ipv4
    swarm.listen_on(
        "/ip4/0.0.0.0/udp/20201/quic-v1".parse()?
    )?;
    swarm.listen_on(
        "/ip4/0.0.0.0/tcp/20201".parse()?
    )?;
    // ipv6
    // swarm.listen_on(
    //     "/ip6/::/udp/20201/quic-v1".parse()?
    // )?;
    // swarm.listen_on(
    //     "/ip6/::/tcp/20201".parse()?
    // )?;
    // gossip
    // topic example: "gozara-me-zone" for the middle east zone
    let topic = gossipsub::IdentTopic::new(format!("gozara-{}-zone", config.zone));
    let _ = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic);
    
    // init kademlia: get to know bootnode(s)    
    swarm.behaviour_mut()
        .kademlia
        .add_address(
            &config.bootnode_peer_id.parse()?,
            format!(
                "/ip4/{}/tcp/20201",
                config.bootnode_ip_addr
            )
            .parse()?
        );
    // initiate bootstrapping
    match swarm.behaviour_mut().kademlia.bootstrap() {
        Ok(query_id) => {            
            info!(
                "Bootstrap is initiated, query id: {:?}",
                query_id
            );
        },
        Err(e) => {
            info!(
                "Bootstrap failed: {:?}",
                e
            );
        }
    };
    // specify the external address    
    swarm.add_external_address(
        format!(
            "/ip4/{}/tcp/{}",
            config.external_ip_addr,
            config.external_port
        )
        .parse()?
    );
    swarm.add_external_address(
        format!(
            "/ip4/{}/udp/{}/quic-v1",
            config.external_ip_addr,
            config.external_port
        )
        .parse()?
    );
    Ok(swarm)
}


pub async fn run(
    config: Config,
) -> Result<()> {
    let swarm = go_public(config).await?;
    let (tx_swarm, rx_swarm) = mpsc::channel::<peyk::SwarmMessage>(16);
    let (tx_handler, rx_handler) = mpsc::channel::<peyk::HandlerMessage>(64);
    peyk::process_swarm(swarm, rx_swarm, tx_handler).await?;        
    blob_store::run(tx_swarm).await?;
    Ok(())
}
