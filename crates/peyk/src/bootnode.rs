use std::time::Duration;
use eyre::Result;
use libp2p::{
    tcp,
    yamux,
    noise,
    identity,
    identify,
    kad,
    kad::store::MemoryStore,
    swarm::{
        Swarm,
        NetworkBehaviour,
    },
};
use crate::p2p;

// used by bootnodes for peer discovery
#[derive(NetworkBehaviour)]
pub struct BootNodeBehaviour {
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
}

// setup a bootnode-specific swram instance
pub fn setup_swarm_for_bootnode(
    keypair: &identity::Keypair,
)-> Result<Swarm<BootNodeBehaviour>> {
    let local_keypair = keypair.clone();
    let swarm = libp2p::SwarmBuilder::with_existing_identity(local_keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default
        )?
        .with_quic()
        .with_dns()?        
        .with_behaviour(|key| {            
            let public_key = key.public();
            Ok(BootNodeBehaviour {
                identify: p2p::prepare_identify_behaviour(&public_key),
                kademlia: p2p::prepare_kademlia_behaviour(&public_key)
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    Ok(swarm)
}
