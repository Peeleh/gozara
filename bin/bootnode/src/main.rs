
#![doc = include_str!("../README.md")]

use std::time::Duration;
use futures::{
    StreamExt, select
};
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;

use libp2p::{
    identity,
    identify, 
    kad,
    PeerId,
    swarm::{
        SwarmEvent
    },
    multiaddr::Protocol,
};

// use tracing_subscriber::EnvFilter;
use clap::Parser;
use bootnode::{BootNodeBehaviourEvent, setup_swarm_for_bootnode};

// CLI
#[derive(Parser, Debug)]
#[command(name = "Bootnode CLI for Gozara: p2p verifiable computing marketplace.")]
#[command(author = "Gozara team")]
#[command(version = "0.1")]
#[command(about = "Yet another verifiable compute marketplace.", long_about = None)]
struct Cli {
    #[arg(short, long)]
    key_file: Option<String>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // let _ = tracing_subscriber::fmt()
    //     .with_env_filter(EnvFilter::from_default_env())
    //     .try_init();

    let cli = Cli::parse();

    // key 
    let local_key = {
        if let Some(key_file) = cli.key_file {
            let bytes = std::fs::read(key_file).unwrap();
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = std::fs::write("./key.secret", bytes);
            println!("No keys were supplied, so one is generated for you and saved to `./key.secret` file.");
            new_key
        }
    };   
    println!("my peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));    

    let mut swarm = setup_swarm_for_bootnode(&local_key)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/20201".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/20201/quic-v1".parse()?)?;
    swarm.behaviour_mut()
        .kademlia
        .set_mode(
            Some(kad::Mode::Server)
        );    

    let mut timer_peer_discovery = IntervalStream::new(
        interval(Duration::from_secs(30))
    )
    .fuse();
    
    // println!("Protocol names: {:#?}", swarm.behaviour_mut().kademlia.protocol_names());
    loop {
        select! {
            // try to discover new peers
            _i = timer_peer_discovery.select_next_some() => {
                let random_peer_id = PeerId::random();
                println!("Searching for the closest peers to `{random_peer_id}`");
                swarm.behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
                for bucket in swarm.behaviour_mut().kademlia.kbuckets() {
                    for entry in bucket.iter() {
                        let peer = entry.node.key.preimage();
                        println!("known peer: {}", peer);
                    }
                }
            },

            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    ..
                } => {
                    println!(
                        "A connection has been established to {} via {:?}",
                        peer_id,
                        endpoint
                    );
                },


                SwarmEvent::Behaviour(BootNodeBehaviourEvent::Identify(identify::Event::Received {
                    peer_id,
                    info,
                    ..
                })) => {
                    println!(
                        "Received identify from {}: {:#?}",
                        peer_id,
                        info
                    );                                                               
                },

                SwarmEvent::NewExternalAddrOfPeer {
                    peer_id,
                    address
                } => {
                    let is_public = address.iter()
                        .filter_map(|c| 
                            if let Protocol::Ip4(ip4_addr) = c {
                                Some(ip4_addr)
                            } else {
                                None
                            }
                        )
                        .all(|a| !a.is_private() && !a.is_loopback());
                    if is_public {                        
                        println!(
                            "added public address of the peer to the DHT: {}",
                            address
                        );
                        swarm.behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, address);
                    }                      
                },

                SwarmEvent::Behaviour(BootNodeBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                    ..
                })) => {
                    println!("Query finished with closest peers: {:#?}", ok.peers);
                },

                SwarmEvent::Behaviour(BootNodeBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result:
                        kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                            ..
                        })),
                    ..
                })) => {
                    eprintln!("Query for closest peers timed out");
                },
                
                _ => {
                    println!("{:#?}", event);
                },
            }
        }
    }
}
