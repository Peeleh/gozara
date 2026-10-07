use std::time::Duration;
use libp2p::{
    core::{        
        muxing::StreamMuxerBox,
        transport::Boxed
    },
    tcp,
    yamux,
    noise,
    Transport,    
    gossipsub,
    identity,
    identify,
    request_response,
    kad, kad::store::MemoryStore,
    swarm::{
        Swarm,
        NetworkBehaviour,
        StreamProtocol,
    },
    SwarmBuilder,
    PeerId,
};
use libp2p_quic as quic;
use eyre::Result;
use crate::protocol;

// prepare gossipsub behaviour
fn prepare_gossipsub_behaviour(
    keypair: &identity::Keypair,
)-> Result<gossipsub::Behaviour> {
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // aid debugging by not cluttering log space
        .validation_mode(gossipsub::ValidationMode::Strict) // enforce message signing
        .build()?;
    Ok(
        gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config
        )
        .map_err(eyre::Error::msg)?
    )
}

// prepare request-response behaviour
fn prepare_request_response_behaviour()
-> request_response::cbor::Behaviour<protocol::Request, protocol::Response> 
{
    request_response::cbor::Behaviour::<protocol::Request, protocol::Response>::new(
        [(
            StreamProtocol::new("/gozara/req_resp/1.0"),
            request_response::ProtocolSupport::Full,
        )],
        request_response::Config::default(),
    )
}

// prepare identify behaviour
pub fn prepare_identify_behaviour(
    public_key: &identity::PublicKey
)-> identify::Behaviour {
    identify::Behaviour::new(
        identify::Config::new(
            String::from("/gozara/identify/1.0"),
            public_key.clone()
        )
    )
}

fn _prepare_quic_transport(
    keypair: &identity::Keypair
) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {    
    Ok(
        quic::tokio::Transport::new(quic::Config::new(keypair))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .boxed()
    )
}

// main network behaviour 
#[derive(NetworkBehaviour)]
pub struct GlobalBehaviour {
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub req_resp: request_response::cbor::Behaviour<protocol::Request, protocol::Response>,
    pub blob_stream: libp2p_stream::Behaviour,
}

pub fn prepare_kademlia_behaviour(
    public_key: &identity::PublicKey,
) -> kad::Behaviour<MemoryStore> {
    let mut cfg = kad::Config::new(
        StreamProtocol::new("/gozara/kad/1.0")
    );
    cfg.set_query_timeout(Duration::from_secs(5 * 60));    
    let local_peer_id = PeerId::from(public_key.clone());
    let store = MemoryStore::new(local_peer_id);
    kad::Behaviour::with_config(local_peer_id, store, cfg)
}

// setup a global swram instance
pub fn setup_global_swarm(
    keypair: &identity::Keypair,
)-> Result<Swarm<GlobalBehaviour>> {
    let local_keypair = keypair.clone();
    let swarm = SwarmBuilder::with_existing_identity(local_keypair)
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
            Ok(GlobalBehaviour {
                identify: prepare_identify_behaviour(&public_key),
                kademlia: prepare_kademlia_behaviour(&public_key),
                gossipsub: prepare_gossipsub_behaviour(&key)?,
                req_resp: prepare_request_response_behaviour(),
                blob_stream: libp2p_stream::Behaviour::new()
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();    
    Ok(swarm)
}
