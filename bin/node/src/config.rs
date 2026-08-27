
pub struct NodeConfig {
    pub grpc_addr: String,
    pub key_file: Option<String>,
    pub bootnode_peer_id: String,
    pub bootnode_ip_addr: String,
    pub bootnode_port: String, 
    pub external_ip_addr: String,
    pub external_port: String,
    pub zone: String,
}