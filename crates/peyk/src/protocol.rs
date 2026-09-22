use serde::{Deserialize, Serialize};
use libp2p::PeerId;


#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    RequestStoragePermit,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    // convention: each ACK is worth ~20 chunks(up to 100mb) of storage space
    AckStoragePermit {
        valid_for: u8
    },
}
