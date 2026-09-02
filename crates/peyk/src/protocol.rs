use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WouldStore;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {        
    Blob {
        id: String,
    }
}

// the client responds to requests
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Accept,

    Reject,
}
