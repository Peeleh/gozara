use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WouldStore;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {        
    Blob {
        id: String,
    }
}

// the client responds to requests
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Accept,

    Reject,
}
