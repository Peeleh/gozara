use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize)]
pub enum Request {        
    Blob {
        id: String,
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Accept,

    Reject,
}
