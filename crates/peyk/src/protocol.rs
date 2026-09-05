use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    RequestStoragePermit {
        id: String,
        chunks: Vec<[u8; 32]>,        
    },    
}


#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    StoragePermit {
        id: String,
        // none means rejection
        valid_until: Option<u64>,
    },
}
