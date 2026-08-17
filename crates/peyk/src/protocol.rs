use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WouldProve(pub u8);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputToken {
    pub hash: u128,

    pub owner: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    SP1(SP1Op)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SP1Op {
    Prove(ProveDetails),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProveDetails { 
    pub id: u128,

    pub elf_kind: ELFKind,

    pub tokens: Vec<InputToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ELFKind {
    Subblock,

    Agg
}

// used by clients when gossiping about compute needs
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeJob {    
    // network-wide id of the job
    pub id: u128,

    pub kind: JobKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProofKind {
    // param: batch_id
    Subblock(u128),

    Agg(u128)
}

// proofs in custody of the prover
// being large in size >256kb, so the prover holds them until the client requests their transfer
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofToken {
    pub job_id: u128,

    pub kind: ProofKind,

    // hash of the proof blob
    pub hash: u128,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {        
    Job(ComputeJob),

    ProofIsReady(ProofToken),
}

// the client responds to requests
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Accept,

    Reject,
}