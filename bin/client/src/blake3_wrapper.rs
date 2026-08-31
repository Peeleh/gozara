use blake3::hash;

#[derive(Clone)]
pub struct Blake3Hash;

impl rs_merkle::Hasher for Blake3Hash {
    type Hash = [u8; 32];

    fn hash(data: &[u8]) -> [u8; 32] {
        hash(data).into()
    }
}