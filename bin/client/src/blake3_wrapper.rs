#[derive(Clone)]
pub struct Blake3;

impl rs_merkle::Hasher for Blake3 {
    type Hash = [u8; 32];

    fn hash(data: &[u8]) -> Self::Hash {
        blake3::hash(data).into()
    }
}
