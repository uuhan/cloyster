use blake3::{Hash, Hasher};

pub fn calc_root<'a>(
    prev: Option<Hash>,
    kvs: impl Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
) -> Hash {
    let mut hasher = Hasher::new();
    if let Some(hash) = prev {
        hasher.update(hash.as_bytes());
    }
    for (key, value) in kvs {
        hasher.update(&key);
        hasher.update(&value);
    }
    hasher.finalize()
}
