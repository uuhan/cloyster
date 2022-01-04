use crate::{pagecache::PageId, prelude::*};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Node {
    // header
    pub(crate) prev: Option<PageId>,
    pub(crate) hash: Option<[u8; 32]>,

    // body
    pub(crate) inner: BTreeMap<Key, Entry>,
}

impl Node {
    pub fn new(prev: Option<PageId>) -> Self {
        Self {
            prev,
            hash: None,
            inner: BTreeMap::new(),
        }
    }
}
