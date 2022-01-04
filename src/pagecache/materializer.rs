use super::*;
use crate::{node::Node, prelude::*};
use std::collections::BTreeMap;

/// A user of a `PageCache` needs to provide a `Materializer` which
/// handles the merging of page fragments.
pub trait Materializer:
    'static + Debug + Clone + Serialize + DeserializeOwned + Send + Sync
{
    /// Used to merge chains of partial pages into a form
    /// that is useful for the `PageCache` owner.
    fn merge(&mut self, other: &Self);
}

impl Materializer for BTreeMap<Key, Value> {
    fn merge(&mut self, other: &Self) {
        self.extend(other.clone().into_iter())
    }
}

impl Materializer for Node {
    fn merge(&mut self, other: &Self) {
        // should not change header data
        debug_assert_eq!(self.prev, other.prev);

        self.hash = other.hash.clone();
        self.inner.extend(other.inner.clone().into_iter())
    }
}
