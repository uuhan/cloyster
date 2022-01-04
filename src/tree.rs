#![allow(unused)]
use crate::{
    atomic::*, config::*, context::Context, iter::*, node::Node, pagecache::PageId, prelude::*,
    sync::*,
};
use binary_heap_plus::{BinaryHeap, MinComparator};
/// K-V Store Implementation
use std::{
    collections::{BTreeMap, VecDeque},
    ops::{self, Bound, RangeBounds},
};

#[must_use]
#[derive(Clone)]
pub struct TreeBlock {
    hash: Arc<RwLock<Option<Hash>>>,

    pub(crate) context: Context,

    /// Table ID
    pub(crate) id: PageId,

    /// Cookie for insertion
    pub(crate) cookie: Arc<RwLock<BTreeMap<Key, Entry>>>,
    // TODO: LRU facility

    // TODO: Bloom Filter facility
}

impl TreeBlock {
    /// New TreeBlock
    pub fn new<'a>(
        context: Context,
        // the prev page id
        prev: Option<PageId>,
        guard: &'a Guard,
    ) -> IResult<Self> {
        let cookie = Arc::new(RwLock::new(BTreeMap::new()));
        let hash = Arc::new(RwLock::new(None));

        let (id, _) = context.pagecache.allocate(Node::new(prev), guard)?;

        Ok(Self {
            context,
            hash,
            cookie,
            id,
        })
    }

    /// The preceded block
    pub fn prev(&self) -> DBResult<Self> {
        let guard = pin();
        let context = self.context.clone();
        let page = self.context.get(self.id, &guard)?;

        Ok(page
            .map(|(_, node, _)| {
                let h = node.hash.as_ref().map(|raw| raw.clone().into());
                let hash = Arc::new(RwLock::new(h));
                let cookie = Arc::new(RwLock::new(BTreeMap::new()));

                node.prev.map(|id| Self {
                    context,
                    hash,
                    cookie,
                    id,
                })
            })
            .flatten())
    }

    /// Get value
    pub fn get(&self, key: impl AsRef<[u8]>) -> DBResult<Value> {
        self.get_inner(key.as_ref(), &mut pin())
    }

    pub(crate) fn get_inner(&self, key: &[u8], guard: &mut Guard) -> DBResult<Value> {
        // #1. read from cookie
        {
            let cookie = self.cookie.read();
            match cookie.get(key) {
                Some(Entry::Value { value }) => return Ok(Some(value.clone())),
                Some(Entry::Deletion) => return Ok(None),
                None => {}
            }
        }

        // TODO: lru & bloom-filter

        // #2. lookup through the chined page-id
        let id = self.id;
        let (_, mut node, _) = self.context.get(id, guard)?.unwrap();
        match node.inner.get(key) {
            Some(Entry::Value { value }) => return Ok(Some(value.clone())),
            Some(Entry::Deletion) => return Ok(None),
            None => {}
        }

        while let Some(prev) = node.prev {
            let (_, prev_node, _) = self.context.get(prev, guard)?.unwrap();
            match prev_node.inner.get(key) {
                Some(Entry::Value { value }) => return Ok(Some(value.clone())),
                Some(Entry::Deletion) => return Ok(None),
                None => {}
            }

            node = prev_node;
        }

        Ok(None)
    }

    /// Hash code of current state or the stablized hash
    fn hash(&self, guard: &Guard) -> IResult<Hash> {
        if let Some(hash) = *self.hash.read() {
            return Ok(hash);
        } else {
            let (mut key, node, _) = self.context.get(self.id, guard).unwrap().unwrap();

            let prev = if let Some(prev) = node.prev {
                let (mut key, node, _) = self.context.get(prev, guard).unwrap().unwrap();

                // should not panic
                node.hash.as_ref().map(|raw| raw.clone().into())
            } else {
                None
            };

            let cookie = self.cookie.read();
            let hash = crate::hasher::calc_root(
                prev,
                cookie.iter().filter_map(|(k, v)| {
                    if let Entry::Value { value } = v {
                        Some((k, value))
                    } else {
                        None
                    }
                }),
            );
            return Ok(hash);
        }
    }

    /// Commit this TreeBlock
    pub fn commit(mut self) -> IResult<Hash> {
        let guard = pin();
        if let Some(hash) = *self.hash.read() {
            return Ok(hash);
        }

        let mut hash_rwl = self.hash.write();
        let mut cookie = self.cookie.write();

        let inner = std::mem::replace(&mut *cookie, BTreeMap::new());

        let id = self.id;
        let hash = if let Some((mut key, node, _)) = self.context.get(id, &guard)? {
            let mut node = Node::new(node.prev);

            let prev_hash = node
                .prev
                .map(|prev| {
                    let (mut key, node, _) = self.context.get(prev, &guard).unwrap().unwrap();

                    // should not panic
                    node.hash.as_ref().map(|raw| raw.clone().into())
                })
                .flatten();

            let hash = crate::hasher::calc_root(
                prev_hash,
                inner.iter().filter_map(|(k, v)| {
                    if let Entry::Value { value } = v {
                        Some((k, value))
                    } else {
                        None
                    }
                }),
            );

            node.hash.replace(hash.as_bytes().clone());
            node.inner = inner;

            // stablize the changes
            self.context.link(id, key, node, &guard)?;
            hash
        } else {
            panic!("pid {} should exist in stable storage.", id);
        };

        hash_rwl.replace(hash);

        // update meta-page
        // NB: maybe fairly slow
        self.context
            .cas_block_in_meta(hash.as_bytes(), None, Some(id), &guard)?;

        self.context.flush();

        Ok(hash)
    }

    pub fn commited(&self) -> bool {
        self.hash.read().is_some()
    }

    /// insert a value, returns old value
    pub fn insert(&self, key: Key, value: Value) -> DBResult<Value> {
        self.insert_inner(key, Entry::Value { value })
    }

    /// delete a value, returns old value
    pub fn delete(&self, key: Key) -> DBResult<Value> {
        self.insert_inner(key, Entry::Deletion)
    }

    /// db insertion, we insert operations not value itself.
    fn insert_inner(&self, key: Key, entry: Entry) -> DBResult<Value> {
        if self.commited() {
            return Err(Error::CommitedState);
        }

        match self.cookie.write().insert(key, entry) {
            Some(Entry::Deletion) => Ok(None),
            Some(Entry::Value { value }) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    /// Iterator over database, which is just a (..) Range of db
    pub fn iter(&self) -> Iter {
        self.range(..)
    }

    /// Scan with some prefix
    pub fn scan_prefix(&self, key: &Key) -> Iter {
        let mut upper = key.to_vec();
        while let Some(last) = upper.pop() {
            if last < u8::max_value() {
                upper.push(last + 1);
                return self.range(key..&upper);
            }
        }
        self.range(key..)
    }

    /// Scan with key-range
    pub fn range<R: Clone>(&self, range: R) -> Iter
    where
        R: RangeBounds<Key>,
    {
        Iter::new(self.clone(), range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use once_cell::sync::Lazy;
    use std::collections::BTreeMap;

    static INIT: Lazy<(Database, Hash)> = Lazy::new(|| {
        let db = Database::default();

        let block = db.genesis().unwrap();
        let hash = block.commit().unwrap();
        println!("[test] db path: {:?}, genesis: {:?}", db.path(), hash);

        (db, hash)
    });

    #[cfg(not(loom))]
    #[test]
    fn test_block_persistence() {
        let db = Database::open("./cloyster.db".into()).unwrap();
        let block = db.genesis().unwrap();
        block.insert(b"key".to_vec(), b"value".to_vec()).unwrap();
        let ref hash = block.commit().unwrap();
        drop(db);

        let db = Database::open("./cloyster.db".into()).unwrap();
        let block = db.open_block(hash).unwrap().unwrap();
        assert_eq!(block.get(b"key".to_vec()).unwrap(), Some(b"value".to_vec()));
    }

    #[cfg(not(loom))]
    #[test]
    fn test_open_block() {
        let (db, hash) = &*INIT;
        let block = db.open_block(hash).unwrap();

        assert!(block.is_some(), "open by id should be ok.");
        let block = block.unwrap();

        block.insert(b"210".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"220".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"100".to_vec(), b"1".to_vec()).unwrap();

        let hash = block.commit().unwrap();
        let block = db.open_block(&hash).unwrap().unwrap();
        block.commit().unwrap();
    }

    #[cfg(not(loom))]
    #[test]
    fn test_block_insert_get() {
        let (db, hash) = &*INIT;
        let block = db.open_block(hash).unwrap().unwrap();

        assert_eq!(block.get(&b"0".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"1".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"2".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"3".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"4".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"5".to_vec()).unwrap(), None);

        block.insert(b"0".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"1".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"2".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"3".to_vec(), b"3".to_vec()).unwrap();
        block.insert(b"4".to_vec(), b"4".to_vec()).unwrap();

        assert_eq!(block.get(&b"0".to_vec()).unwrap(), Some(b"0".to_vec()));
        assert_eq!(block.get(&b"1".to_vec()).unwrap(), Some(b"1".to_vec()));
        assert_eq!(block.get(&b"2".to_vec()).unwrap(), Some(b"2".to_vec()));
        assert_eq!(block.get(&b"3".to_vec()).unwrap(), Some(b"3".to_vec()));
        assert_eq!(block.get(&b"4".to_vec()).unwrap(), Some(b"4".to_vec()));
        assert_eq!(block.get(&b"5".to_vec()).unwrap(), None);

        block.delete(b"0".to_vec()).unwrap();
        block.delete(b"1".to_vec()).unwrap();
        block.delete(b"2".to_vec()).unwrap();
        block.delete(b"3".to_vec()).unwrap();
        block.delete(b"4".to_vec()).unwrap();
        block.delete(b"5".to_vec()).unwrap();

        assert_eq!(block.get(&b"0".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"1".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"2".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"3".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"4".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"5".to_vec()).unwrap(), None);

        let hash = block.commit().unwrap();
        let block = db.open_block(&hash).unwrap().unwrap();

        assert_eq!(block.get(&b"0".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"1".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"2".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"3".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"4".to_vec()).unwrap(), None);
        assert_eq!(block.get(&b"5".to_vec()).unwrap(), None);

        block.insert(b"0".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"1".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"2".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"3".to_vec(), b"3".to_vec()).unwrap();
        block.insert(b"4".to_vec(), b"4".to_vec()).unwrap();

        assert_eq!(block.get(&b"0".to_vec()).unwrap(), Some(b"0".to_vec()));
        assert_eq!(block.get(&b"1".to_vec()).unwrap(), Some(b"1".to_vec()));
        assert_eq!(block.get(&b"2".to_vec()).unwrap(), Some(b"2".to_vec()));
        assert_eq!(block.get(&b"3".to_vec()).unwrap(), Some(b"3".to_vec()));
        assert_eq!(block.get(&b"4".to_vec()).unwrap(), Some(b"4".to_vec()));
        assert_eq!(block.get(&b"5".to_vec()).unwrap(), None);
    }

    #[cfg(not(loom))]
    #[test]
    fn test_block_range() {
        let (db, hash) = &*INIT;
        let block = db.open_block(hash).unwrap().unwrap();

        // case #1. empty database
        assert!(block.iter().next().is_none());

        block.insert(b"2".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"1".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"4".to_vec(), b"4".to_vec()).unwrap();
        block.insert(b"3".to_vec(), b"3".to_vec()).unwrap();
        block.insert(b"9".to_vec(), b"*".to_vec()).unwrap();
        block.insert(b"8".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"6".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"7".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"5".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"0".to_vec(), b"0".to_vec()).unwrap();

        // case #2. range from non-empty store
        assert_eq!(
            block.iter().next().transpose().unwrap(),
            Some((b"0".to_vec(), b"0".to_vec()))
        );
        assert_eq!(
            block.iter().last().transpose().unwrap(),
            Some((b"9".to_vec(), b"*".to_vec()))
        );

        // Have: 0 in memtable
        // Have: 2 1 4 3 in l0 sstable

        // case #3. unbound to unbound
        for (r, ref correct) in block.range(..).zip(vec![
            b"0".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"6".to_vec(),
            b"7".to_vec(),
            b"8".to_vec(),
            b"9".to_vec(),
        ]) {
            let (ref k, v) = r.unwrap();
            assert_eq!(k, correct);
        }

        // case #4. inclusive to unbound
        for (r, ref correct) in block.range(b"1".to_vec()..).zip(vec![
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"6".to_vec(),
            b"7".to_vec(),
            b"8".to_vec(),
            b"9".to_vec(),
        ]) {
            let (ref k, v) = r.unwrap();
            assert_eq!(k, correct);
        }

        // case #5. unbound to exclusive
        for (r, ref correct) in block.range(..b"9".to_vec()).zip(vec![
            b"0".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"6".to_vec(),
            b"7".to_vec(),
            b"8".to_vec(),
        ]) {
            let (ref k, v) = r.unwrap();
            assert_eq!(k, correct);
        }

        // case #6. inclusive to exclusive
        for (r, ref correct) in block.range(b"1".to_vec()..b"9".to_vec()).zip(vec![
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"6".to_vec(),
            b"7".to_vec(),
            b"8".to_vec(),
        ]) {
            let (ref k, v) = r.unwrap();
            assert_eq!(k, correct);
        }

        assert_eq!(
            block.iter().last().unwrap().unwrap(),
            (b"9".to_vec(), b"*".to_vec())
        );
    }

    #[cfg(not(loom))]
    #[test]
    fn test_block_scan_prefix() {
        let (db, hash) = &*INIT;
        let block = db.open_block(hash).unwrap().unwrap();

        block.insert(b"210".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"220".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"100".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"12".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"123".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"400".to_vec(), b"4".to_vec()).unwrap();
        block.insert(b"300".to_vec(), b"3".to_vec()).unwrap();
        block.insert(b"010".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"020".to_vec(), b"0".to_vec()).unwrap();

        let result = block
            .scan_prefix(&b"0".to_vec())
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<IResult<Vec<_>>>()
            .unwrap();

        assert_eq!(
            result,
            vec![
                (b"010".to_vec(), b"0".to_vec()),
                (b"020".to_vec(), b"0".to_vec()),
            ]
        );

        let result = block
            .scan_prefix(&b"12".to_vec())
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<IResult<Vec<_>>>()
            .unwrap();

        assert_eq!(
            result,
            vec![
                (b"12".to_vec(), b"1".to_vec()),
                (b"123".to_vec(), b"1".to_vec()),
            ]
        );
    }

    #[cfg(not(loom))]
    #[test]
    fn test_block_iter_count() {
        let (db, hash) = &*INIT;
        let block = db.open_block(hash).unwrap().unwrap();

        block.insert(b"210".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"220".to_vec(), b"2".to_vec()).unwrap();
        block.insert(b"100".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"12".to_vec(), b"1".to_vec()).unwrap();

        let ref hash = block.commit().unwrap();

        let block = db.open_block(hash).unwrap().unwrap();

        block.insert(b"123".to_vec(), b"1".to_vec()).unwrap();
        block.insert(b"400".to_vec(), b"4".to_vec()).unwrap();
        block.insert(b"300".to_vec(), b"3".to_vec()).unwrap();

        assert_eq!(block.iter().count(), 7);

        let ref hash = block.commit().unwrap();
        let block = db.open_block(hash).unwrap().unwrap();

        // two more insertions
        block.insert(b"010".to_vec(), b"0".to_vec()).unwrap();
        block.insert(b"020".to_vec(), b"0".to_vec()).unwrap();

        // delete an old item
        block.delete(b"300".to_vec()).unwrap();

        assert_eq!(block.iter().count(), 8);
    }
}
