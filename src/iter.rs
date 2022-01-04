// Iterator over kv-store
use crate::{prelude::*, tree::*};
use binary_heap_plus::{BinaryHeap, MinComparator};
use std::ops::{Bound, RangeBounds};

/// An iterator over keys and values in this K-V store
pub struct Iter {
    pub(crate) block: TreeBlock,
    pub(crate) heap: BinaryHeap<Key, MinComparator>,
    prev: Option<Key>,
    pub(crate) hi: Bound<Key>,
    pub(crate) lo: Bound<Key>,
    guard: Guard,
}

impl Iter {
    pub fn new<R: Clone>(block: TreeBlock, range: R) -> Self
    where
        R: RangeBounds<Key>,
    {
        let id = block.id;
        let lo = range.start_bound().cloned();
        let hi = range.end_bound().cloned();
        let guard = pin();

        // initialize the min-binary-heap
        let mut heap = BinaryHeap::<Key, MinComparator>::new_min();

        // #1. from cookie
        let cookie = block.cookie.read();
        for (k, _) in cookie.range(range.clone()) {
            heap.push(k.clone());
        }

        // TODO: check the boundary of block to enhance this Iterator
        // #2. from chainned block
        let (_, mut node, _) = block.context.get(id, &guard).unwrap().unwrap();
        for (k, _) in node.inner.range(range.clone()) {
            heap.push(k.clone());
        }

        while let Some(prev) = node.prev {
            let (_, prev_node, _) = block.context.get(prev, &guard).unwrap().unwrap();
            for (k, _) in prev_node.inner.range(range.clone()) {
                heap.push(k.clone());
            }

            node = prev_node;
        }

        Iter {
            block: block.clone(),
            heap,
            prev: None,
            lo,
            hi,
            guard,
        }
    }

    fn bounds_collapsed(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Included(ref start), Bound::Included(ref end))
            | (Bound::Included(ref start), Bound::Excluded(ref end))
            | (Bound::Excluded(ref start), Bound::Included(ref end))
            | (Bound::Excluded(ref start), Bound::Excluded(ref end)) => start > end,
            _ => false,
        }
    }

    fn next_inner(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.bounds_collapsed() {
            return None;
        }

        while !self.heap.is_empty() {
            let key = self.heap.pop().unwrap();

            // If there is a previous key
            if let Some(prev) = self.prev.as_ref() {
                // and current key equeals the previous key
                if prev == &key {
                    continue;
                }
            }

            self.prev.replace(key.clone());

            match self.block.get(&key) {
                Ok(Some(value)) => return Some(Ok((key, value))),
                // maybe a Deletion
                Ok(None) => {}
                // error occurs
                _ => {}
            }
        }

        None
    }

    fn last_inner(&mut self) -> Option<<Self as Iterator>::Item> {
        // consume the min-binary-heap
        while !self.heap.is_empty() {
            let key = self.heap.pop().unwrap();
            self.prev.replace(key.clone());
        }

        if let Some(key) = self.prev.take() {
            self.block
                .get(&key)
                .map(|v| v.map(|v| (key, v)))
                .transpose()
        } else {
            None
        }
    }
}

impl Iterator for Iter {
    type Item = IResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner()
    }

    fn last(mut self) -> Option<Self::Item> {
        self.last_inner()
    }
}
