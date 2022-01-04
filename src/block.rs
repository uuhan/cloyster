use crate::prelude::*;
/// Blocks in SSTable
use std::ops::{Bound, RangeBounds};

/// Block for index
#[allow(dead_code)]
pub struct IndexBlock {
    pub(crate) min: Key,
    pub(crate) max: Key,
}

#[allow(dead_code)]
impl IndexBlock {
    pub fn new(min: Key, max: Key) -> Self {
        Self { min, max }
    }

    pub fn min(&self) -> &Key {
        &self.min
    }

    pub fn max(&self) -> &Key {
        &self.max
    }

    // Range Filter
    pub fn overlaps<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<Key>,
    {
        // #1. overlaps if min ∈ range or max ∈ range
        if range.contains(self.min()) || range.contains(self.max()) {
            return true
        }

        // #2. overlaps if range ⊂ [min, max] & range ≠ ∅
        match (&range.start_bound(), &range.end_bound()) {
            (Bound::Included(ref start), Bound::Included(ref end))
            | (Bound::Included(ref start), Bound::Excluded(ref end))
            | (Bound::Excluded(ref start), Bound::Included(ref end))
            | (Bound::Excluded(ref start), Bound::Excluded(ref end)) => {
                // not overlaps if range is ∅
                if start > end {
                    false
                } else {
                    (self.min()..=self.max()).contains(start)
                        && (self.min()..=self.max()).contains(end)
                }
            }

            // ignore infinity bound dut to #1:
            // (∞, y) (x, ∞) (∞, ∞)
            _ => false,
        }
    }
}
