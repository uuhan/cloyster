/// A simple lock-free stack
use crate::prelude::*;
use either::Either;
use std::{ops::Deref, sync::atomic::Ordering::*};

type CasResult<'g, T> = Either<Shared<'g, Node<T>>, (Shared<'g, Node<T>>, Owned<Node<T>>)>;

#[derive(Debug)]
pub struct Node<T: Send + 'static> {
    pub(crate) inner: T,
    pub(crate) next: Atomic<Node<T>>,
}

impl<T: Send + 'static> Deref for Node<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: Send + 'static> Drop for Node<T> {
    fn drop(&mut self) {
        unsafe {
            let next = self.next.load(Relaxed, unprotected());
            if !next.as_raw().is_null() {
                drop(next.into_owned());
            }
        }
    }
}

#[derive(Debug)]
pub struct Stack<T: Send + 'static> {
    head: Atomic<Node<T>>,
}

impl<T: Send + 'static> Default for Stack<T> {
    fn default() -> Self {
        Self { head: Atomic::null() }
    }
}

impl<T: Send + 'static> Drop for Stack<T> {
    fn drop(&mut self) {
        unsafe {
            let curr = self.head.load(Relaxed, unprotected());
            if !curr.as_raw().is_null() {
                drop(curr.into_owned());
            }
        }
    }
}

/// `compare_and_set` is equivalent to `compare_exchange`
/// with the following mapping for memory orderings:
///
/// Original | Success | Failure
/// -------- | ------- | -------
/// Relaxed  | Relaxed | Relaxed
/// Acquire  | Acquire | Acquire
/// Release  | Release | Relaxed
/// AcqRel   | AcqRel  | Acquire
/// SeqCst   | SeqCst  | SeqCst
impl<T: Clone + Send + Sync + 'static> Stack<T> {
    /// return current head pointer of the stack
    pub fn head<'g>(&self, guard: &'g Guard) -> Shared<'g, Node<T>> {
        self.head.load(Acquire, guard)
    }

    pub fn push(&self, inner: T) {
        let node = Owned::new(Node { inner, next: Atomic::null() });

        unsafe {
            let node = node.into_shared(unprotected());

            loop {
                let head = self.head(unprotected());
                node.deref().next.store(head, Release);
                if self
                    .head
                    .compare_exchange(
                        // compare
                        head,
                        node,
                        // orderings
                        Release,
                        Relaxed,
                        // guard
                        unprotected(),
                    )
                    .is_ok()
                {
                    return
                }
            }
        }
    }

    // cas operation
    pub fn cas<'g>(
        &self,
        old: Shared<'g, Node<T>>,
        new: Owned<Node<T>>,
        guard: &'g Guard,
    ) -> CasResult<'g, T> {
        let res = self.head.compare_exchange(old, new, AcqRel, Acquire, guard);
        match res {
            Ok(success) => {
                if !old.is_null() {
                    unsafe {
                        // reclaim old data
                        guard.defer_destroy(old);
                    }
                }
                // a success cas operation
                Either::Left(success)
            }
            Err(e) => {
                // a failure cas operation
                Either::Right((e.current, e.new))
            }
        }
    }

    // compare and push
    pub fn cap<'g>(
        &self,
        old: Shared<'_, Node<T>>,
        mut node: Owned<Node<T>>,
        guard: &'g Guard,
    ) -> CasResult<'g, T> {
        // pushed node always keeps the prev pointer
        node.next = Atomic::from(old);
        let res = self.head.compare_exchange(old, node, AcqRel, Acquire, guard);

        match res {
            Ok(success) => Either::Left(success),
            Err(e) => {
                // we want to set next to null to prevent
                // the current shared head from being
                // dropped when we drop this node.
                let mut returned = e.new;
                returned.next = Atomic::null();
                Either::Right((e.current, returned))
            }
        }
    }
}
