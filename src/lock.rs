use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

use crate::atomic::*;

/// a fast lock
#[repr(C)]
pub struct FastLock<T> {
    inner: UnsafeCell<T>,
    lock: AtomicBool,
}

#[allow(unsafe_code)]
unsafe impl<T: Send> Sync for FastLock<T> {}

#[allow(unsafe_code)]
unsafe impl<T: Send> Send for FastLock<T> {}

impl<T> FastLock<T> {
    #[cfg(not(loom))]
    pub const fn new(inner: T) -> FastLock<T> {
        FastLock { lock: AtomicBool::new(false), inner: UnsafeCell::new(inner) }
    }

    #[cfg(loom)]
    pub fn new(inner: T) -> FastLock<T> {
        FastLock { lock: AtomicBool::new(false), inner: UnsafeCell::new(inner) }
    }

    pub fn try_lock(&self) -> Option<FastLockGuard<'_, T>> {
        let lock_result = self.lock.compare_exchange_weak(false, true, Acquire, Acquire);

        let success = lock_result.is_ok();

        if success {
            Some(FastLockGuard { mu: self })
        } else {
            None
        }
    }
}

pub struct FastLockGuard<'a, T> {
    mu: &'a FastLock<T>,
}

impl<'a, T> Drop for FastLockGuard<'a, T> {
    fn drop(&mut self) {
        assert!(self.mu.lock.swap(false, Release));
    }
}

impl<'a, T> Deref for FastLockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        #[allow(unsafe_code)]
        unsafe {
            &*self.mu.inner.get()
        }
    }
}

impl<'a, T> DerefMut for FastLockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        #[allow(unsafe_code)]
        unsafe {
            &mut *self.mu.inner.get()
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(loom)]
    #[test]
    fn test_fastlock() {
        loom::model(|| {
            let count = Arc::new(AtomicUsize::new(0));
            let count_clone = count.clone();

            let lock = Arc::new(FastLock::new(0));
            let lock_clone = lock.clone();

            loom::thread::spawn(move || {
                count_clone.fetch_add(1, Release);
                loop {
                    if let Some(mut lock) = lock_clone.try_lock() {
                        *lock = *lock + 1;
                        break
                    }
                }
            })
            .join();

            assert_eq!(*lock.try_lock().unwrap(), count.load(Relaxed));
        });
    }
}
