/// A promise which is used for io-heavy tasks.
mod pool;
use crate::sync::*;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

#[derive(Debug)]
struct PromiseState<T> {
    /// if the promise-pair is rejected
    rejected: bool,
    /// empty or fullfilled
    item: Option<T>,
    /// the waker of a future
    waker: Option<Waker>,
}

impl<T> PromiseState<T> {
    fn filled(&self) -> bool {
        self.item.is_some()
    }
}

impl<T> Default for PromiseState<T> {
    fn default() -> Self {
        PromiseState { rejected: false, item: None, waker: None }
    }
}

#[derive(Debug)]
pub struct Promise<T> {
    state: Arc<Mutex<PromiseState<T>>>,
    cdv: Arc<Condvar>,
}

#[derive(Clone, Debug)]
pub struct PromiseResolver<T>(Arc<PromiseResolverInner<T>>);

impl<T> std::ops::Deref for PromiseResolver<T> {
    type Target = PromiseResolverInner<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct PromiseResolverInner<T> {
    state: Arc<Mutex<PromiseState<T>>>,
    cdv: Arc<Condvar>,
}

impl<T> Promise<T> {
    /// Spawn task in adaptive thread pool
    /// NB: should use this only for io heavy task, use `Promise::pair()` otherwise
    pub fn new<F>(work: F) -> Self
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + Clone + 'static,
    {
        pool::spawn(work)
    }

    /// If this promise is rejected
    pub fn rejected(&self) -> bool {
        self.state.lock().rejected
    }

    /// Make a promise pair
    pub fn pair() -> (PromiseResolver<T>, Self) {
        let state = Arc::new(Mutex::new(PromiseState::<T>::default()));
        let cdv = Arc::new(Condvar::new());

        let promise = Promise { state: state.clone(), cdv: cdv.clone() };

        let resolver = PromiseResolverInner { state, cdv };

        (PromiseResolver(Arc::new(resolver)), promise)
    }

    pub fn resolve(self) -> Option<T> {
        let mut state = self.state.lock();

        // if the promise is not filled and it is not rejected
        // wait for the next condition variable
        while !(state.filled() || state.rejected) {
            self.cdv.wait(&mut state);
        }

        state.item.take()
    }

    pub fn timeout(&self, mut timeout: Duration) -> Result<Option<T>, ()> {
        let mut state = self.state.lock();

        // if the promise is not filled and it is not rejected
        // wait for the next condition variable with timeout
        while !(state.filled() || state.rejected) {
            let start = Instant::now();
            let waited = self.cdv.wait_for(&mut state, timeout);

            if waited.timed_out() {
                return Err(())
            }

            // keep the remaining time
            timeout = if let Some(timeout) = timeout.checked_sub(start.elapsed()) {
                timeout
            } else {
                Duration::from_nanos(0)
            }
        }

        Ok(state.item.take())
    }
}

impl<T> PromiseResolver<T> {
    pub fn resolve(self, item: T) {
        let mut state = self.state.lock();

        state.item.replace(item);

        if let Some(waker) = state.waker.take() {
            waker.wake();
        }

        drop(state);

        self.cdv.notify_all();
    }

    pub fn reject(self) {
        let mut state = self.state.lock();

        // the promise is rejected
        state.rejected = true;
        drop(state);

        self.cdv.notify_all();
    }
}

impl<T> Drop for PromiseResolverInner<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock();

        // the promise is unwired, so is rejected
        state.rejected = true;
        drop(state);

        self.cdv.notify_all();
    }
}

impl<T> Future for Promise<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock();

        // if the promise is fullfilled
        if let Some(item) = state.item.take() {
            Poll::Ready(Some(item))
        } else {
            // if the promise is rejected, nothing will be polled
            if state.rejected {
                Poll::Ready(None)
            } else {
                state.waker = Some(ctx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_promise_resolve() {
        let promise = Promise::new(|| 0);
        assert_eq!(promise.resolve(), Some(0));
    }

    #[test]
    fn test_promise_unwire() {
        // resolve a rejected promise will return None
        let (_, promise) = Promise::<()>::pair();
        assert_eq!(promise.resolve(), None);

        // wait a rejected promise will return None
        let (_, promise) = Promise::<()>::pair();
        assert_eq!(promise.timeout(Duration::from_nanos(0)), Ok(None));
    }

    #[test]
    fn test_promise_timeout() {
        let promise = Promise::new(|| {
            std::thread::sleep(Duration::from_millis(100));
            0
        });
        assert!(promise.timeout(Duration::from_millis(50)).is_err());
        assert_eq!(promise.timeout(Duration::from_millis(100)), Ok(Some(0)));
    }
}
