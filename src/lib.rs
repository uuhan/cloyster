pub mod prelude {
    pub use serde::{Deserialize, Serialize};
    /// Stored Element in Memtable
    #[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
    pub enum Entry {
        Value { value: Value },
        Deletion,
    }
    pub use super::Error;
    pub use blake3::{Hash, Hasher};

    pub use super::{config::Config, tree::TreeBlock};

    pub use crossbeam_epoch::{
        pin, unprotected, Atomic, Collector, Guard, LocalHandle, Owned, Shared,
    };

    pub type TableID = usize;
    pub type BlockID = usize;
    pub type Key = Vec<u8>;
    pub type Value = Vec<u8>;
    pub type Idx = usize;
    pub type IResult<T> = Result<T, Error>;
    pub type DBResult<T> = Result<Option<T>, Error>;
}

mod sync {
    pub use parking_lot::{Condvar, Mutex, RwLock};
    pub use std::sync::Arc;
}

mod atomic {
    #[cfg(loom)]
    mod inner {
        pub use loom::sync::atomic::{
            AtomicBool, AtomicI64, AtomicI64 as AtomicLsn, AtomicU64, AtomicUsize,
        };
    }

    #[cfg(not(loom))]
    mod inner {
        pub use std::sync::atomic::{
            AtomicBool, AtomicI64, AtomicI64 as AtomicLsn, AtomicU64, AtomicUsize,
        };
    }

    pub use inner::*;
    pub use std::sync::atomic::Ordering::*;
}

mod block;
mod config;
mod context;
mod database;
mod ds;
mod hasher;
mod iter;
mod lock;
mod node;
pub mod pagecache;
mod tree;

pub use database::Database;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    IOError(#[from] std::io::Error),
    PCError(#[from] pagecache::Error),
    CommitedState,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("error")
    }
}
