#![allow(unused)]
use crate::{config::*, context::*, ds::stack::*, iter::*, pagecache::Meta, prelude::*, tree::*};
/// K-V Store Implementation
use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
    path::PathBuf,
    sync::{Arc, RwLock},
};

/// The K-V store
#[derive(Clone)]
pub struct Database {
    /// Database Configuration
    config: Config,
    /// Context for Database
    context: Context,
    // trees: Arc<RwLock<HashMap<Key, TreeBlock>>>,
}

impl Database {
    /// Open a database
    pub fn open(path: PathBuf) -> IResult<Self> {
        let mut config = Config::new(Some(path));
        let context = Context::new(config.clone())?;

        Ok(Self { config, context })
    }

    pub fn new(config: Config) -> IResult<Self> {
        let context = Context::new(config.clone())?;

        Ok(Self { config, context })
    }

    // This is used for common k-v store
    pub fn open_bucket(&self, _keyspace: Key) -> DBResult<TreeBlock> {
        todo!()
    }

    pub fn open_block(&self, hash: &Hash) -> DBResult<TreeBlock> {
        let guard = pin();

        let meta = self.context.meta(&guard)?;
        if let Some(id) = meta.get_block(hash.as_bytes()) {
            TreeBlock::new(self.context.clone(), Some(id), &guard).map(Option::Some)
        } else {
            Ok(None)
        }
    }

    pub fn genesis(&self) -> IResult<TreeBlock> {
        let guard = pin();

        TreeBlock::new(self.context.clone(), None, &guard)
    }

    pub fn path(&self) -> PathBuf {
        self.context.path()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new(Config::default()).unwrap()
    }
}

/// some common property check
mod compile_time_assertions {
    use crate::{prelude::*, *};

    #[allow(unreachable_code)]
    fn assert_database_send_sync() {
        _assert_send_sync::<TreeBlock>(unreachable!());
        _assert_send_sync::<Database>(unreachable!());
    }

    fn _assert_send<S: Send>(_: &S) {}

    fn _assert_send_sync<S: Send + Sync>(_: &S) {}
}
