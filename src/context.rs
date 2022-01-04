use crate::{
    config::*,
    node::Node,
    pagecache::{ConfigBuilder, PageCache},
    prelude::*,
};
use std::ops::Deref;

#[derive(Clone)]
pub struct Context {
    /// Configuration
    pub config: Config,
    /// Pagecache for persistence
    pub pagecache: PageCache<Node>,
}

impl Deref for Context {
    type Target = PageCache<Node>;
    fn deref(&self) -> &Self::Target {
        &self.pagecache
    }
}

impl Context {
    pub fn new(config: Config) -> IResult<Self> {
        let pc = if let Some(ref path) = config.path {
            ConfigBuilder::new().path(path).build()
        } else {
            ConfigBuilder::new().temporary(true).build()
        };

        let pagecache = PageCache::start(pc)?;

        Ok(Self { config, pagecache })
    }
}
