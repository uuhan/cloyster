use crate::sync::*;
/// Configuration of this K-V store
use std::path::PathBuf;

#[derive(Clone, Debug, Default)]
pub struct Config(Arc<ConfigInner>);

impl Config {
    pub fn new(path: Option<PathBuf>) -> Self {
        let inner = ConfigInner { path };
        Self(Arc::new(inner))
    }
}

impl std::ops::Deref for Config {
    type Target = ConfigInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct ConfigInner {
    /// Path to data position
    pub path: Option<PathBuf>,
}

impl Default for ConfigInner {
    fn default() -> Self {
        Self { path: None }
    }
}
