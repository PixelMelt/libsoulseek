use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::ShareCatalog;
use crate::types::{BoxFuture, FileReader};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShareKey {
    folder: Box<[u8]>,
    file: Box<[u8]>,
}

impl ShareKey {
    pub fn new(folder: impl Into<Box<[u8]>>, file: impl Into<Box<[u8]>>) -> Self {
        Self {
            folder: folder.into(),
            file: file.into(),
        }
    }

    pub fn folder(&self) -> &[u8] {
        &self.folder
    }

    pub fn file(&self) -> &[u8] {
        &self.file
    }
}

impl fmt::Display for ShareKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/{}",
            String::from_utf8_lossy(&self.folder),
            String::from_utf8_lossy(&self.file)
        )
    }
}

pub struct OpenedShare {
    pub reader: FileReader,
    pub size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("{0}")]
    Source(Box<dyn std::error::Error + Send + Sync>),
    #[error("scan superseded by a newer scan")]
    Superseded,
    #[error("scan task panicked: {reason}")]
    Panicked { reason: String },
}

#[derive(Clone)]
pub struct ScanProgress {
    cancelled: Arc<AtomicBool>,
    notify: Arc<dyn Fn(u64) + Send + Sync>,
}

impl ScanProgress {
    pub fn new(cancelled: Arc<AtomicBool>, notify: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            cancelled,
            notify: Arc::new(notify),
        }
    }

    pub fn files(&self, count: u64) {
        (self.notify)(count);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

pub trait ShareSource: Send + Sync + 'static {
    fn load(&self) -> BoxFuture<Result<Option<ShareCatalog>, ScanError>>;

    fn scan(&self, progress: ScanProgress) -> BoxFuture<Result<ShareCatalog, ScanError>>;

    fn size(&self, key: ShareKey) -> BoxFuture<io::Result<u64>>;

    fn open(&self, key: ShareKey, offset: u64) -> BoxFuture<io::Result<OpenedShare>>;
}
