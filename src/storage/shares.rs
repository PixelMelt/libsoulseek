use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::ShareCatalog;
use crate::types::{BoxFuture, FileReader};

const PROGRESS_INTERVAL: u64 = 1000;

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
    files: Arc<AtomicU64>,
    notify: Arc<dyn Fn(u64) + Send + Sync>,
}

impl ScanProgress {
    pub fn new(cancelled: Arc<AtomicBool>, notify: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            cancelled,
            files: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(notify),
        }
    }

    pub fn add_file(&self) {
        let count = self.files.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(PROGRESS_INTERVAL) {
            (self.notify)(count);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_notifies_every_thousand_files_across_clones() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress = ScanProgress::new(Arc::new(AtomicBool::new(false)), {
            let seen = seen.clone();
            move |count| seen.lock().unwrap().push(count)
        });
        let clone = progress.clone();
        for index in 0..2001 {
            if index % 2 == 0 {
                progress.add_file();
            } else {
                clone.add_file();
            }
        }
        assert_eq!(*seen.lock().unwrap(), vec![1000, 2000]);
    }
}
