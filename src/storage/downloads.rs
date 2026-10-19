use std::io;

use crate::types::{BoxFuture, FileWriter};

pub struct DownloadRequest<'a> {
    pub username: &'a str,
    pub virtual_path: &'a str,
    pub root: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Download {
    pub username: String,
    pub virtual_path: String,
    pub size: u64,
    pub destination: String,
}

pub struct OpenedDownload {
    pub writer: FileWriter,
    pub offset: u64,
}

pub trait DownloadStore: Send + Sync + 'static {
    fn destination(&self, request: &DownloadRequest<'_>) -> String;

    fn existing(&self, downloads: Vec<Download>) -> BoxFuture<Vec<io::Result<Option<String>>>>;

    fn open(&self, download: Download, restart: bool) -> BoxFuture<io::Result<OpenedDownload>>;

    fn finish(&self, download: Download) -> BoxFuture<io::Result<String>>;

    fn discard(&self, download: Download) -> BoxFuture<io::Result<()>>;
}
