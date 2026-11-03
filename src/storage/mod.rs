mod catalog;
mod download_naming;
mod downloads;
mod local;
mod share_rules;
mod shares;

pub use catalog::{CatalogError, CatalogFile, CatalogFolder, ShareCatalog};
pub use download_naming::{
    DEFAULT_BASENAME_BYTE_LIMIT, candidate_basenames, download_basename, download_folder,
    incomplete_basename,
};
pub use downloads::{Download, DownloadRequest, DownloadStore, OpenedDownload};
pub use local::{LocalDownloads, LocalShares, ScanMode, SharedFolder};
pub use share_rules::{ShareFilters, check_share_name, escape_backslashes};
pub use shares::{OpenedShare, ScanError, ScanProgress, ShareKey, ShareSource};
