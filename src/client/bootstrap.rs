use std::sync::Arc;

use crate::storage::{DownloadStore, ShareSource};
use crate::types::{RuntimeConfig, TransferSnapshot};

use super::ProfileSource;

#[derive(Clone)]
pub struct ClientBootstrap {
    pub runtime: RuntimeConfig,
    pub shares: Arc<dyn ShareSource>,
    pub downloads: Arc<dyn DownloadStore>,
    pub profile: Arc<dyn ProfileSource>,
    pub scan_on_startup: bool,
    pub connect_on_startup: bool,
    pub buddies: Vec<String>,
    pub banned: Vec<String>,
    pub ignored: Vec<String>,
    pub ip_bans: Vec<String>,
    pub wishlist: Vec<String>,
    pub liked_interests: Vec<String>,
    pub hated_interests: Vec<String>,
    pub transfers: Vec<TransferSnapshot>,
}
