mod downloads;
mod folders;
mod queue;
mod registry;
mod speed;
mod uploads;

pub(super) use downloads::{CheckAnswer, Downloads};
pub(super) use folders::{FolderRequests, destination_root};
pub(super) use uploads::Uploads;

use std::io;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::types::{BoxFuture, TransferDirection, TransferId, TransferSnapshot, TransferStatus};
use registry::TransferKey;

const TRANSFER_REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const USER_OFFLINE: &str = "user is offline";
const CONNECTION_TIMEOUT: &str = "connection timeout";
const CONNECTION_CLOSED: &str = "connection closed";
const LOCAL_FILE_ERROR: &str = "local file error";

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueResult {
    Enqueued,
    AlreadyActive,
    PathTooLong,
}

pub const VIRTUAL_PATH_LIMIT: usize = 4096;
const PEER_REASON_LIMIT: usize = 255;

pub(super) fn peer_reason(reason: &str) -> String {
    reason.chars().take(PEER_REASON_LIMIT).collect()
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryResult {
    Requeued,
    AlreadyActive,
    NotFound,
}

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortResult {
    Aborted,
    NotFound,
}

#[derive(Debug)]
pub enum TransferWork {
    Progress(TransferSnapshot),
    Update(TransferSnapshot),
    Finished {
        snapshot: TransferSnapshot,
        avg_speed_bps: Option<u32>,
        delivered_bytes: u64,
    },
    Removed {
        direction: TransferDirection,
        ids: Vec<TransferId>,
    },
}

pub(in crate::client) enum StorageDone {
    Checked {
        sequence: u64,
        answer: CheckAnswer,
    },
    Placed {
        key: TransferKey,
        epoch: u64,
        result: io::Result<String>,
    },
    Sized {
        key: TransferKey,
        token: u32,
        result: io::Result<u64>,
    },
}

pub(in crate::client) type StorageSender = mpsc::UnboundedSender<StorageDone>;

fn spawn_storage<T: Send + 'static>(
    storage: &StorageSender,
    work: BoxFuture<T>,
    done: impl FnOnce(Result<T, String>) -> StorageDone + Send + 'static,
) {
    let storage = storage.clone();
    let task = tokio::spawn(work);
    tokio::spawn(async move {
        let result = task
            .await
            .map_err(|error| format!("storage task panicked: {error}"));
        let _ = storage.send(done(result));
    });
}

pub(super) struct TransferIds {
    next: u64,
}

impl TransferIds {
    pub(super) fn new(seeds: &[TransferSnapshot]) -> Self {
        Self {
            next: seeds.iter().map(|seed| seed.id.0).max().unwrap_or(0) + 1,
        }
    }

    pub(super) fn mint(&mut self) -> TransferId {
        let id = TransferId(self.next);
        self.next += 1;
        id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TransferPhase {
    Checking(u64),
    Queued,
    Limited,
    GettingStatus,
    Transferring,
    Placing,
    Finished,
    Aborted,
    Failed(String),
}

impl TransferPhase {
    pub(super) fn status(&self) -> TransferStatus {
        match self {
            Self::Checking(_) | Self::Queued | Self::Limited | Self::GettingStatus => {
                TransferStatus::Queued
            }
            Self::Transferring | Self::Placing => TransferStatus::Transferring,
            Self::Finished => TransferStatus::Finished,
            Self::Aborted => TransferStatus::Aborted,
            Self::Failed(_) => TransferStatus::Failed,
        }
    }

    pub(super) fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Checking(_)
                | Self::Queued
                | Self::Limited
                | Self::GettingStatus
                | Self::Transferring
                | Self::Placing
        )
    }

    pub(super) fn from_seed(seed: &TransferSnapshot) -> Self {
        assert!(
            seed.is_settled(),
            "transfer {} was seeded without settle_after_restart",
            seed.id.0
        );
        match seed.status {
            TransferStatus::Queued => Self::Queued,
            TransferStatus::Transferring => unreachable!("settled seeds are never transferring"),
            TransferStatus::Finished => Self::Finished,
            TransferStatus::Aborted => Self::Aborted,
            TransferStatus::Failed => Self::Failed(seed.failure_reason.clone().unwrap_or_default()),
        }
    }
}

pub(super) struct TransferRejectReason;

impl TransferRejectReason {
    pub(super) const QUEUED: &'static str = "Queued";
    pub(super) const COMPLETE: &'static str = "Complete";
    pub(super) const CANCELLED: &'static str = "Cancelled";
    pub(super) const FILE_NOT_SHARED: &'static str = "File not shared.";
    pub(super) const TOO_MANY_FILES: &'static str = "Too many files";
    pub(super) const TOO_MANY_MEGABYTES: &'static str = "Too many megabytes";
    pub(super) const USER_LIMIT_PREFIX: &'static str = "User limit of";
    pub(super) const PENDING_SHUTDOWN: &'static str = "Pending shutdown.";
    pub(super) const FILE_READ_ERROR: &'static str = "File read error.";
    pub(super) const REPEATED: &'static str =
        "Already sent to you several times; message me if your client is stuck";
}
