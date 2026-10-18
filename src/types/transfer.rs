use serde::{Deserialize, Serialize};

const INTERRUPTED_BY_RESTART: &str = "interrupted by restart";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransferId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Download,
    Upload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferStatus {
    Queued,
    Transferring,
    Finished,
    Aborted,
    Failed,
}

impl TransferStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Finished | Self::Aborted | Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferSnapshot {
    pub id: TransferId,
    pub direction: TransferDirection,
    pub username: String,
    pub virtual_path: String,
    pub folder_path: Option<String>,
    pub size: u64,
    pub bytes_done: u64,
    pub status: TransferStatus,
    pub failure_reason: Option<String>,
    pub file_path: Option<String>,
    pub queue_place: u32,
    pub speed_bps: u32,
    pub attributes: crate::types::FileAttributes,
}

impl TransferSnapshot {
    fn after_restart(&self) -> Option<(TransferStatus, Option<&'static str>)> {
        match (self.direction, self.status) {
            (TransferDirection::Download, TransferStatus::Transferring) => {
                Some((TransferStatus::Queued, None))
            }
            (TransferDirection::Upload, TransferStatus::Queued | TransferStatus::Transferring) => {
                Some((TransferStatus::Failed, Some(INTERRUPTED_BY_RESTART)))
            }
            _ => None,
        }
    }

    pub fn settle_after_restart(&mut self) -> bool {
        let Some((status, reason)) = self.after_restart() else {
            return false;
        };
        self.status = status;
        self.failure_reason = reason.map(str::to_owned);
        true
    }

    pub(crate) fn is_settled(&self) -> bool {
        self.after_restart().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(direction: TransferDirection, status: TransferStatus) -> TransferSnapshot {
        TransferSnapshot {
            id: TransferId(1),
            direction,
            username: "peer".into(),
            virtual_path: "Music\\a.flac".into(),
            folder_path: None,
            size: 10,
            bytes_done: 4,
            status,
            failure_reason: Some("connection timeout".into()),
            file_path: None,
            queue_place: 0,
            speed_bps: 0,
            attributes: crate::types::FileAttributes::default(),
        }
    }

    #[test]
    fn a_restart_requeues_running_downloads_and_fails_open_uploads() {
        let mut download = snapshot(TransferDirection::Download, TransferStatus::Transferring);
        assert!(download.settle_after_restart());
        assert_eq!(download.status, TransferStatus::Queued);
        assert_eq!(download.failure_reason, None);
        assert_eq!(download.bytes_done, 4);
        for status in [TransferStatus::Queued, TransferStatus::Transferring] {
            let mut upload = snapshot(TransferDirection::Upload, status);
            assert!(upload.settle_after_restart());
            assert_eq!(upload.status, TransferStatus::Failed);
            assert_eq!(
                upload.failure_reason.as_deref(),
                Some(INTERRUPTED_BY_RESTART)
            );
            assert!(upload.is_settled());
        }
    }

    #[test]
    fn settled_transfers_are_left_alone() {
        let settled = [
            (TransferDirection::Download, TransferStatus::Queued),
            (TransferDirection::Download, TransferStatus::Failed),
            (TransferDirection::Upload, TransferStatus::Finished),
            (TransferDirection::Upload, TransferStatus::Aborted),
            (TransferDirection::Upload, TransferStatus::Failed),
        ];
        for (direction, status) in settled {
            let mut transfer = snapshot(direction, status);
            assert!(transfer.is_settled());
            assert!(!transfer.settle_after_restart());
            assert_eq!(transfer.status, status);
            assert_eq!(
                transfer.failure_reason.as_deref(),
                Some("connection timeout")
            );
        }
    }
}
