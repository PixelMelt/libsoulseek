use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use super::registry::{Registry, TransferKey};
use super::speed::SpeedMeter;
use super::{
    CONNECTION_CLOSED, CONNECTION_TIMEOUT, LOCAL_FILE_ERROR, StorageDone, StorageSender,
    TRANSFER_REQUEST_TIMEOUT, TransferIds, TransferPhase, TransferRejectReason, USER_OFFLINE,
    VIRTUAL_PATH_LIMIT, peer_reason, spawn_storage,
};
use crate::client::users::Users;
use crate::client::{AbortResult, EnqueueResult, RetryResult, TransferWork};
use crate::network::{ConnId, NetworkCommand, NetworkHandle, OpenDownload};
use crate::protocol::PeerMessage;
use crate::storage::{Download, DownloadRequest, DownloadStore};
use crate::types::{
    FileAttributes, FileInfo, TransferDirection, TransferId, TransferSnapshot, TransferStatus,
};

const QUEUE_POSITION_INTERVAL: Duration = Duration::from_secs(300);
const CHECK_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECTION_RETRY_INTERVAL: Duration = Duration::from_secs(180);
const IO_RETRY_INTERVAL: Duration = Duration::from_secs(900);
const MIN_LIMITED_BATCH: usize = 5;
const RECOVERY_BATCH: usize = 200;

const REQUEST_TIMED_OUT: &str = "request timed out";
const UPLOAD_FAILED: &str = "upload failed";
const PLACEMENT_ERROR: &str = "cannot place finished download";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recovery {
    UserOnline,
    Connection,
    Io,
}

fn recovery(phase: &TransferPhase) -> Option<Recovery> {
    let TransferPhase::Failed(reason) = phase else {
        return None;
    };
    match reason.as_str() {
        USER_OFFLINE => Some(Recovery::UserOnline),
        CONNECTION_TIMEOUT
        | CONNECTION_CLOSED
        | REQUEST_TIMED_OUT
        | UPLOAD_FAILED
        | TransferRejectReason::PENDING_SHUTDOWN => Some(Recovery::Connection),
        TransferRejectReason::FILE_READ_ERROR => Some(Recovery::Io),
        reason if is_local_reason(reason) => Some(Recovery::Io),
        _ => None,
    }
}

fn is_local_reason(reason: &str) -> bool {
    reason.starts_with(LOCAL_FILE_ERROR) || reason.starts_with(PLACEMENT_ERROR)
}

fn is_internal_reason(reason: &str) -> bool {
    matches!(
        reason,
        USER_OFFLINE | CONNECTION_TIMEOUT | CONNECTION_CLOSED | REQUEST_TIMED_OUT | UPLOAD_FAILED
    ) || is_local_reason(reason)
}

fn resumes_online(phase: &TransferPhase) -> bool {
    matches!(phase, TransferPhase::Queued | TransferPhase::Limited) || recovery(phase).is_some()
}

fn parks_offline(phase: &TransferPhase) -> bool {
    match phase {
        TransferPhase::Queued | TransferPhase::Limited | TransferPhase::GettingStatus => true,
        phase => matches!(recovery(phase), Some(Recovery::Connection | Recovery::Io)),
    }
}

fn is_queue_limit(reason: &str) -> bool {
    reason == TransferRejectReason::TOO_MANY_FILES
        || reason == TransferRejectReason::TOO_MANY_MEGABYTES
        || reason.starts_with(TransferRejectReason::USER_LIMIT_PREFIX)
}

fn due(at: &mut Instant, interval: Duration) -> bool {
    if at.elapsed() < interval {
        return false;
    }
    *at = Instant::now();
    true
}

struct Transfer {
    id: TransferId,
    username: String,
    virtual_path: String,
    destination: String,
    store: Arc<dyn DownloadStore>,
    size: u64,
    attributes: FileAttributes,
    phase: TransferPhase,
    bytes_done: u64,
    file_path: Option<String>,
    queue_place: u32,
    retry_attempt: bool,
    legacy_attempt: bool,
    size_changed: bool,
    placement: u64,
    activated_at: Option<Instant>,
    speed: SpeedMeter,
}

impl Transfer {
    fn new(
        id: TransferId,
        username: String,
        file: FileInfo,
        destination: String,
        store: Arc<dyn DownloadStore>,
    ) -> Self {
        Self {
            id,
            username,
            virtual_path: file.name,
            destination,
            store,
            size: file.size,
            attributes: file.attributes,
            phase: TransferPhase::Queued,
            bytes_done: 0,
            file_path: None,
            queue_place: 0,
            retry_attempt: false,
            legacy_attempt: false,
            size_changed: false,
            placement: 0,
            activated_at: None,
            speed: SpeedMeter::default(),
        }
    }

    fn download(&self) -> Download {
        Download {
            username: self.username.clone(),
            virtual_path: self.virtual_path.clone(),
            size: self.size,
            destination: self.destination.clone(),
        }
    }

    fn file(&self) -> FileInfo {
        FileInfo {
            name: self.virtual_path.clone(),
            size: self.size,
            attributes: self.attributes.clone(),
        }
    }

    fn needs_watch(&self) -> bool {
        self.phase.is_active() || recovery(&self.phase).is_some()
    }

    fn snapshot(&self) -> TransferSnapshot {
        TransferSnapshot {
            id: self.id,
            direction: TransferDirection::Download,
            username: self.username.clone(),
            virtual_path: self.virtual_path.clone(),
            destination: Some(self.destination.clone()),
            size: self.size,
            bytes_done: self.bytes_done,
            status: self.phase.status(),
            failure_reason: match &self.phase {
                TransferPhase::Failed(reason) => Some(reason.clone()),
                _ => None,
            },
            file_path: self.file_path.clone(),
            queue_place: self.queue_place,
            speed_bps: if self.phase == TransferPhase::Transferring {
                self.speed.bps()
            } else {
                0
            },
            attributes: self.attributes.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryStep {
    Request,
    Park,
}

#[derive(Debug, Default)]
struct QueueLimit {
    batch: usize,
    held: VecDeque<TransferKey>,
}

type Discard = (Arc<dyn DownloadStore>, Download);

pub(in crate::client) type CheckAnswer = (
    Vec<TransferKey>,
    u64,
    Result<Vec<io::Result<Option<String>>>, String>,
);

pub(in crate::client) struct Downloads {
    net: NetworkHandle,
    store: Arc<dyn DownloadStore>,
    storage: StorageSender,
    transfers: Registry<Transfer>,
    epoch: u64,
    checks: VecDeque<u64>,
    answers: HashMap<u64, CheckAnswer>,
    closing: HashMap<ConnId, TransferKey>,
    placing: HashMap<u64, TransferKey>,
    discards: HashMap<TransferKey, Vec<Discard>>,
    seeded: Option<u64>,
    queue_limits: HashMap<String, QueueLimit>,
    recovery_queue: VecDeque<TransferKey>,
    recovery_pending: HashMap<TransferKey, RecoveryStep>,
    queue_positions_at: Instant,
    connection_retry_at: Instant,
    io_retry_at: Instant,
}

fn discard(store: Arc<dyn DownloadStore>, download: Download) {
    tokio::spawn(async move {
        let (username, virtual_path) = (download.username.clone(), download.virtual_path.clone());
        if let Err(error) = store.discard(download).await {
            warn!(username, virtual_path, %error, "cannot discard a cleared download");
        }
    });
}

impl Downloads {
    pub fn new(net: NetworkHandle, store: Arc<dyn DownloadStore>, storage: StorageSender) -> Self {
        Self {
            net,
            store,
            storage,
            transfers: Registry::default(),
            epoch: 0,
            checks: VecDeque::new(),
            answers: HashMap::new(),
            closing: HashMap::new(),
            placing: HashMap::new(),
            discards: HashMap::new(),
            seeded: None,
            queue_limits: HashMap::new(),
            recovery_queue: VecDeque::new(),
            recovery_pending: HashMap::new(),
            queue_positions_at: Instant::now(),
            connection_retry_at: Instant::now(),
            io_retry_at: Instant::now(),
        }
    }

    pub fn set_store(&mut self, store: Arc<dyn DownloadStore>) {
        self.store = store;
    }

    fn destination(&self, username: &str, virtual_path: &str, root: Option<&str>) -> String {
        self.store.destination(&DownloadRequest {
            username,
            virtual_path,
            root,
        })
    }

    pub fn seed(&mut self, seed: TransferSnapshot) {
        let phase = TransferPhase::from_seed(&seed);
        let key = (seed.username.clone(), seed.virtual_path.clone());
        let destination = seed
            .destination
            .unwrap_or_else(|| self.destination(&seed.username, &seed.virtual_path, None));
        let file = FileInfo {
            name: seed.virtual_path,
            size: seed.size,
            attributes: seed.attributes,
        };
        let mut transfer = Transfer::new(
            seed.id,
            seed.username,
            file,
            destination,
            self.store.clone(),
        );
        transfer.phase = phase;
        transfer.bytes_done = seed.bytes_done;
        transfer.file_path = seed.file_path;
        self.transfers.insert(seed.id, key, transfer);
    }

    pub fn check_seeded(&mut self) {
        let queued = self
            .transfers
            .keys_where(|transfer| transfer.phase == TransferPhase::Queued);
        if queued.is_empty() {
            return;
        }
        let epoch = self.next_epoch();
        self.seeded = Some(epoch);
        for key in &queued {
            self.transfers.get_mut(key).unwrap().phase = TransferPhase::Checking(epoch);
        }
        self.check(self.store.clone(), epoch, queued);
    }

    pub fn enqueue(
        &mut self,
        ids: &mut TransferIds,
        users: &mut Users,
        username: String,
        file: FileInfo,
        root: Option<&str>,
    ) -> (EnqueueResult, Vec<TransferWork>) {
        if file.name.len() > VIRTUAL_PATH_LIMIT {
            return (EnqueueResult::PathTooLong, Vec::new());
        }
        let Some(id) = self.admissible_id(ids, &username, &file.name) else {
            return (EnqueueResult::AlreadyActive, Vec::new());
        };
        let store = self.store.clone();
        let destination = self.destination(&username, &file.name, root);
        let epoch = self.next_epoch();
        users.watch(&username);
        let (key, admitted) = self.admit(id, username, file, destination, store.clone(), epoch);
        self.check(store, epoch, vec![key]);
        (EnqueueResult::Enqueued, vec![admitted])
    }

    pub fn enqueue_folder(
        &mut self,
        ids: &mut TransferIds,
        users: &mut Users,
        username: String,
        files: Vec<FileInfo>,
        root: &str,
    ) -> Vec<TransferWork> {
        let store = self.store.clone();
        let epoch = self.next_epoch();
        let mut work = Vec::with_capacity(files.len());
        let mut keys = Vec::new();
        for file in files {
            if file.name.len() > VIRTUAL_PATH_LIMIT {
                warn!(username, "skipping a folder file whose path is too long");
                continue;
            }
            let Some(id) = self.admissible_id(ids, &username, &file.name) else {
                continue;
            };
            let destination = self.destination(&username, &file.name, Some(root));
            let (key, admitted) = self.admit(
                id,
                username.clone(),
                file,
                destination,
                store.clone(),
                epoch,
            );
            keys.push(key);
            work.push(admitted);
        }
        if !keys.is_empty() {
            users.watch(&username);
            self.check(store, epoch, keys);
        }
        work
    }

    fn admissible_id(
        &self,
        ids: &mut TransferIds,
        username: &str,
        virtual_path: &str,
    ) -> Option<TransferId> {
        match self
            .transfers
            .get(&(username.to_owned(), virtual_path.to_owned()))
        {
            Some(existing) if existing.phase.is_active() => {
                debug!(username, virtual_path, "download already in progress");
                None
            }
            Some(existing) => Some(existing.id),
            None => Some(ids.mint()),
        }
    }

    fn next_epoch(&mut self) -> u64 {
        self.epoch += 1;
        self.epoch
    }

    fn admit(
        &mut self,
        id: TransferId,
        username: String,
        file: FileInfo,
        destination: String,
        store: Arc<dyn DownloadStore>,
        epoch: u64,
    ) -> (TransferKey, TransferWork) {
        let key = (username.clone(), file.name.clone());
        self.recovery_pending.remove(&key);
        self.discards.remove(&key);
        let mut transfer = Transfer::new(id, username, file, destination, store);
        transfer.phase = TransferPhase::Checking(epoch);
        let admitted = TransferWork::Update(transfer.snapshot());
        self.transfers.insert(id, key.clone(), transfer);
        (key, admitted)
    }

    fn check(&mut self, store: Arc<dyn DownloadStore>, epoch: u64, keys: Vec<TransferKey>) {
        let downloads = keys
            .iter()
            .map(|key| self.transfers.get(key).unwrap().download())
            .collect();
        let sequence = self.next_epoch();
        self.checks.push_back(sequence);
        let existing = store.existing(downloads);
        let bounded = Box::pin(async move { tokio::time::timeout(CHECK_TIMEOUT, existing).await });
        spawn_storage(&self.storage, bounded, move |found| StorageDone::Checked {
            sequence,
            answer: (
                keys,
                epoch,
                match found {
                    Ok(Ok(results)) => Ok(results),
                    Ok(Err(_)) => Err("the existence check timed out".to_owned()),
                    Err(error) => Err(error),
                },
            ),
        });
    }

    pub fn handle_checked(
        &mut self,
        logged_in: bool,
        sequence: u64,
        answer: CheckAnswer,
    ) -> Vec<TransferWork> {
        self.answers.insert(sequence, answer);
        let mut work = Vec::new();
        while let Some(next) = self.checks.front() {
            let Some((keys, epoch, found)) = self.answers.remove(next) else {
                break;
            };
            self.checks.pop_front();
            work.extend(self.apply_check(logged_in, keys, epoch, found));
        }
        work
    }

    fn apply_check(
        &mut self,
        logged_in: bool,
        keys: Vec<TransferKey>,
        epoch: u64,
        found: Result<Vec<io::Result<Option<String>>>, String>,
    ) -> Vec<TransferWork> {
        let count = keys.len();
        let results = match found {
            Ok(results) if results.len() == count => results,
            Ok(results) => {
                let error = format!(
                    "download store answered {} of {count} existence checks",
                    results.len()
                );
                (0..count)
                    .map(|_| Err(io::Error::other(error.clone())))
                    .collect()
            }
            Err(error) => (0..count)
                .map(|_| Err(io::Error::other(error.clone())))
                .collect(),
        };
        let seeded = self.seeded == Some(epoch);
        let mut unchecked = 0;
        let mut work = Vec::new();
        let mut requests: Vec<(String, Vec<PeerMessage>)> = Vec::new();
        for (key, result) in keys.into_iter().zip(results) {
            let Some(transfer) = self.transfers.get_mut(&key) else {
                continue;
            };
            if transfer.phase != TransferPhase::Checking(epoch) {
                continue;
            }
            match result {
                Ok(Some(location)) => {
                    info!(
                        username = key.0,
                        virtual_path = key.1,
                        location,
                        "file is already downloaded"
                    );
                    transfer.phase = TransferPhase::Finished;
                    transfer.bytes_done = transfer.size;
                    transfer.file_path = Some(location);
                    work.push(TransferWork::Finished {
                        snapshot: transfer.snapshot(),
                        avg_speed_bps: None,
                        delivered_bytes: 0,
                    });
                }
                Err(error) if seeded => {
                    unchecked += 1;
                    debug!(username = key.0, virtual_path = key.1, %error, "cannot check a restored download");
                    transfer.phase = TransferPhase::Queued;
                    let request = PeerMessage::QueueUpload {
                        file: key.1,
                        legacy_client: transfer.legacy_attempt,
                    };
                    match requests.iter_mut().find(|(username, _)| *username == key.0) {
                        Some((_, messages)) => messages.push(request),
                        None => requests.push((key.0, vec![request])),
                    }
                }
                Ok(None) => {
                    transfer.phase = TransferPhase::Queued;
                    let request = PeerMessage::QueueUpload {
                        file: key.1,
                        legacy_client: transfer.legacy_attempt,
                    };
                    match requests.iter_mut().find(|(username, _)| *username == key.0) {
                        Some((_, messages)) => messages.push(request),
                        None => requests.push((key.0, vec![request])),
                    }
                }
                Err(error) => work.extend(self.fail(&key, format!("{LOCAL_FILE_ERROR}: {error}"))),
            }
        }
        if unchecked > 0 {
            warn!(
                unchecked,
                "cannot check restored downloads for existing files, requesting them"
            );
        }
        if logged_in {
            for (username, messages) in requests {
                self.net
                    .send(NetworkCommand::SendPeerMessages { username, messages });
            }
        }
        work
    }

    fn requeue(&mut self, users: &mut Users, keys: Vec<TransferKey>) -> Vec<TransferWork> {
        let epoch = self.next_epoch();
        let mut work = Vec::with_capacity(keys.len());
        let mut checks: Vec<(Arc<dyn DownloadStore>, Vec<TransferKey>)> = Vec::new();
        for key in keys {
            let transfer = self.transfers.get(&key).unwrap();
            let (id, username, file, destination, store) = (
                transfer.id,
                transfer.username.clone(),
                transfer.file(),
                transfer.destination.clone(),
                transfer.store.clone(),
            );
            users.watch(&username);
            let (key, admitted) = self.admit(id, username, file, destination, store.clone(), epoch);
            work.push(admitted);
            match checks
                .iter_mut()
                .find(|(checked, _)| Arc::ptr_eq(checked, &store))
            {
                Some((_, keys)) => keys.push(key),
                None => checks.push((store, vec![key])),
            }
        }
        for (store, keys) in checks {
            self.check(store, epoch, keys);
        }
        work
    }

    pub fn retry(&mut self, users: &mut Users, id: TransferId) -> (RetryResult, Vec<TransferWork>) {
        let Some(key) = self.transfers.key_of(id).cloned() else {
            return (RetryResult::NotFound, Vec::new());
        };
        if self.transfers.get(&key).unwrap().phase.is_active() {
            return (RetryResult::AlreadyActive, Vec::new());
        }
        (RetryResult::Requeued, self.requeue(users, vec![key]))
    }

    pub fn needs_watch(&self, username: &str) -> bool {
        self.transfers
            .values()
            .any(|transfer| transfer.username == username && transfer.needs_watch())
    }

    pub fn abort(&mut self, id: TransferId) -> (AbortResult, Vec<TransferWork>) {
        let Some(key) = self.transfers.key_of(id).cloned() else {
            return (AbortResult::NotFound, Vec::new());
        };
        let transfer = self.transfers.get_mut(&key).unwrap();
        if !transfer.phase.is_active() && recovery(&transfer.phase).is_none() {
            return (AbortResult::Aborted, Vec::new());
        }
        transfer.phase = TransferPhase::Aborted;
        let aborted = TransferWork::Update(transfer.snapshot());
        self.release(&key);
        (AbortResult::Aborted, vec![aborted])
    }

    pub fn clear(&mut self, statuses: Option<&[TransferStatus]>) -> Vec<TransferId> {
        let removed: Vec<(TransferId, bool, Arc<dyn DownloadStore>, Download)> = self
            .transfers
            .values()
            .filter(|transfer| {
                statuses.is_none_or(|statuses| statuses.contains(&transfer.phase.status()))
            })
            .map(|transfer| {
                (
                    transfer.id,
                    transfer.phase == TransferPhase::Finished,
                    transfer.store.clone(),
                    transfer.download(),
                )
            })
            .collect();
        let mut ids = Vec::with_capacity(removed.len());
        for (id, finished, store, download) in removed {
            let (key, detached) = self.transfers.remove(id).unwrap();
            if let Some(conn_id) = detached.conn_id {
                self.net.send(NetworkCommand::CloseConnection(conn_id));
                self.closing.insert(conn_id, key.clone());
            }
            ids.push(id);
            if finished {
                continue;
            }
            if self.in_flight(&key) {
                self.discards
                    .entry(key)
                    .or_default()
                    .push((store, download));
            } else {
                discard(store, download);
            }
        }
        ids
    }

    fn in_flight(&self, key: &TransferKey) -> bool {
        self.closing.values().any(|closing| closing == key)
            || self.placing.values().any(|placing| placing == key)
    }

    fn settle_discards(&mut self, key: &TransferKey) {
        if self.in_flight(key) {
            return;
        }
        for (store, download) in self.discards.remove(key).into_iter().flatten() {
            discard(store, download);
        }
    }

    pub fn start_session(&mut self, users: &mut Users) {
        let now = Instant::now();
        self.queue_positions_at = now;
        self.connection_retry_at = now;
        self.io_retry_at = now;
        for transfer in self.transfers.values() {
            if transfer.needs_watch() {
                users.watch(&transfer.username);
            }
        }
        let queued = self
            .transfers
            .keys_where(|transfer| transfer.phase == TransferPhase::Queued);
        self.schedule(queued, RecoveryStep::Request);
    }

    fn schedule(&mut self, keys: Vec<TransferKey>, step: RecoveryStep) {
        for key in keys {
            if let Entry::Vacant(pending) = self.recovery_pending.entry(key.clone()) {
                pending.insert(step);
                self.recovery_queue.push_back(key);
            }
        }
    }

    fn unschedule_user(&mut self, username: &str) {
        self.recovery_pending.retain(|key, _| key.0 != username);
        self.recovery_queue.retain(|key| key.0 != username);
    }

    pub fn drain_recovery(&mut self, users: &mut Users) -> Vec<TransferWork> {
        let mut updates = Vec::new();
        let mut requeued = Vec::new();
        let mut handled = 0;
        while handled < RECOVERY_BATCH {
            let Some(key) = self.recovery_queue.pop_front() else {
                break;
            };
            let Some(step) = self.recovery_pending.remove(&key) else {
                continue;
            };
            let Some(transfer) = self.transfers.get(&key) else {
                continue;
            };
            match (step, &transfer.phase) {
                (RecoveryStep::Request, TransferPhase::Queued) => self.send_queue_request(key),
                (RecoveryStep::Request, phase) if resumes_online(phase) => requeued.push(key),
                (RecoveryStep::Park, phase) if parks_offline(phase) => {
                    updates.push(self.park_offline(&key));
                }
                _ => continue,
            }
            handled += 1;
        }
        if !requeued.is_empty() {
            updates.extend(self.requeue(users, requeued));
        }
        updates
    }

    fn park_offline(&mut self, key: &TransferKey) -> TransferWork {
        self.release(key);
        let transfer = self.transfers.get_mut(key).unwrap();
        transfer.phase = TransferPhase::Failed(USER_OFFLINE.into());
        transfer.activated_at = None;
        transfer.queue_place = 0;
        transfer.legacy_attempt = false;
        transfer.size_changed = false;
        TransferWork::Update(transfer.snapshot())
    }

    fn send_queue_request(&mut self, key: TransferKey) {
        let transfer = self.transfers.get(&key).unwrap();
        let legacy_client = transfer.legacy_attempt;
        self.net.peer(
            key.0,
            PeerMessage::QueueUpload {
                file: key.1,
                legacy_client,
            },
        );
    }

    pub fn request_queue_positions(&mut self) {
        if !due(&mut self.queue_positions_at, QUEUE_POSITION_INTERVAL) {
            return;
        }
        let mut requests: HashMap<String, Vec<PeerMessage>> = HashMap::new();
        for transfer in self
            .transfers
            .values()
            .filter(|transfer| transfer.phase == TransferPhase::Queued)
        {
            requests.entry(transfer.username.clone()).or_default().push(
                PeerMessage::PlaceInQueueRequest {
                    file: transfer.virtual_path.clone(),
                    legacy_client: transfer.legacy_attempt,
                },
            );
        }
        for (username, messages) in requests {
            self.net
                .send(NetworkCommand::SendPeerMessages { username, messages });
        }
    }

    pub fn user_offline(&mut self, username: &str) {
        self.unschedule_user(username);
        let keys = self
            .transfers
            .keys_where(|transfer| transfer.username == username && parks_offline(&transfer.phase));
        self.schedule(keys, RecoveryStep::Park);
    }

    pub fn user_online(&mut self, username: &str) {
        self.unschedule_user(username);
        let keys = self.transfers.keys_where(|transfer| {
            transfer.username == username && resumes_online(&transfer.phase)
        });
        self.schedule(keys, RecoveryStep::Request);
    }

    pub fn retry_failed(&mut self) {
        let connection = due(&mut self.connection_retry_at, CONNECTION_RETRY_INTERVAL);
        let io = due(&mut self.io_retry_at, IO_RETRY_INTERVAL);
        if !connection && !io {
            return;
        }
        let keys = self
            .transfers
            .keys_where(|transfer| match recovery(&transfer.phase) {
                Some(Recovery::Connection) => connection,
                Some(Recovery::Io) => io,
                Some(Recovery::UserOnline) | None => false,
            });
        self.schedule(keys, RecoveryStep::Request);
    }

    pub fn release_limited(&mut self) {
        if self.queue_limits.is_empty() {
            return;
        }
        let busy: HashSet<&str> = self
            .transfers
            .values()
            .filter(|transfer| {
                matches!(
                    transfer.phase,
                    TransferPhase::Queued | TransferPhase::Checking(_)
                )
            })
            .map(|transfer| transfer.username.as_str())
            .chain(self.recovery_pending.keys().map(|key| key.0.as_str()))
            .collect();
        let drained: Vec<String> = self
            .queue_limits
            .keys()
            .filter(|username| !busy.contains(username.as_str()))
            .cloned()
            .collect();
        for username in drained {
            let limit = self.queue_limits.get_mut(&username).unwrap();
            let mut released = Vec::new();
            while released.len() < limit.batch {
                let Some(key) = limit.held.pop_front() else {
                    break;
                };
                if self
                    .transfers
                    .get(&key)
                    .is_some_and(|transfer| transfer.phase == TransferPhase::Limited)
                {
                    released.push(key);
                }
            }
            if limit.held.is_empty() {
                self.queue_limits.remove(&username);
            }
            self.schedule(released, RecoveryStep::Request);
        }
    }

    pub fn queue_place(
        &mut self,
        username: &str,
        virtual_path: &str,
        place: u32,
    ) -> Option<TransferWork> {
        let transfer = self
            .transfers
            .get_mut(&(username.to_owned(), virtual_path.to_owned()))?;
        if transfer.phase != TransferPhase::Queued {
            return None;
        }
        transfer.queue_place = place;
        Some(TransferWork::Progress(transfer.snapshot()))
    }

    pub fn handle_transfer_request(
        &mut self,
        username: &str,
        token: u32,
        file: &str,
        filesize: Option<u64>,
    ) -> Vec<TransferWork> {
        let key = (username.to_owned(), file.to_owned());
        let token_taken = self
            .transfers
            .key_by_token(username, token)
            .is_some_and(|holder| *holder != key);
        if token_taken {
            debug!(
                username,
                token,
                virtual_path = file,
                "transfer request reuses a token held by another download"
            );
        }
        let accepted = match self.transfers.get_mut(&key) {
            Some(transfer)
                if !token_taken
                    && matches!(
                        transfer.phase,
                        TransferPhase::Checking(_)
                            | TransferPhase::Queued
                            | TransferPhase::Limited
                            | TransferPhase::GettingStatus
                            | TransferPhase::Failed(_)
                    ) =>
            {
                if let Some(size) = filesize
                    && size > 0
                {
                    if transfer.size != size {
                        transfer.size_changed = true;
                    }
                    transfer.size = size;
                }
                transfer.phase = TransferPhase::GettingStatus;
                transfer.activated_at = Some(Instant::now());
                transfer.queue_place = 0;
                Ok(TransferWork::Update(transfer.snapshot()))
            }
            Some(transfer) if transfer.phase == TransferPhase::Finished => {
                Err(TransferRejectReason::COMPLETE)
            }
            _ => Err(TransferRejectReason::CANCELLED),
        };
        if accepted.is_ok() {
            self.transfers.attach_token(&key, token);
        }
        self.net.peer(
            username,
            PeerMessage::TransferResponse {
                token,
                allowed: accepted.is_ok(),
                reason: accepted.as_ref().err().map(|reason| (*reason).to_owned()),
                filesize: None,
            },
        );
        accepted.into_iter().collect()
    }

    pub fn handle_file_transfer_init(
        &mut self,
        username: &str,
        token: u32,
        conn_id: ConnId,
    ) -> Vec<TransferWork> {
        let Some(key) = self.transfers.key_by_token(username, token).cloned() else {
            debug!(
                username,
                token, "file transfer init with unknown token, closing"
            );
            self.net.send(NetworkCommand::CloseConnection(conn_id));
            return Vec::new();
        };
        if self.transfers.conn_of(&key).is_some() {
            self.net.send(NetworkCommand::CloseConnection(conn_id));
            return Vec::new();
        }
        self.transfers.attach_conn(&key, conn_id);
        let transfer = self.transfers.get_mut(&key).unwrap();
        transfer.activated_at = None;
        transfer.retry_attempt = false;
        transfer.phase = TransferPhase::Transferring;
        transfer.queue_place = 0;
        transfer.speed.reset();
        let size = transfer.size;
        let started = TransferWork::Update(transfer.snapshot());
        info!(username, virtual_path = key.1, size, "download started");
        let (store, download, restart) = (
            transfer.store.clone(),
            transfer.download(),
            transfer.size_changed,
        );
        let open = OpenDownload::new(move || {
            Box::pin(async move {
                store
                    .open(download, restart)
                    .await
                    .map(|opened| (opened.writer, opened.offset))
            })
        });
        self.net.send(NetworkCommand::DownloadFile {
            conn_id,
            open,
            size,
        });
        vec![started]
    }

    pub fn handle_download_progress(
        &mut self,
        conn_id: ConnId,
        bytes_left: u64,
    ) -> Vec<TransferWork> {
        let Some(key) = self.transfers.key_by_conn(conn_id).cloned() else {
            return Vec::new();
        };
        let transfer = self.transfers.get_mut(&key).unwrap();
        transfer.bytes_done = transfer.size.saturating_sub(bytes_left);
        if bytes_left == 0 {
            return self.finish(&key);
        }
        transfer.speed.sample(transfer.bytes_done);
        vec![TransferWork::Progress(transfer.snapshot())]
    }

    pub fn handle_transfer_error(&mut self, conn_id: ConnId, error: &str) -> Vec<TransferWork> {
        let Some(key) = self.transfers.key_by_conn(conn_id).cloned() else {
            return Vec::new();
        };
        self.fail(&key, format!("{LOCAL_FILE_ERROR}: {error}"))
    }

    pub fn handle_file_connection_closed(&mut self, conn_id: ConnId) -> Vec<TransferWork> {
        if let Some(key) = self.closing.remove(&conn_id) {
            self.settle_discards(&key);
        }
        let Some(key) = self.transfers.key_by_conn(conn_id).cloned() else {
            return Vec::new();
        };
        self.transfers.detach_conn(&key);
        match self.transfers.get(&key).unwrap().phase {
            TransferPhase::Transferring => self.fail(&key, CONNECTION_CLOSED.into()),
            _ => Vec::new(),
        }
    }

    pub fn handle_upload_denied(
        &mut self,
        username: &str,
        file: &str,
        reason: &str,
    ) -> Vec<TransferWork> {
        let key = (username.to_owned(), file.to_owned());
        let Some(transfer) = self.transfers.get(&key) else {
            return Vec::new();
        };
        match transfer.phase {
            TransferPhase::Queued => {}
            TransferPhase::Limited if !is_queue_limit(reason) => {}
            _ => return Vec::new(),
        }
        if reason == TransferRejectReason::FILE_NOT_SHARED
            && transfer.phase == TransferPhase::Queued
            && !transfer.legacy_attempt
        {
            info!(
                username,
                virtual_path = file,
                "file not shared, retrying with latin-1 encoded path"
            );
            self.transfers.get_mut(&key).unwrap().legacy_attempt = true;
            self.send_queue_request(key);
            return Vec::new();
        }
        if is_queue_limit(reason) {
            info!(
                username,
                virtual_path = file,
                reason,
                "uploader queue limit reached, holding download until the queue drains"
            );
            let queued = self
                .transfers
                .values()
                .filter(|transfer| {
                    transfer.username == username && transfer.phase == TransferPhase::Queued
                })
                .count();
            let limit = self.queue_limits.entry(username.to_owned()).or_default();
            limit.batch = queued.saturating_sub(1).max(MIN_LIMITED_BATCH);
            limit.held.push_back(key.clone());
            let transfer = self.transfers.get_mut(&key).unwrap();
            transfer.phase = TransferPhase::Limited;
            transfer.queue_place = 0;
            return vec![TransferWork::Update(transfer.snapshot())];
        }
        let reason = peer_reason(reason);
        if is_internal_reason(&reason) {
            return self.fail(&key, TransferRejectReason::CANCELLED.into());
        }
        self.fail(&key, reason)
    }

    pub fn handle_upload_failed(&mut self, username: &str, file: &str) -> Vec<TransferWork> {
        let key = (username.to_owned(), file.to_owned());
        let Some(transfer) = self.transfers.get(&key) else {
            return Vec::new();
        };
        if matches!(
            transfer.phase,
            TransferPhase::Checking(_)
                | TransferPhase::Placing
                | TransferPhase::Finished
                | TransferPhase::Aborted
        ) || matches!(&transfer.phase, TransferPhase::Failed(reason) if is_local_reason(reason))
        {
            return Vec::new();
        }
        if !transfer.retry_attempt {
            self.release(&key);
            let transfer = self.transfers.get_mut(&key).unwrap();
            transfer.retry_attempt = true;
            transfer.legacy_attempt = true;
            transfer.size_changed = false;
            transfer.phase = TransferPhase::Queued;
            transfer.activated_at = None;
            let requeued = TransferWork::Update(transfer.snapshot());
            self.send_queue_request(key);
            return vec![requeued];
        }
        let updates = self.fail(&key, UPLOAD_FAILED.into());
        self.transfers.get_mut(&key).unwrap().retry_attempt = false;
        updates
    }

    pub fn handle_peer_connection_error(
        &mut self,
        username: &str,
        unsent: &[PeerMessage],
        is_offline: bool,
    ) -> Vec<TransferWork> {
        let reason = if is_offline {
            USER_OFFLINE
        } else {
            CONNECTION_TIMEOUT
        };
        let mut updates = Vec::new();
        for message in unsent {
            if let PeerMessage::QueueUpload { file, .. }
            | PeerMessage::PlaceInQueueRequest { file, .. } = message
            {
                let key = (username.to_owned(), file.clone());
                if self.transfers.get(&key).is_some_and(|transfer| {
                    matches!(
                        transfer.phase,
                        TransferPhase::Queued | TransferPhase::Limited
                    )
                }) {
                    updates.extend(self.fail(&key, reason.into()));
                }
            }
        }
        updates
    }

    pub fn sweep_request_timeouts(&mut self) -> Vec<TransferWork> {
        let expired = self.transfers.keys_where(|transfer| {
            transfer.phase == TransferPhase::GettingStatus
                && transfer
                    .activated_at
                    .is_some_and(|at| at.elapsed() > TRANSFER_REQUEST_TIMEOUT)
        });
        expired
            .iter()
            .flat_map(|key| self.fail(key, REQUEST_TIMED_OUT.into()))
            .collect()
    }

    pub fn reset(&mut self) -> Vec<TransferWork> {
        self.queue_limits.clear();
        self.recovery_queue.clear();
        self.recovery_pending.clear();
        self.closing.clear();
        let waiting: Vec<TransferKey> = self.discards.keys().cloned().collect();
        for key in waiting {
            self.settle_discards(&key);
        }
        let active = self.transfers.keys_where(|transfer| {
            matches!(
                transfer.phase,
                TransferPhase::GettingStatus | TransferPhase::Transferring
            )
        });
        let mut updates = Vec::new();
        for key in active {
            self.transfers.detach(&key);
            let transfer = self.transfers.get_mut(&key).unwrap();
            transfer.phase = TransferPhase::Queued;
            transfer.activated_at = None;
            transfer.legacy_attempt = false;
            transfer.size_changed = false;
            updates.push(TransferWork::Update(transfer.snapshot()));
        }
        self.transfers.detach_conns();
        updates
    }

    fn finish(&mut self, key: &TransferKey) -> Vec<TransferWork> {
        self.transfers.detach_token(key);
        let epoch = self.next_epoch();
        self.placing.insert(epoch, key.clone());
        let transfer = self.transfers.get_mut(key).unwrap();
        transfer.phase = TransferPhase::Placing;
        transfer.placement = epoch;
        transfer.bytes_done = transfer.size;
        let update = TransferWork::Update(transfer.snapshot());
        let placing = key.clone();
        spawn_storage(
            &self.storage,
            transfer.store.finish(transfer.download()),
            move |result| StorageDone::Placed {
                key: placing,
                epoch,
                result: result.unwrap_or_else(|error| Err(io::Error::other(error))),
            },
        );
        vec![update]
    }

    pub fn handle_placement_done(
        &mut self,
        key: &TransferKey,
        epoch: u64,
        result: io::Result<String>,
    ) -> Vec<TransferWork> {
        self.placing.remove(&epoch);
        self.settle_discards(key);
        let Some(transfer) = self.transfers.get(key) else {
            return Vec::new();
        };
        if transfer.phase != TransferPhase::Placing || transfer.placement != epoch {
            debug!(
                username = key.0,
                virtual_path = key.1,
                phase = ?transfer.phase,
                "placement completed for a transfer that moved on"
            );
            return Vec::new();
        }
        match result {
            Ok(location) => {
                let transfer = self.transfers.get_mut(key).unwrap();
                transfer.phase = TransferPhase::Finished;
                info!(
                    username = key.0,
                    virtual_path = key.1,
                    location,
                    "download finished"
                );
                transfer.file_path = Some(location);
                vec![TransferWork::Finished {
                    snapshot: transfer.snapshot(),
                    avg_speed_bps: None,
                    delivered_bytes: transfer.size,
                }]
            }
            Err(error) => self.fail(key, format!("{PLACEMENT_ERROR}: {error}")),
        }
    }

    fn release(&mut self, key: &TransferKey) {
        if let Some(conn_id) = self.transfers.detach(key).conn_id {
            self.net.send(NetworkCommand::CloseConnection(conn_id));
            self.closing.insert(conn_id, key.clone());
        }
    }

    fn fail(&mut self, key: &TransferKey, reason: String) -> Vec<TransferWork> {
        let active = |transfer: &Transfer| transfer.phase.is_active();
        if !self.transfers.get(key).is_some_and(active) {
            return Vec::new();
        }
        self.release(key);
        let transfer = self.transfers.get_mut(key).unwrap();
        transfer.phase = TransferPhase::Failed(reason);
        transfer.legacy_attempt = false;
        transfer.size_changed = false;
        vec![TransferWork::Update(transfer.snapshot())]
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tokio::sync::mpsc;

    use super::*;
    use crate::protocol::ServerRequest;
    use crate::storage::LocalDownloads;

    struct Harness {
        downloads: Downloads,
        commands: mpsc::Receiver<NetworkCommand>,
        storage: mpsc::UnboundedReceiver<StorageDone>,
        ids: TransferIds,
        users: Users,
        logged_in: bool,
        dir: PathBuf,
    }

    fn no_users() -> Users {
        Users::new(HashSet::new(), HashSet::new(), HashSet::new(), Vec::new())
    }

    fn local_store(dir: &std::path::Path, username_subfolders: bool) -> Arc<dyn DownloadStore> {
        Arc::new(
            LocalDownloads::new(
                dir.join("complete"),
                dir.join("incomplete"),
                username_subfolders,
            )
            .unwrap(),
        )
    }

    impl Harness {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("soulseek-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let (net, commands) = crate::network::test_channel();
            let (storage_tx, storage) = mpsc::unbounded_channel();
            Self {
                downloads: Downloads::new(net, local_store(&dir, false), storage_tx),
                commands,
                storage,
                ids: TransferIds::new(&[]),
                users: no_users(),
                logged_in: true,
                dir,
            }
        }

        async fn next_done(&mut self) -> StorageDone {
            tokio::time::timeout(Duration::from_secs(10), self.storage.recv())
                .await
                .expect("a storage completion")
                .expect("the storage channel stays open")
        }

        async fn route(&mut self, done: StorageDone) -> Vec<TransferWork> {
            match done {
                StorageDone::Checked { sequence, answer } => {
                    self.downloads
                        .handle_checked(self.logged_in, sequence, answer)
                }
                StorageDone::Placed { key, epoch, result } => {
                    self.downloads.handle_placement_done(&key, epoch, result)
                }
                StorageDone::Sized { .. } => unreachable!("downloads never size files"),
            }
        }

        async fn settle(&mut self) -> Vec<TransferWork> {
            let mut work = Vec::new();
            while self
                .downloads
                .transfers
                .values()
                .any(|transfer| matches!(transfer.phase, TransferPhase::Checking(_)))
            {
                let done = self.next_done().await;
                work.extend(self.route(done).await);
            }
            work
        }

        async fn placed(&mut self) -> Vec<TransferWork> {
            let done = self.next_done().await;
            assert!(matches!(done, StorageDone::Placed { .. }));
            self.route(done).await
        }

        async fn enqueue(
            &mut self,
            username: &str,
            file: &str,
            root: Option<&str>,
        ) -> (EnqueueResult, Vec<TransferWork>) {
            let (result, mut work) = self.downloads.enqueue(
                &mut self.ids,
                &mut self.users,
                username.into(),
                FileInfo {
                    name: file.into(),
                    size: 100,
                    attributes: FileAttributes::default(),
                },
                root,
            );
            work.extend(self.settle().await);
            (result, work)
        }

        async fn queue_from(&mut self, username: &str, file: &str) -> TransferKey {
            assert_eq!(
                self.enqueue(username, file, None).await.0,
                EnqueueResult::Enqueued
            );
            (username.into(), file.into())
        }

        async fn queue(&mut self, file: &str) -> TransferKey {
            self.queue_from("uploader", file).await
        }

        fn request(&mut self, file: &str, token: u32, size: u64) -> Vec<TransferWork> {
            self.downloads
                .handle_transfer_request("uploader", token, file, Some(size))
        }

        fn start(&mut self, file: &str, token: u32, conn_id: ConnId) -> TransferKey {
            let key = (String::from("uploader"), String::from(file));
            assert_eq!(self.request(file, token, 100).len(), 1);
            let started = self
                .downloads
                .handle_file_transfer_init("uploader", token, conn_id);
            assert_eq!(started.len(), 1);
            assert_eq!(self.phase(&key), TransferPhase::Transferring);
            key
        }

        fn deny(&mut self, key: &TransferKey, reason: &str) -> Vec<TransferWork> {
            self.downloads.handle_upload_denied(&key.0, &key.1, reason)
        }

        fn transfer(&self, key: &TransferKey) -> &Transfer {
            self.downloads.transfers.get(key).unwrap()
        }

        fn phase(&self, key: &TransferKey) -> TransferPhase {
            self.transfer(key).phase.clone()
        }

        fn drain(&mut self) -> Vec<NetworkCommand> {
            let mut commands = Vec::new();
            while let Ok(command) = self.commands.try_recv() {
                commands.push(command);
            }
            commands
        }

        fn closed(&mut self, conn_id: ConnId) -> bool {
            self.drain().iter().any(
                |command| matches!(command, NetworkCommand::CloseConnection(id) if *id == conn_id),
            )
        }

        async fn recover(&mut self) -> Vec<TransferWork> {
            let mut work = self.downloads.drain_recovery(&mut self.users);
            work.extend(self.settle().await);
            work
        }

        fn send_watches(&mut self) {
            self.users.send_watches(&self.downloads.net);
        }

        fn fail_connection(&mut self, key: &TransferKey, is_offline: bool) -> Vec<TransferWork> {
            self.downloads.handle_peer_connection_error(
                &key.0,
                &[PeerMessage::QueueUpload {
                    file: key.1.clone(),
                    legacy_client: false,
                }],
                is_offline,
            )
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn failed(reason: &str) -> TransferPhase {
        TransferPhase::Failed(reason.into())
    }

    fn past(interval: Duration) -> Instant {
        Instant::now()
            .checked_sub(interval + Duration::from_secs(1))
            .unwrap()
    }

    fn statuses(work: &[TransferWork]) -> Vec<TransferStatus> {
        work.iter()
            .map(|item| match item {
                TransferWork::Update(snapshot) => snapshot.status,
                other => panic!("expected an update, got {other:?}"),
            })
            .collect()
    }

    fn server_requests(commands: &[NetworkCommand]) -> Vec<&ServerRequest> {
        commands
            .iter()
            .filter_map(|command| match command {
                NetworkCommand::SendServerMessage(request) => Some(request),
                _ => None,
            })
            .collect()
    }

    fn assert_watches_the_uploader_once(commands: &[NetworkCommand]) {
        assert_eq!(
            server_requests(commands),
            vec![
                &ServerRequest::WatchUser {
                    user: "uploader".into()
                },
                &ServerRequest::GetUserStatus {
                    user: "uploader".into()
                },
            ]
        );
    }

    fn queue_uploads(commands: &[NetworkCommand]) -> Vec<&str> {
        commands
            .iter()
            .flat_map(|command| match command {
                NetworkCommand::SendPeerMessage { message, .. } => std::slice::from_ref(message),
                NetworkCommand::SendPeerMessages { messages, .. } => messages.as_slice(),
                _ => &[],
            })
            .filter_map(|message| match message {
                PeerMessage::QueueUpload { file, .. } => Some(file.as_str()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn folder_downloads_keep_the_remote_hierarchy() {
        let mut harness = Harness::new("hierarchy");
        harness.downloads.set_store(Arc::new(
            LocalDownloads::new("/downloads".into(), "/incomplete".into(), true).unwrap(),
        ));
        let file = "share\\Soulseek\\folder1\\sub1\\file4.mp3";
        let (result, _) = harness
            .enqueue("uploader", file, Some("share\\Soulseek"))
            .await;
        assert_eq!(result, EnqueueResult::Enqueued);
        assert_eq!(
            harness
                .transfer(&("uploader".into(), file.into()))
                .destination,
            "/downloads/uploader/Soulseek/folder1/sub1"
        );
    }

    async fn enqueue_beside_existing(
        name: &str,
        existing_len: usize,
    ) -> (Harness, Vec<TransferWork>) {
        let mut harness = Harness::new(name);
        let destination = harness.dir.join("complete/Album");
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::write(destination.join("song.mp3"), vec![0u8; existing_len]).unwrap();
        let (result, work) = harness
            .enqueue("uploader", "share\\Album\\song.mp3", Some("share\\Album"))
            .await;
        assert_eq!(result, EnqueueResult::Enqueued);
        (harness, work)
    }

    #[tokio::test]
    async fn an_already_downloaded_file_finishes_without_asking_the_peer() {
        let (mut harness, work) = enqueue_beside_existing("skip", 100).await;
        assert!(matches!(
            work.as_slice(),
            [
                TransferWork::Update(_),
                TransferWork::Finished {
                    delivered_bytes: 0,
                    ..
                }
            ]
        ));
        let key = ("uploader".to_owned(), "share\\Album\\song.mp3".to_owned());
        let transfer = harness.transfer(&key);
        assert_eq!(transfer.phase, TransferPhase::Finished);
        assert_eq!(
            transfer.file_path.as_deref(),
            harness.dir.join("complete/Album/song.mp3").to_str()
        );
        assert!(harness.drain().is_empty());
    }

    #[tokio::test]
    async fn a_size_mismatch_still_downloads() {
        let (_harness, work) = enqueue_beside_existing("mismatch", 64).await;
        assert!(matches!(work.as_slice(), [TransferWork::Update(_)]));
    }

    #[tokio::test]
    async fn late_denial_does_not_revive_aborted_transfer() {
        let mut harness = Harness::new("late-denial");
        let key = harness.queue("Music\\song.mp3").await;
        let id = harness.transfer(&key).id;
        let (aborted, _) = harness.downloads.abort(id);
        assert_eq!(aborted, AbortResult::Aborted);
        let updates = harness.deny(&key, TransferRejectReason::FILE_NOT_SHARED);
        assert!(updates.is_empty());
        let transfer = harness.transfer(&key);
        assert_eq!(transfer.phase, TransferPhase::Aborted);
        assert!(!transfer.legacy_attempt);
    }

    #[tokio::test]
    async fn a_token_held_by_another_download_is_refused() {
        let mut harness = Harness::new("token-reuse");
        let first = harness.queue("Music\\a.mp3").await;
        let second = harness.queue("Music\\b.mp3").await;
        harness.start("Music\\a.mp3", 7, 1);
        harness.drain();

        assert!(harness.request("Music\\b.mp3", 7, 100).is_empty());
        assert_eq!(harness.phase(&second), TransferPhase::Queued);
        assert!(harness.drain().iter().any(|command| matches!(
            command,
            NetworkCommand::SendPeerMessage {
                message: PeerMessage::TransferResponse {
                    token: 7,
                    allowed: false,
                    ..
                },
                ..
            }
        )));

        let work = harness.downloads.handle_download_progress(1, 0);
        assert!(matches!(work.as_slice(), [TransferWork::Update(_)]));
        assert_eq!(harness.phase(&first), TransferPhase::Placing);
    }

    #[tokio::test]
    async fn progress_from_a_superseded_connection_is_ignored() {
        let mut harness = Harness::new("stale-conn");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start("Music\\a.mp3", 7, 1);
        let work = harness
            .downloads
            .handle_upload_failed("uploader", "Music\\a.mp3");
        assert_eq!(statuses(&work), vec![TransferStatus::Queued]);
        assert!(harness.closed(1));

        harness.start("Music\\a.mp3", 7, 2);
        let downloads = &mut harness.downloads;
        assert!(downloads.handle_download_progress(1, 0).is_empty());
        assert!(downloads.handle_transfer_error(1, "stale").is_empty());
        assert!(downloads.handle_file_connection_closed(1).is_empty());
        assert_eq!(harness.phase(&key), TransferPhase::Transferring);
    }

    #[tokio::test]
    async fn failing_a_transferring_download_closes_its_connection() {
        let mut harness = Harness::new("fail-close");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start("Music\\a.mp3", 7, 1);
        harness.drain();

        let work = harness.downloads.handle_transfer_error(1, "disk full");
        assert_eq!(statuses(&work), vec![TransferStatus::Failed]);
        assert!(harness.closed(1));
        assert!(harness.downloads.transfers.key_by_conn(1).is_none());
        assert_eq!(
            harness.phase(&key),
            failed(&format!("{LOCAL_FILE_ERROR}: disk full"))
        );
    }

    #[tokio::test]
    async fn denials_and_connection_errors_only_touch_queued_downloads() {
        let mut harness = Harness::new("queued-only");
        let key = harness.queue("Music\\a.mp3").await;
        let requested = harness.request("Music\\a.mp3", 7, 100);
        assert_eq!(statuses(&requested), vec![TransferStatus::Queued]);

        let denied = harness.deny(&key, TransferRejectReason::TOO_MANY_FILES);
        assert!(denied.is_empty());
        let errored = harness.fail_connection(&key, true);
        assert!(errored.is_empty());
        assert_eq!(harness.phase(&key), TransferPhase::GettingStatus);

        harness.start("Music\\a.mp3", 7, 1);
        let placing = harness.downloads.handle_download_progress(1, 0);
        assert_eq!(placing.len(), 1);
        assert!(
            harness
                .downloads
                .handle_upload_failed("uploader", "Music\\a.mp3")
                .is_empty()
        );
        assert_eq!(harness.phase(&key), TransferPhase::Placing);
    }

    #[tokio::test]
    async fn a_denial_reason_from_a_peer_is_bounded_and_cannot_name_an_internal_status() {
        let mut harness = Harness::new("denial-reason");
        let long = harness.queue("Music\\a.mp3").await;
        let _ = harness.deny(&long, &"x".repeat(100_000));
        let TransferPhase::Failed(reason) = harness.phase(&long) else {
            panic!("expected a failed download");
        };
        assert_eq!(reason.len(), 255);

        let spoofed = harness.queue("Music\\b.mp3").await;
        let _ = harness.deny(&spoofed, CONNECTION_TIMEOUT);
        assert_eq!(
            harness.phase(&spoofed),
            failed(TransferRejectReason::CANCELLED)
        );
    }

    #[tokio::test]
    async fn aborting_a_download_that_waits_to_retry_stops_the_retry() {
        let mut harness = Harness::new("abort-waiting");
        let key = harness.queue("Music\\a.mp3").await;
        let _ = harness.fail_connection(&key, false);
        assert_eq!(harness.phase(&key), failed(CONNECTION_TIMEOUT));
        let id = harness.transfer(&key).id;
        let (result, work) = harness.downloads.abort(id);
        assert_eq!(result, AbortResult::Aborted);
        assert_eq!(statuses(&work), vec![TransferStatus::Aborted]);
        harness.downloads.user_online("uploader");
        assert!(harness.recover().await.is_empty());
        assert_eq!(harness.phase(&key), TransferPhase::Aborted);
    }

    #[tokio::test]
    async fn an_overlong_virtual_path_is_refused() {
        let mut harness = Harness::new("long-path");
        let (result, work) = harness
            .enqueue("uploader", &"a".repeat(VIRTUAL_PATH_LIMIT + 1), None)
            .await;
        assert_eq!(result, EnqueueResult::PathTooLong);
        assert!(work.is_empty());
    }

    #[tokio::test]
    async fn limited_downloads_stay_held_until_rejected_or_disconnected() {
        let mut harness = Harness::new("limited-denials");
        let rejected = harness.queue("Music\\a.mp3").await;
        let dropped = harness.queue("Music\\b.mp3").await;
        for key in [&rejected, &dropped] {
            let held = harness.deny(key, TransferRejectReason::TOO_MANY_FILES);
            assert_eq!(statuses(&held), vec![TransferStatus::Queued]);
        }
        assert!(
            harness
                .deny(&rejected, TransferRejectReason::TOO_MANY_MEGABYTES)
                .is_empty()
        );
        assert_eq!(harness.phase(&rejected), TransferPhase::Limited);
        assert_eq!(harness.downloads.queue_limits["uploader"].held.len(), 2);

        let failed_work = harness.deny(&rejected, TransferRejectReason::FILE_NOT_SHARED);
        assert_eq!(statuses(&failed_work), vec![TransferStatus::Failed]);
        let failed_work = harness.fail_connection(&dropped, false);
        assert_eq!(statuses(&failed_work), vec![TransferStatus::Failed]);
        assert_eq!(harness.phase(&dropped), failed(CONNECTION_TIMEOUT));
    }

    #[tokio::test]
    async fn every_requeue_is_published() {
        let mut harness = Harness::new("requeue-updates");
        let key = harness.queue("Music\\a.mp3").await;
        let work = harness.fail_connection(&key, true);
        assert_eq!(statuses(&work), vec![TransferStatus::Failed]);

        harness.downloads.user_online("uploader");
        let requeued = harness.recover().await;
        assert_eq!(statuses(&requeued), vec![TransferStatus::Queued]);

        let work = harness.deny(&key, TransferRejectReason::CANCELLED);
        assert_eq!(statuses(&work), vec![TransferStatus::Failed]);
        let revived = harness.request("Music\\a.mp3", 9, 100);
        assert_eq!(statuses(&revived), vec![TransferStatus::Queued]);
        assert_eq!(harness.phase(&key), TransferPhase::GettingStatus);
    }

    #[tokio::test]
    async fn aborting_a_session_clears_size_changed_and_starting_clears_retry() {
        let mut harness = Harness::new("flag-resets");
        let key = harness.queue("Music\\a.mp3").await;
        let _ = harness.request("Music\\a.mp3", 7, 250);
        assert!(harness.transfer(&key).size_changed);
        let _ = harness
            .downloads
            .handle_file_transfer_init("uploader", 7, 1);
        let _ = harness.downloads.handle_transfer_error(1, "reset by peer");
        assert!(!harness.transfer(&key).size_changed);

        let _ = harness.request("Music\\a.mp3", 6, 400);
        assert!(harness.transfer(&key).size_changed);
        let reset = harness.downloads.reset();
        assert_eq!(statuses(&reset), vec![TransferStatus::Queued]);
        assert!(!harness.transfer(&key).size_changed);

        let _ = harness
            .downloads
            .handle_upload_failed("uploader", "Music\\a.mp3");
        let transfer = harness.transfer(&key);
        assert!(transfer.retry_attempt);
        assert_eq!(transfer.phase, TransferPhase::Queued);
        let _ = harness.request("Music\\a.mp3", 8, 250);
        let _ = harness
            .downloads
            .handle_file_transfer_init("uploader", 8, 2);
        assert!(!harness.transfer(&key).retry_attempt);
    }

    #[tokio::test]
    async fn a_folder_is_requested_in_one_batch() {
        let mut harness = Harness::new("folder-batch");
        let files = (0..900)
            .map(|index| FileInfo {
                name: format!("share\\Album\\{index}.mp3"),
                size: 100,
                attributes: FileAttributes::default(),
            })
            .collect();
        let work = harness.downloads.enqueue_folder(
            &mut harness.ids,
            &mut harness.users,
            "uploader".into(),
            files,
            "share\\Album",
        );
        assert_eq!(work.len(), 900);
        assert!(harness.settle().await.is_empty());
        let commands = harness.drain();
        assert!(matches!(
            commands.as_slice(),
            [NetworkCommand::SendPeerMessages { messages, .. }] if messages.len() == 900
        ));
        harness.send_watches();
        assert_watches_the_uploader_once(&harness.drain());
    }

    #[tokio::test]
    async fn a_folder_enqueue_watches_the_uploader_once() {
        let mut harness = Harness::new("watch-once");
        for index in 0..20 {
            harness.queue(&format!("Music\\{index}.mp3")).await;
        }
        assert!(server_requests(&harness.drain()).is_empty());
        harness.send_watches();
        harness.send_watches();
        let sent = harness.drain();
        assert_watches_the_uploader_once(&sent);
        assert!(queue_uploads(&sent).is_empty());
    }

    #[tokio::test]
    async fn offline_uploader_parks_pending_downloads_and_resumes_them_on_return() {
        let mut harness = Harness::new("offline-park");
        let queued = harness.queue("a\\1.mp3").await;
        let dropped = harness.queue("a\\2.mp3").await;
        let denied = harness.queue("a\\3.mp3").await;
        let _ = harness.fail_connection(&dropped, false);
        let _ = harness.deny(&denied, TransferRejectReason::CANCELLED);
        harness.drain();

        harness.downloads.user_offline("uploader");
        let updates = harness.recover().await;
        assert_eq!(updates.len(), 2);
        assert_eq!(harness.phase(&queued), failed(USER_OFFLINE));
        assert_eq!(harness.phase(&dropped), failed(USER_OFFLINE));
        assert_eq!(
            harness.phase(&denied),
            failed(TransferRejectReason::CANCELLED)
        );

        harness.downloads.user_online("uploader");
        assert!(queue_uploads(&harness.drain()).is_empty());
        let updates = harness.recover().await;
        assert_eq!(
            statuses(&updates),
            vec![TransferStatus::Queued, TransferStatus::Queued]
        );
        assert_eq!(harness.phase(&queued), TransferPhase::Queued);
        assert_eq!(harness.phase(&dropped), TransferPhase::Queued);
        assert_eq!(
            queue_uploads(&harness.drain()),
            vec![queued.1.as_str(), dropped.1.as_str()]
        );
    }

    #[tokio::test]
    async fn parking_a_requested_download_forgets_its_token() {
        let mut harness = Harness::new("park-token");
        let key = harness.queue("a\\1.mp3").await;
        let _ = harness.request(&key.1, 7, 100);
        harness.downloads.user_offline("uploader");
        assert_eq!(
            statuses(&harness.recover().await),
            vec![TransferStatus::Failed]
        );
        let downloads = &mut harness.downloads;
        assert!(downloads.transfers.key_by_token("uploader", 7).is_none());
        harness.drain();
        let downloads = &mut harness.downloads;
        assert!(
            downloads
                .handle_file_transfer_init("uploader", 7, 3)
                .is_empty()
        );
        assert!(harness.closed(3));
    }

    #[tokio::test]
    async fn a_new_session_watches_users_with_recoverable_downloads_only() {
        let mut harness = Harness::new("session-watch");
        let offline = harness.queue_from("away", "a\\1.mp3").await;
        let _ = harness.fail_connection(&offline, true);
        let rejected = harness.queue_from("rejecter", "b\\1.mp3").await;
        for _ in 0..2 {
            let _ = harness.deny(&rejected, TransferRejectReason::FILE_NOT_SHARED);
        }
        assert!(matches!(harness.phase(&rejected), TransferPhase::Failed(_)));
        harness.drain();

        let fresh_key = harness.queue_from("fresh", "c\\1.mp3").await;
        harness.drain();

        harness.users = no_users();
        harness.downloads.start_session(&mut harness.users);
        harness.send_watches();
        let sent = harness.drain();
        assert!(queue_uploads(&sent).is_empty());
        assert!(harness.recover().await.is_empty());
        assert_eq!(queue_uploads(&harness.drain()), vec![fresh_key.1.as_str()]);
        let watched: HashSet<&str> = server_requests(&sent)
            .into_iter()
            .filter_map(|request| match request {
                ServerRequest::WatchUser { user } => Some(user.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(watched, HashSet::from(["away", "fresh"]));
        assert!(harness.downloads.needs_watch("away"));
        assert!(!harness.downloads.needs_watch("rejecter"));
    }

    #[tokio::test]
    async fn queue_limit_denials_hold_downloads_until_the_queue_drains() {
        let mut harness = Harness::new("queue-limit");
        let mut keys: Vec<TransferKey> = Vec::new();
        for index in 0..8 {
            keys.push(harness.queue(&format!("a\\{index}.mp3")).await);
        }
        for key in &keys[1..] {
            let updates = harness.deny(key, TransferRejectReason::TOO_MANY_FILES);
            assert_eq!(statuses(&updates), vec![TransferStatus::Queued]);
            assert_eq!(harness.phase(key), TransferPhase::Limited);
        }
        harness.drain();

        harness.downloads.release_limited();
        assert!(harness.recover().await.is_empty());

        let _ = harness.request(&keys[0].1, 7, 100);
        harness.drain();
        harness.downloads.release_limited();
        let released = harness.recover().await;
        assert_eq!(released.len(), MIN_LIMITED_BATCH);
        let sent = harness.drain();
        let expected: Vec<&str> = keys[1..=MIN_LIMITED_BATCH]
            .iter()
            .map(|key| key.1.as_str())
            .collect();
        assert_eq!(queue_uploads(&sent), expected);
        harness.downloads.release_limited();
        assert!(harness.recover().await.is_empty());

        for key in &keys[1..=MIN_LIMITED_BATCH] {
            let _ = harness.deny(key, TransferRejectReason::TOO_MANY_MEGABYTES);
        }
        harness.downloads.release_limited();
        let released = harness.recover().await;
        assert_eq!(released.len(), MIN_LIMITED_BATCH);
        let sent = harness.drain();
        let rotated: Vec<&str> = keys[MIN_LIMITED_BATCH + 1..]
            .iter()
            .chain(&keys[1..=3])
            .map(|key| key.1.as_str())
            .collect();
        assert_eq!(queue_uploads(&sent), rotated);
    }

    #[tokio::test]
    async fn connection_and_io_failures_retry_on_their_timers_and_rejections_do_not() {
        let mut harness = Harness::new("retry-timers");
        let timed_out = harness.queue("a\\1.mp3").await;
        let shutdown = harness.queue("a\\2.mp3").await;
        let io = harness.queue("a\\3.mp3").await;
        let rejected = harness.queue("a\\4.mp3").await;
        let offline = harness.queue_from("gone", "b\\1.mp3").await;
        let _ = harness.fail_connection(&timed_out, false);
        let _ = harness.deny(&shutdown, TransferRejectReason::PENDING_SHUTDOWN);
        harness.start(&io.1, 9, 1);
        let _ = harness
            .downloads
            .handle_transfer_error(1, "No space left on device");
        let _ = harness.deny(&rejected, "Banned");
        let _ = harness.fail_connection(&offline, true);
        harness.drain();

        harness.downloads.start_session(&mut harness.users);
        harness.drain();
        harness.downloads.retry_failed();
        assert!(harness.recover().await.is_empty());

        harness.downloads.connection_retry_at = past(CONNECTION_RETRY_INTERVAL);
        harness.downloads.retry_failed();
        assert_eq!(harness.recover().await.len(), 2);
        assert_eq!(
            queue_uploads(&harness.drain()),
            vec![timed_out.1.as_str(), shutdown.1.as_str()]
        );
        assert!(matches!(harness.phase(&io), TransferPhase::Failed(_)));

        harness.downloads.io_retry_at = past(IO_RETRY_INTERVAL);
        harness.downloads.retry_failed();
        assert_eq!(harness.recover().await.len(), 1);
        assert_eq!(queue_uploads(&harness.drain()), vec![io.1.as_str()]);
        assert_eq!(harness.phase(&rejected), failed("Banned"));
        assert_eq!(harness.phase(&offline), failed(USER_OFFLINE));
    }

    #[tokio::test]
    async fn a_retry_timer_does_not_override_pending_offline_parking() {
        let mut harness = Harness::new("park-over-retry");
        let key = harness.queue("a\\1.mp3").await;
        let _ = harness.fail_connection(&key, false);
        harness.downloads.user_offline("uploader");
        harness.downloads.connection_retry_at = past(CONNECTION_RETRY_INTERVAL);
        harness.downloads.retry_failed();
        harness.drain();
        assert_eq!(
            statuses(&harness.recover().await),
            vec![TransferStatus::Failed]
        );
        assert!(queue_uploads(&harness.drain()).is_empty());
        assert_eq!(harness.phase(&key), failed(USER_OFFLINE));
    }

    #[tokio::test]
    async fn queue_positions_are_requested_in_one_batch_per_user() {
        let mut harness = Harness::new("queue-positions");
        for index in 0..300 {
            harness.queue(&format!("a\\{index}.mp3")).await;
        }
        harness.queue_from("other", "b\\1.mp3").await;
        harness.drain();
        harness.downloads.queue_positions_at = past(QUEUE_POSITION_INTERVAL);
        harness.downloads.request_queue_positions();
        let mut batches: Vec<(String, usize)> = harness
            .drain()
            .into_iter()
            .map(|command| match command {
                NetworkCommand::SendPeerMessages { username, messages } => {
                    (username, messages.len())
                }
                other => panic!("expected a batch, got {other:?}"),
            })
            .collect();
        batches.sort();
        assert_eq!(
            batches,
            vec![("other".to_owned(), 1), ("uploader".to_owned(), 300)]
        );
    }

    #[tokio::test]
    async fn recovery_is_paced_and_dropped_when_the_uploader_leaves() {
        let mut harness = Harness::new("recovery-pacing");
        let total = RECOVERY_BATCH + 50;
        for index in 0..total {
            let key = harness.queue(&format!("a\\{index}.mp3")).await;
            let _ = harness.fail_connection(&key, true);
            harness.drain();
        }
        harness.downloads.user_online("uploader");
        assert_eq!(harness.recover().await.len(), RECOVERY_BATCH);
        assert_eq!(queue_uploads(&harness.drain()).len(), RECOVERY_BATCH);
        harness.downloads.user_offline("uploader");
        assert_eq!(harness.recover().await.len(), RECOVERY_BATCH);
        assert!(harness.recover().await.is_empty());
        assert!(queue_uploads(&harness.drain()).is_empty());
        harness.downloads.user_online("uploader");
        assert_eq!(harness.recover().await.len(), RECOVERY_BATCH);
        assert_eq!(harness.recover().await.len(), 50);
    }

    #[tokio::test]
    async fn a_manual_retry_cancels_the_pending_automatic_one() {
        let mut harness = Harness::new("manual-retry");
        let key = harness.queue("a\\1.mp3").await;
        let _ = harness.fail_connection(&key, true);
        harness.downloads.user_online("uploader");
        let id = harness.transfer(&key).id;
        let (result, _) = harness.downloads.retry(&mut harness.users, id);
        assert_eq!(result, RetryResult::Requeued);
        harness.settle().await;
        let _ = harness.deny(&key, TransferRejectReason::TOO_MANY_FILES);
        harness.drain();
        assert!(harness.recover().await.is_empty());
        assert!(queue_uploads(&harness.drain()).is_empty());
        assert_eq!(harness.phase(&key), TransferPhase::Limited);
    }

    type Scripted = (Duration, Vec<io::Result<Option<String>>>);

    struct ScriptedStore {
        answers: std::sync::Mutex<VecDeque<Scripted>>,
    }

    impl ScriptedStore {
        fn answering(answers: Vec<Vec<io::Result<Option<String>>>>) -> Arc<dyn DownloadStore> {
            Self::answering_after(
                answers
                    .into_iter()
                    .map(|answer| (Duration::ZERO, answer))
                    .collect(),
            )
        }

        fn answering_after(answers: Vec<Scripted>) -> Arc<dyn DownloadStore> {
            Arc::new(Self {
                answers: std::sync::Mutex::new(answers.into()),
            })
        }
    }

    impl DownloadStore for ScriptedStore {
        fn destination(&self, _: &DownloadRequest<'_>) -> String {
            "scripted".into()
        }

        fn existing(
            &self,
            _: Vec<Download>,
        ) -> crate::types::BoxFuture<Vec<io::Result<Option<String>>>> {
            let (delay, answer) = self
                .answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("a scripted answer");
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                answer
            })
        }

        fn open(
            &self,
            _: Download,
            _: bool,
        ) -> crate::types::BoxFuture<io::Result<crate::storage::OpenedDownload>> {
            unreachable!("scripted downloads never open")
        }

        fn finish(&self, _: Download) -> crate::types::BoxFuture<io::Result<String>> {
            unreachable!("scripted downloads never finish")
        }

        fn discard(&self, _: Download) -> crate::types::BoxFuture<io::Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    impl Harness {
        fn opener(&mut self) -> OpenDownload {
            self.drain()
                .into_iter()
                .find_map(|command| match command {
                    NetworkCommand::DownloadFile { open, .. } => Some(open),
                    _ => None,
                })
                .expect("a download command")
        }

        async fn write_partial(&mut self, bytes: &[u8]) -> u64 {
            use tokio::io::AsyncWriteExt;
            let (mut writer, offset) = self.opener().open().await.unwrap();
            writer.write_all(bytes).await.unwrap();
            writer.shutdown().await.unwrap();
            offset
        }

        fn partials(&self) -> usize {
            std::fs::read_dir(self.dir.join("incomplete")).map_or(0, |entries| entries.count())
        }

        async fn wait_for_partials(&self, count: usize) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.partials() != count {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the partial count to settle");
        }
    }

    #[tokio::test]
    async fn a_failed_existence_check_fails_the_download_as_a_local_error() {
        let mut harness = Harness::new("check-error");
        harness
            .downloads
            .set_store(ScriptedStore::answering(vec![vec![Err(io::Error::other(
                "bucket unreachable",
            ))]]));
        let key = harness.queue("Music\\a.mp3").await;
        assert_eq!(
            harness.phase(&key),
            failed(&format!("{LOCAL_FILE_ERROR}: bucket unreachable"))
        );
        assert!(queue_uploads(&harness.drain()).is_empty());
        assert_eq!(recovery(&harness.phase(&key)), Some(Recovery::Io));
    }

    #[tokio::test]
    async fn a_store_answering_the_wrong_number_of_checks_fails_the_whole_batch() {
        let mut harness = Harness::new("check-count");
        harness
            .downloads
            .set_store(ScriptedStore::answering(vec![vec![Ok(None)]]));
        let files = ["share\\a.mp3", "share\\b.mp3"]
            .into_iter()
            .map(|name| FileInfo {
                name: name.into(),
                size: 100,
                attributes: FileAttributes::default(),
            })
            .collect();
        let _ = harness.downloads.enqueue_folder(
            &mut harness.ids,
            &mut harness.users,
            "uploader".into(),
            files,
            "share",
        );
        let work = harness.settle().await;
        assert_eq!(
            statuses(&work),
            vec![TransferStatus::Failed, TransferStatus::Failed]
        );
        assert!(queue_uploads(&harness.drain()).is_empty());
    }

    #[tokio::test]
    async fn an_answer_for_an_earlier_check_is_ignored() {
        let mut harness = Harness::new("stale-check");
        harness.downloads.set_store(ScriptedStore::answering(vec![
            vec![Ok(Some("already here".into()))],
            vec![Ok(None)],
        ]));
        let (_, _) = harness.downloads.enqueue(
            &mut harness.ids,
            &mut harness.users,
            "uploader".into(),
            FileInfo {
                name: "Music\\a.mp3".into(),
                size: 100,
                attributes: FileAttributes::default(),
            },
            None,
        );
        let key = ("uploader".to_owned(), "Music\\a.mp3".to_owned());
        let id = harness.transfer(&key).id;
        let _ = harness.downloads.abort(id);
        let _ = harness.downloads.retry(&mut harness.users, id);
        harness.settle().await;
        assert_eq!(harness.phase(&key), TransferPhase::Queued);
        assert_eq!(queue_uploads(&harness.drain()), vec![key.1.as_str()]);
    }

    #[tokio::test]
    async fn a_started_download_resumes_its_partial_and_restarts_it_when_the_size_changed() {
        let mut harness = Harness::new("resume");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        assert_eq!(harness.write_partial(b"abc").await, 0);
        let _ = harness.downloads.handle_file_connection_closed(1);

        let id = harness.transfer(&key).id;
        let _ = harness.downloads.retry(&mut harness.users, id);
        harness.settle().await;
        harness.start(&key.1, 8, 2);
        assert_eq!(harness.write_partial(b"").await, 3);
        let _ = harness.downloads.handle_file_connection_closed(2);

        let id = harness.transfer(&key).id;
        let _ = harness.downloads.retry(&mut harness.users, id);
        harness.settle().await;
        let _ = harness.request(&key.1, 9, 250);
        let _ = harness
            .downloads
            .handle_file_transfer_init("uploader", 9, 3);
        assert_eq!(harness.write_partial(b"").await, 0);
    }

    #[tokio::test]
    async fn clearing_an_idle_download_discards_its_partial() {
        let mut harness = Harness::new("discard-idle");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(b"abc").await;
        let _ = harness.downloads.handle_transfer_error(1, "reset by peer");
        assert!(
            harness
                .downloads
                .handle_file_connection_closed(1)
                .is_empty()
        );
        assert_eq!(harness.partials(), 1);

        harness.downloads.clear(None);
        harness.wait_for_partials(0).await;
    }

    #[tokio::test]
    async fn clearing_an_aborted_download_waits_for_its_connection_to_close() {
        let mut harness = Harness::new("discard-aborted");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(b"abc").await;
        let _ = harness.downloads.abort(harness.transfer(&key).id);
        assert!(harness.closed(1));

        harness.downloads.clear(None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(harness.partials(), 1);
        assert!(
            harness
                .downloads
                .handle_file_connection_closed(1)
                .is_empty()
        );
        harness.wait_for_partials(0).await;
    }

    #[tokio::test]
    async fn a_server_drop_discards_downloads_whose_connections_it_closed_silently() {
        let mut harness = Harness::new("discard-reset");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(b"abc").await;
        harness.downloads.clear(None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(harness.partials(), 1);

        let _ = harness.downloads.reset();
        harness.wait_for_partials(0).await;
    }

    #[tokio::test]
    async fn clearing_a_transferring_download_discards_once_its_connection_closes() {
        let mut harness = Harness::new("discard-conn");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(b"abc").await;

        assert_eq!(harness.downloads.clear(None).len(), 1);
        assert!(harness.closed(1));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(harness.partials(), 1);

        assert!(
            harness
                .downloads
                .handle_file_connection_closed(1)
                .is_empty()
        );
        harness.wait_for_partials(0).await;
    }

    #[tokio::test]
    async fn a_finished_download_is_placed_and_clearing_it_keeps_the_file() {
        let mut harness = Harness::new("place");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(&[1u8; 100]).await;
        let placing = harness.downloads.handle_download_progress(1, 0);
        assert_eq!(statuses(&placing), vec![TransferStatus::Transferring]);

        let placed = harness.placed().await;
        let location = harness.dir.join("complete/a.mp3");
        assert!(matches!(
            placed.as_slice(),
            [TransferWork::Finished { snapshot, delivered_bytes: 100, .. }]
                if snapshot.file_path.as_deref() == location.to_str()
        ));
        assert_eq!(harness.partials(), 0);

        harness.downloads.clear(None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(std::fs::read(location).unwrap(), vec![1u8; 100]);
    }

    #[tokio::test]
    async fn check_answers_apply_in_admission_order() {
        let mut harness = Harness::new("check-order");
        harness
            .downloads
            .set_store(ScriptedStore::answering_after(vec![
                (Duration::from_millis(100), vec![Ok(None)]),
                (Duration::ZERO, vec![Ok(None)]),
            ]));
        for name in ["Music\\a.mp3", "Music\\b.mp3"] {
            let file = FileInfo {
                name: name.into(),
                size: 100,
                attributes: FileAttributes::default(),
            };
            let _ = harness.downloads.enqueue(
                &mut harness.ids,
                &mut harness.users,
                "uploader".into(),
                file,
                None,
            );
        }
        harness.settle().await;
        assert_eq!(
            queue_uploads(&harness.drain()),
            vec!["Music\\a.mp3", "Music\\b.mp3"]
        );
    }

    #[tokio::test]
    async fn a_download_checked_while_logged_out_is_requested_at_the_next_login() {
        let mut harness = Harness::new("logged-out");
        harness.logged_in = false;
        let key = harness.queue("Music\\a.mp3").await;
        assert_eq!(harness.phase(&key), TransferPhase::Queued);
        assert!(queue_uploads(&harness.drain()).is_empty());

        harness.downloads.start_session(&mut harness.users);
        harness.recover().await;
        assert_eq!(queue_uploads(&harness.drain()), vec![key.1.as_str()]);
    }

    #[tokio::test]
    async fn seeded_queued_downloads_are_checked_before_they_are_requested() {
        let mut harness = Harness::new("seed-check");
        let destination = harness.dir.join("complete");
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::write(destination.join("done.mp3"), vec![0u8; 100]).unwrap();
        for (id, name) in [(1, "Music\\done.mp3"), (2, "Music\\missing.mp3")] {
            harness.downloads.seed(TransferSnapshot {
                id: TransferId(id),
                direction: TransferDirection::Download,
                username: "uploader".into(),
                virtual_path: name.into(),
                destination: Some(destination.display().to_string()),
                size: 100,
                bytes_done: 0,
                status: TransferStatus::Queued,
                failure_reason: None,
                file_path: None,
                queue_place: 0,
                speed_bps: 0,
                attributes: FileAttributes::default(),
            });
        }
        harness.downloads.check_seeded();
        let work = harness.settle().await;
        assert!(matches!(
            work.as_slice(),
            [TransferWork::Finished { snapshot, .. }] if snapshot.id == TransferId(1)
        ));
        let missing = ("uploader".to_owned(), "Music\\missing.mp3".to_owned());
        assert_eq!(harness.phase(&missing), TransferPhase::Queued);
        assert_eq!(queue_uploads(&harness.drain()), vec![missing.1.as_str()]);
    }

    struct GatedStore {
        local: Arc<dyn DownloadStore>,
        gate: Arc<tokio::sync::Notify>,
    }

    impl DownloadStore for GatedStore {
        fn destination(&self, request: &DownloadRequest<'_>) -> String {
            self.local.destination(request)
        }

        fn existing(
            &self,
            downloads: Vec<Download>,
        ) -> crate::types::BoxFuture<Vec<io::Result<Option<String>>>> {
            self.local.existing(downloads)
        }

        fn open(
            &self,
            download: Download,
            restart: bool,
        ) -> crate::types::BoxFuture<io::Result<crate::storage::OpenedDownload>> {
            self.local.open(download, restart)
        }

        fn finish(&self, download: Download) -> crate::types::BoxFuture<io::Result<String>> {
            let (gate, finished) = (self.gate.clone(), self.local.finish(download));
            Box::pin(async move {
                gate.notified().await;
                finished.await
            })
        }

        fn discard(&self, download: Download) -> crate::types::BoxFuture<io::Result<()>> {
            self.local.discard(download)
        }
    }

    #[tokio::test]
    async fn clearing_a_placing_download_waits_for_the_placement() {
        let mut harness = Harness::new("discard-placing");
        let gate = Arc::new(tokio::sync::Notify::new());
        harness.downloads.set_store(Arc::new(GatedStore {
            local: local_store(&harness.dir, false),
            gate: gate.clone(),
        }));
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(&[1u8; 100]).await;
        let _ = harness.downloads.handle_download_progress(1, 0);
        harness.downloads.clear(None);
        assert!(
            harness
                .downloads
                .handle_file_connection_closed(1)
                .is_empty()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(harness.partials(), 1);

        gate.notify_one();
        assert!(harness.placed().await.is_empty());
        assert_eq!(
            std::fs::read(harness.dir.join("complete/a.mp3")).unwrap(),
            vec![1u8; 100]
        );
        assert_eq!(harness.partials(), 0);
    }

    #[tokio::test]
    async fn a_placement_answer_for_an_earlier_placement_is_ignored() {
        let mut harness = Harness::new("stale-placement");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(&[1u8; 100]).await;
        let _ = harness.downloads.handle_download_progress(1, 0);
        let stale = harness
            .downloads
            .handle_placement_done(&key, u64::MAX, Ok("elsewhere".into()));
        assert!(stale.is_empty());
        assert_eq!(harness.phase(&key), TransferPhase::Placing);
        let placed = harness.placed().await;
        assert!(matches!(placed.as_slice(), [TransferWork::Finished { .. }]));
        assert_eq!(
            harness.transfer(&key).file_path.as_deref(),
            harness.dir.join("complete/a.mp3").to_str()
        );
    }

    #[tokio::test]
    async fn enqueueing_a_cleared_download_again_keeps_its_partial() {
        let mut harness = Harness::new("discard-cancel");
        let key = harness.queue("Music\\a.mp3").await;
        harness.start(&key.1, 7, 1);
        harness.write_partial(b"abc").await;
        let _ = harness.downloads.abort(harness.transfer(&key).id);
        harness.downloads.clear(None);
        harness.queue(&key.1).await;
        assert!(
            harness
                .downloads
                .handle_file_connection_closed(1)
                .is_empty()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(harness.partials(), 1);
    }

    #[tokio::test]
    async fn a_retry_watches_the_uploader_again() {
        let mut harness = Harness::new("retry-watch");
        let key = harness.queue("Music\\a.mp3").await;
        let _ = harness.fail_connection(&key, false);
        harness.users = no_users();
        let id = harness.transfer(&key).id;
        let _ = harness.downloads.retry(&mut harness.users, id);
        harness.settle().await;
        harness.drain();
        harness.send_watches();
        assert_watches_the_uploader_once(&harness.drain());
    }

    #[tokio::test(start_paused = true)]
    async fn an_unanswered_existence_check_times_out() {
        let mut harness = Harness::new("check-timeout");
        harness
            .downloads
            .set_store(ScriptedStore::answering_after(vec![(
                Duration::from_secs(3600),
                vec![Ok(None)],
            )]));
        let file = FileInfo {
            name: "Music\\a.mp3".into(),
            size: 100,
            attributes: FileAttributes::default(),
        };
        let _ = harness.downloads.enqueue(
            &mut harness.ids,
            &mut harness.users,
            "uploader".into(),
            file,
            None,
        );
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(CHECK_TIMEOUT + Duration::from_secs(1)).await;
        harness.settle().await;
        let key = ("uploader".to_owned(), "Music\\a.mp3".to_owned());
        assert_eq!(
            harness.phase(&key),
            failed(&format!(
                "{LOCAL_FILE_ERROR}: the existence check timed out"
            ))
        );
    }

    #[tokio::test]
    async fn restored_downloads_that_cannot_be_checked_are_requested_quietly() {
        let mut harness = Harness::new("seed-check-error");
        harness.logged_in = false;
        harness
            .downloads
            .set_store(ScriptedStore::answering(vec![vec![
                Err(io::Error::other("bucket unreachable")),
                Err(io::Error::other("bucket unreachable")),
            ]]));
        for (id, name) in [(1, "Music\\a.mp3"), (2, "Music\\b.mp3")] {
            harness.downloads.seed(TransferSnapshot {
                id: TransferId(id),
                direction: TransferDirection::Download,
                username: "uploader".into(),
                virtual_path: name.into(),
                destination: Some("scripted".into()),
                size: 100,
                bytes_done: 0,
                status: TransferStatus::Queued,
                failure_reason: None,
                file_path: None,
                queue_place: 0,
                speed_bps: 0,
                attributes: FileAttributes::default(),
            });
        }
        harness.downloads.check_seeded();
        assert!(harness.settle().await.is_empty());
        assert!(queue_uploads(&harness.drain()).is_empty());

        harness.downloads.start_session(&mut harness.users);
        harness.recover().await;
        assert_eq!(
            queue_uploads(&harness.drain()),
            vec!["Music\\a.mp3", "Music\\b.mp3"]
        );
    }
}
