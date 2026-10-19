use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use super::bandwidth::{Bandwidth, Grant};
use super::{
    ConnControl, ConnEvent, FileEvent, PEER_IDLE_TIMEOUT, SharedLimits, SocketReader, write_all,
};
use crate::network::{ConnId, OpenDownload, OpenUpload};
use crate::protocol::{FileOffset, FileTransferInit};
use crate::types::FileWriter;

pub(super) async fn run_file_loop(
    conn_id: ConnId,
    events: mpsc::Sender<ConnEvent>,
    mut control: mpsc::Receiver<ConnControl>,
    limits: SharedLimits,
    mut reader: SocketReader,
    mut writer: BufWriter<OwnedWriteHalf>,
) {
    let mut init_exchanged = false;
    let deadline = Instant::now() + PEER_IDLE_TIMEOUT;
    let error = loop {
        tokio::select! {
            init_result = reader.read_u32_le(), if !init_exchanged => {
                match init_result {
                    Ok(token) => {
                        init_exchanged = true;
                        let event = FileEvent::Init { token };
                        if events.send(ConnEvent::File { conn_id, event }).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => break Some(error.to_string()),
                }
            }
            ctrl = control.recv() => {
                match ctrl {
                    Some(ConnControl::SendFileInit(token)) => {
                        init_exchanged = true;
                        let bytes = FileTransferInit { token }.to_bytes();
                        if write_all(&mut writer, &bytes).await.is_err() {
                            break Some("write failed".into());
                        }
                    }
                    Some(ConnControl::Download { open, size }) => {
                        let task = TransferTask { conn_id, events: &events, control: &mut control, limits: &limits };
                        break run_download(task, &mut writer, &mut reader, open, size).await;
                    }
                    Some(ConnControl::Upload { open, size }) => {
                        let task = TransferTask { conn_id, events: &events, control: &mut control, limits: &limits };
                        break run_upload(task, &mut writer, &mut reader, open, size).await;
                    }
                    Some(ConnControl::Close) | None => break None,
                    Some(other) => unreachable!("invalid file control {other:?}"),
                }
            }
            _ = sleep_until(deadline) => break Some("file init timed out".into()),
        }
    };
    let _ = events.send(ConnEvent::Closed { conn_id, error }).await;
}

async fn take_turn(
    bandwidth: &Bandwidth,
    control: &mut mpsc::Receiver<ConnControl>,
    max_len: u64,
) -> Option<Grant> {
    loop {
        let changed = bandwidth.changed();
        let Some(wait_until) = bandwidth.wait_until() else {
            return Some(bandwidth.grant(max_len));
        };
        tokio::select! {
            _ = sleep_until(wait_until) => return Some(bandwidth.grant(max_len)),
            _ = changed => {}
            ctrl = control.recv() => match ctrl {
                Some(ConnControl::Close) | None => return None,
                Some(other) => unreachable!("invalid transfer control {other:?}"),
            },
        }
    }
}

async fn read_granted(reader: &mut SocketReader, buffer: &mut [u8]) -> std::io::Result<usize> {
    if reader.buffer().is_empty() {
        reader.get_mut().read(buffer).await
    } else {
        reader.read(buffer).await
    }
}

struct TransferTask<'a> {
    conn_id: ConnId,
    events: &'a mpsc::Sender<ConnEvent>,
    control: &'a mut mpsc::Receiver<ConnControl>,
    limits: &'a SharedLimits,
}

impl TransferTask<'_> {
    async fn report(&self, event: FileEvent) {
        let conn_id = self.conn_id;
        let _ = self.events.send(ConnEvent::File { conn_id, event }).await;
    }

    async fn fail(&self, error: impl ToString) -> Option<String> {
        self.report(FileEvent::Error(error.to_string())).await;
        None
    }
}

async fn run_download(
    mut task: TransferTask<'_>,
    writer: &mut BufWriter<OwnedWriteHalf>,
    reader: &mut SocketReader,
    open: OpenDownload,
    size: u64,
) -> Option<String> {
    let (mut file, offset) = match timeout(PEER_IDLE_TIMEOUT, open.open()).await {
        Ok(Ok(opened)) => opened,
        Ok(Err(error)) => return task.fail(error).await,
        Err(_) => return task.fail("opening the download file timed out").await,
    };
    let received = match size.checked_sub(offset) {
        None => Err(task
            .fail(format!(
                "partial download holds {offset} bytes of a {size} byte file"
            ))
            .await),
        Some(0) => Ok(()),
        Some(bytes_left) => receive(&mut task, writer, reader, &mut file, offset, bytes_left).await,
    };
    let closed = timeout(PEER_IDLE_TIMEOUT, file.shutdown()).await;
    match (received, closed) {
        (Ok(()), Ok(Ok(()))) => {
            task.report(FileEvent::DownloadProgress { bytes_left: 0 })
                .await;
            None
        }
        (Ok(()), Ok(Err(error))) => task.fail(error).await,
        (Ok(()), Err(_)) => task.fail("closing the download file timed out").await,
        (Err(outcome), _) => outcome,
    }
}

async fn receive(
    task: &mut TransferTask<'_>,
    writer: &mut BufWriter<OwnedWriteHalf>,
    reader: &mut SocketReader,
    file: &mut FileWriter,
    offset: u64,
    mut bytes_left: u64,
) -> Result<(), Option<String>> {
    if write_all(writer, &FileOffset { offset }.to_bytes())
        .await
        .is_err()
    {
        return Err(Some("write failed".into()));
    }
    task.report(FileEvent::DownloadProgress { bytes_left })
        .await;
    let mut buffer = vec![0u8; 65536];
    let mut last_report = Instant::now();
    let limit = &task.limits.download;
    let _active = limit.join();
    while bytes_left > 0 {
        let max_len = bytes_left.min(buffer.len() as u64);
        let Some(grant) = take_turn(limit, task.control, max_len).await else {
            return Err(None);
        };
        tokio::select! {
            biased;
            ctrl = task.control.recv() => {
                match ctrl {
                    Some(ConnControl::Close) | None => return Err(None),
                    Some(other) => unreachable!("invalid download control {other:?}"),
                }
            }
            read_result = read_granted(reader, &mut buffer[..grant.len]) => {
                match read_result {
                    Ok(0) => return Err(Some("connection closed".into())),
                    Ok(count) => {
                        match timeout(PEER_IDLE_TIMEOUT, file.write_all(&buffer[..count])).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => return Err(task.fail(error).await),
                            Err(_) => return Err(task.fail("writing the download file timed out").await),
                        }
                        bytes_left -= count as u64;
                        limit.charge(&grant, count as u64);
                        if bytes_left > 0 && last_report.elapsed() >= Duration::from_secs(1) {
                            last_report = Instant::now();
                            task.report(FileEvent::DownloadProgress { bytes_left }).await;
                        }
                    }
                    Err(error) => return Err(Some(error.to_string())),
                }
            }
            _ = sleep(PEER_IDLE_TIMEOUT) => return Err(Some("download stalled".into())),
        }
    }
    Ok(())
}

const UPLOAD_WRITE_PIECE: usize = 4096;

async fn write_progressing(
    writer: &mut BufWriter<OwnedWriteHalf>,
    bytes: &[u8],
) -> Result<std::io::Result<()>, tokio::time::error::Elapsed> {
    for piece in bytes.chunks(UPLOAD_WRITE_PIECE) {
        if let Err(error) = timeout(PEER_IDLE_TIMEOUT, write_all(writer, piece)).await? {
            return Ok(Err(error));
        }
    }
    Ok(Ok(()))
}

async fn run_upload(
    task: TransferTask<'_>,
    writer: &mut BufWriter<OwnedWriteHalf>,
    reader: &mut SocketReader,
    open: OpenUpload,
    size: u64,
) -> Option<String> {
    let offset = tokio::select! {
        ctrl = task.control.recv() => match ctrl {
            Some(ConnControl::Close) | None => return None,
            Some(other) => unreachable!("invalid upload control {other:?}"),
        },
        offset = timeout(PEER_IDLE_TIMEOUT, reader.read_u64_le()) => offset,
    };
    let offset = match offset {
        Ok(Ok(offset)) => offset,
        Err(_) => return Some("offset read timed out".into()),
        Ok(Err(error)) => return Some(error.to_string()),
    };
    if offset > size {
        return task
            .fail(format!(
                "peer requested offset {offset} beyond the {size} byte file"
            ))
            .await;
    }
    let mut bytes_sent = 0u64;
    task.report(FileEvent::UploadProgress { offset, bytes_sent })
        .await;

    let opened = tokio::select! {
        ctrl = task.control.recv() => match ctrl {
            Some(ConnControl::Close) | None => return None,
            Some(other) => unreachable!("invalid upload control {other:?}"),
        },
        opened = timeout(PEER_IDLE_TIMEOUT, open.open(offset)) => opened,
    };
    let mut file = match opened {
        Ok(Ok(file)) => file,
        Ok(Err(error)) => return task.fail(error).await,
        Err(_) => return task.fail("opening the shared file timed out").await,
    };

    let mut buffer = vec![0u8; 65536];
    let mut last_report = Instant::now();
    let limit = &task.limits.upload;
    let _active = limit.join();
    loop {
        if offset + bytes_sent >= size {
            task.report(FileEvent::UploadProgress { offset, bytes_sent })
                .await;
            return None;
        }
        let max_len = (size - offset - bytes_sent).min(buffer.len() as u64);
        let grant = take_turn(limit, task.control, max_len).await?;
        let count = match timeout(PEER_IDLE_TIMEOUT, file.read(&mut buffer[..grant.len])).await {
            Ok(Ok(0)) => return task.fail("file truncated").await,
            Ok(Ok(count)) => count,
            Ok(Err(error)) => return task.fail(error).await,
            Err(_) => return task.fail("reading the shared file timed out").await,
        };
        let written = tokio::select! {
            written = write_progressing(writer, &buffer[..count]) => written,
            ctrl = task.control.recv() => match ctrl {
                Some(ConnControl::Close) | None => return None,
                Some(other) => unreachable!("invalid upload control {other:?}"),
            },
        };
        match written {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Some("write failed".into()),
            Err(_) => return Some("upload stalled".into()),
        }
        bytes_sent += count as u64;
        limit.charge(&grant, count as u64);
        if last_report.elapsed() >= Duration::from_secs(1) {
            last_report = Instant::now();
            task.report(FileEvent::UploadProgress { offset, bytes_sent })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::Arc;

    use tokio::io::AsyncSeekExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::network::conn::{SharedTraffic, TransferLimits, split_tracked};
    use crate::types::FileReader;

    fn upload_from(file: std::fs::File) -> OpenUpload {
        OpenUpload::new(move |offset| {
            Box::pin(async move {
                let mut file = tokio::fs::File::from_std(file);
                file.seek(std::io::SeekFrom::Start(offset)).await?;
                Ok(Box::new(file) as FileReader)
            })
        })
    }

    fn download_into(file: std::fs::File) -> OpenDownload {
        OpenDownload::new(move || {
            Box::pin(
                async move { Ok((Box::new(tokio::fs::File::from_std(file)) as FileWriter, 0)) },
            )
        })
    }

    fn file_errors(events: &mut mpsc::Receiver<ConnEvent>) -> Vec<String> {
        let mut errors = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let ConnEvent::File {
                event: FileEvent::Error(error),
                ..
            } = event
            {
                errors.push(error);
            }
        }
        errors
    }

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "soulseek-{label}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
        ))
    }

    struct Harness {
        peer: TcpStream,
        events: mpsc::Receiver<ConnEvent>,
        control: mpsc::Sender<ConnControl>,
        task: JoinHandle<()>,
    }

    async fn start(limits: SharedLimits) -> Harness {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, writer) = split_tracked(stream, SharedTraffic::default());
        let (events_tx, events) = mpsc::channel(256);
        let (control, control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_file_loop(
            1, events_tx, control_rx, limits, reader, writer,
        ));
        Harness {
            peer,
            events,
            control,
            task,
        }
    }

    fn assert_done(events: &mut mpsc::Receiver<ConnEvent>) {
        let mut saw_done = false;
        while let Ok(event) = events.try_recv() {
            match event {
                ConnEvent::Closed { error, .. } => {
                    assert_eq!(error, None);
                    saw_done = true;
                }
                ConnEvent::File {
                    event: FileEvent::Error(error),
                    ..
                } => panic!("unexpected file error {error}"),
                _ => {}
            }
        }
        assert!(saw_done);
    }

    #[tokio::test]
    async fn upload_stops_at_the_advertised_size_when_the_file_grew() {
        let path = temp_path("upload-cap");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&vec![7u8; 200_000])
            .unwrap();
        let file = std::fs::File::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        let mut harness = start(Arc::new(TransferLimits::default())).await;
        harness
            .control
            .send(ConnControl::SendFileInit(9))
            .await
            .unwrap();
        harness
            .control
            .send(ConnControl::Upload {
                open: upload_from(file),
                size: 100_000,
            })
            .await
            .unwrap();
        assert_eq!(harness.peer.read_u32_le().await.unwrap(), 9);
        harness.peer.write_u64_le(1_000).await.unwrap();
        let mut received = Vec::new();
        harness.peer.read_to_end(&mut received).await.unwrap();
        harness.task.await.unwrap();

        assert_eq!(received.len(), 99_000);
        assert_done(&mut harness.events);
    }

    #[tokio::test]
    async fn throttled_download_keeps_bytes_buffered_with_the_init() {
        let payload: Vec<u8> = (0..3_000u32).map(|i| (i % 251) as u8).collect();
        let limits = Arc::new(TransferLimits::default());
        limits.download.set_limit(64 * 1024);
        let mut harness = start(limits).await;
        let mut init = 5u32.to_le_bytes().to_vec();
        init.extend_from_slice(&payload[..1_000]);
        harness.peer.write_all(&init).await.unwrap();
        match harness.events.recv().await.unwrap() {
            ConnEvent::File {
                event: FileEvent::Init { token: 5 },
                ..
            } => {}
            other => panic!("unexpected event {other:?}"),
        }

        let path = temp_path("download");
        let file = std::fs::File::create(&path).unwrap();
        harness
            .control
            .send(ConnControl::Download {
                open: download_into(file),
                size: payload.len() as u64,
            })
            .await
            .unwrap();
        assert_eq!(harness.peer.read_u64_le().await.unwrap(), 0);
        harness.peer.write_all(&payload[1_000..]).await.unwrap();
        harness.task.await.unwrap();

        let mut written = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut written)
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(written, payload);
        assert_done(&mut harness.events);
    }

    #[tokio::test]
    async fn an_upload_offset_beyond_the_file_is_refused() {
        let mut harness = start(Arc::new(TransferLimits::default())).await;
        harness
            .control
            .send(ConnControl::SendFileInit(9))
            .await
            .unwrap();
        let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let open = OpenUpload::new({
            let opened = opened.clone();
            move |_| {
                opened.store(true, std::sync::atomic::Ordering::Relaxed);
                Box::pin(async { Ok(Box::new(tokio::io::empty()) as FileReader) })
            }
        });
        harness
            .control
            .send(ConnControl::Upload { open, size: 10 })
            .await
            .unwrap();
        assert_eq!(harness.peer.read_u32_le().await.unwrap(), 9);
        harness.peer.write_u64_le(11).await.unwrap();
        harness.task.await.unwrap();

        assert!(!opened.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(
            file_errors(&mut harness.events),
            vec!["peer requested offset 11 beyond the 10 byte file".to_owned()]
        );
    }

    #[tokio::test]
    async fn a_download_that_cannot_open_its_file_never_sends_an_offset() {
        let mut harness = start(Arc::new(TransferLimits::default())).await;
        harness.peer.write_u32_le(5).await.unwrap();
        harness.events.recv().await.unwrap();
        let open =
            OpenDownload::new(|| Box::pin(async { Err(std::io::Error::other("storage offline")) }));
        harness
            .control
            .send(ConnControl::Download { open, size: 10 })
            .await
            .unwrap();
        harness.task.await.unwrap();

        let mut sent = Vec::new();
        harness.peer.read_to_end(&mut sent).await.unwrap();
        assert!(sent.is_empty());
        assert_eq!(
            file_errors(&mut harness.events),
            vec!["storage offline".to_owned()]
        );
    }

    #[tokio::test]
    async fn a_partial_larger_than_the_file_fails_without_completing() {
        let mut harness = start(Arc::new(TransferLimits::default())).await;
        harness.peer.write_u32_le(5).await.unwrap();
        harness.events.recv().await.unwrap();
        let open = OpenDownload::new(|| {
            Box::pin(async { Ok((Box::new(tokio::io::sink()) as FileWriter, 12)) })
        });
        harness
            .control
            .send(ConnControl::Download { open, size: 10 })
            .await
            .unwrap();
        harness.task.await.unwrap();

        let mut completed = false;
        let mut errors = Vec::new();
        while let Ok(event) = harness.events.try_recv() {
            match event {
                ConnEvent::File {
                    event: FileEvent::DownloadProgress { bytes_left: 0 },
                    ..
                } => completed = true,
                ConnEvent::File {
                    event: FileEvent::Error(error),
                    ..
                } => errors.push(error),
                _ => {}
            }
        }
        assert!(!completed);
        assert_eq!(
            errors,
            vec!["partial download holds 12 bytes of a 10 byte file".to_owned()]
        );
    }
}
