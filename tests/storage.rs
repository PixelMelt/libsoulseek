mod common;

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;

use common::{client_config, start_fake_server};
use soulseek::client::{Client, ClientEvent, EnqueueResult, SearchScope, TransferWork};
use soulseek::storage::{
    CatalogFile, Download, DownloadRequest, DownloadStore, OpenedDownload, OpenedShare, ScanError,
    ScanProgress, ShareCatalog, ShareKey, ShareSource,
};
use soulseek::types::{BoxFuture, FileAttributes, TransferSnapshot};

type Blobs = Arc<Mutex<HashMap<String, Vec<u8>>>>;

struct MemoryShares {
    files: HashMap<String, Vec<u8>>,
}

impl MemoryShares {
    fn file(&self, key: &ShareKey) -> io::Result<Vec<u8>> {
        assert_eq!(key.folder(), b"album");
        String::from_utf8(key.file().to_vec())
            .ok()
            .and_then(|name| self.files.get(&name).cloned())
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    }
}

impl ShareSource for MemoryShares {
    fn load(&self) -> BoxFuture<Result<Option<ShareCatalog>, ScanError>> {
        Box::pin(async {
            Err(ScanError::Source(Box::new(io::Error::other(
                "nothing persisted yet",
            ))))
        })
    }

    fn scan(&self, _: ScanProgress) -> BoxFuture<Result<ShareCatalog, ScanError>> {
        let files = self
            .files
            .iter()
            .map(|(name, bytes)| {
                CatalogFile::new(
                    name.clone(),
                    name.clone().into_bytes(),
                    bytes.len() as u64,
                    0,
                    FileAttributes::default(),
                )
            })
            .collect();
        Box::pin(async move {
            let mut catalog = ShareCatalog::new();
            catalog
                .push_folder("Memory\\Album".into(), b"album".to_vec(), false, files)
                .map_err(|error| ScanError::Source(Box::new(error)))?;
            Ok(catalog)
        })
    }

    fn size(&self, key: ShareKey) -> BoxFuture<io::Result<u64>> {
        let file = self.file(&key);
        Box::pin(async move { file.map(|bytes| bytes.len() as u64) })
    }

    fn open(&self, key: ShareKey, offset: u64) -> BoxFuture<io::Result<OpenedShare>> {
        let file = self.file(&key);
        Box::pin(async move {
            let bytes = file?;
            let size = bytes.len() as u64;
            Ok(OpenedShare {
                reader: Box::new(io::Cursor::new(bytes[offset as usize..].to_vec())),
                size,
            })
        })
    }
}

#[derive(Default)]
struct MemoryDownloads {
    partials: Blobs,
    finished: Blobs,
}

struct MemoryWriter {
    blobs: Blobs,
    key: String,
}

impl AsyncWrite for MemoryWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut blobs = self.blobs.lock().unwrap();
        blobs.get_mut(&self.key).unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl DownloadStore for MemoryDownloads {
    fn destination(&self, request: &DownloadRequest<'_>) -> String {
        format!(
            "memory://{}/{}",
            request.username,
            request.virtual_path.replace('\\', "/")
        )
    }

    fn existing(&self, downloads: Vec<Download>) -> BoxFuture<Vec<io::Result<Option<String>>>> {
        let finished = self.finished.clone();
        Box::pin(async move {
            let finished = finished.lock().unwrap();
            downloads
                .iter()
                .map(|download| {
                    Ok(finished
                        .contains_key(&download.destination)
                        .then(|| download.destination.clone()))
                })
                .collect()
        })
    }

    fn open(&self, download: Download, restart: bool) -> BoxFuture<io::Result<OpenedDownload>> {
        let blobs = self.partials.clone();
        Box::pin(async move {
            let offset = {
                let mut partials = blobs.lock().unwrap();
                let partial = partials.entry(download.destination.clone()).or_default();
                if restart {
                    partial.clear();
                }
                partial.len() as u64
            };
            Ok(OpenedDownload {
                writer: Box::new(MemoryWriter {
                    blobs,
                    key: download.destination,
                }),
                offset,
            })
        })
    }

    fn finish(&self, download: Download) -> BoxFuture<io::Result<String>> {
        let (partials, finished) = (self.partials.clone(), self.finished.clone());
        Box::pin(async move {
            let bytes = partials
                .lock()
                .unwrap()
                .remove(&download.destination)
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
            finished
                .lock()
                .unwrap()
                .insert(download.destination.clone(), bytes);
            Ok(download.destination)
        })
    }

    fn discard(&self, download: Download) -> BoxFuture<io::Result<()>> {
        let partials = self.partials.clone();
        Box::pin(async move {
            partials.lock().unwrap().remove(&download.destination);
            Ok(())
        })
    }
}

async fn wait<E, T>(events: &mut Receiver<E>, mut matcher: impl FnMut(E) -> Option<T>) -> T {
    timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.expect("event channel closed");
            if let Some(value) = matcher(event) {
                return value;
            }
        }
    })
    .await
    .expect("timed out waiting for an event")
}

async fn finished(transfers: &mut Receiver<TransferWork>) -> (TransferSnapshot, u64) {
    wait(transfers, |work| match work {
        TransferWork::Finished {
            snapshot,
            delivered_bytes,
            ..
        } => Some((snapshot, delivered_bytes)),
        _ => None,
    })
    .await
}

#[tokio::test]
async fn clients_share_and_download_through_storage_that_is_not_the_local_disk() {
    let (server_addr, _registry) = start_fake_server().await;
    let payload: Vec<u8> = (0u32..50_000)
        .flat_map(|value| value.to_le_bytes())
        .collect();

    let mut eve_config = client_config(server_addr, "eve");
    eve_config.shares = Arc::new(MemoryShares {
        files: HashMap::from([("song.mp3".to_owned(), payload.clone())]),
    });
    let (_eve, mut eve_events, _eve_transfers) = Client::spawn(eve_config);
    wait(&mut eve_events, |event| match event {
        ClientEvent::SharesInstalled { files: 1, .. } => Some(()),
        _ => None,
    })
    .await;

    let store = Arc::new(MemoryDownloads::default());
    let mut frank_config = client_config(server_addr, "frank");
    frank_config.downloads = store.clone();
    let (frank, mut frank_events, mut frank_transfers) = Client::spawn(frank_config);
    wait(&mut frank_events, |event| match event {
        ClientEvent::LoggedIn { .. } => Some(()),
        _ => None,
    })
    .await;

    let token = frank.search("song", SearchScope::Global).await.unwrap();
    let results = wait(&mut frank_events, |event| match event {
        ClientEvent::SearchResults(result) if result.token == token => Some(result.results),
        _ => None,
    })
    .await;
    assert_eq!(results[0].name, "Memory\\Album\\song.mp3");

    let download = || {
        frank.download(
            "eve",
            "Memory\\Album\\song.mp3",
            payload.len() as u64,
            FileAttributes::default(),
            None,
        )
    };
    assert_eq!(download().await, EnqueueResult::Enqueued);
    let (snapshot, delivered) = finished(&mut frank_transfers).await;
    let destination = "memory://eve/Memory/Album/song.mp3";
    assert_eq!(snapshot.destination.as_deref(), Some(destination));
    assert_eq!(snapshot.file_path.as_deref(), Some(destination));
    assert_eq!(delivered, payload.len() as u64);
    assert_eq!(store.finished.lock().unwrap()[destination], payload);
    assert!(store.partials.lock().unwrap().is_empty());

    assert_eq!(download().await, EnqueueResult::Enqueued);
    let (snapshot, delivered) = finished(&mut frank_transfers).await;
    assert_eq!(snapshot.file_path.as_deref(), Some(destination));
    assert_eq!(delivered, 0);
}
