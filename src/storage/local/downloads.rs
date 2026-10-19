use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::files;
use crate::storage::{Download, DownloadRequest, DownloadStore, OpenedDownload};
use crate::types::BoxFuture;

#[derive(Debug, Clone)]
pub struct LocalDownloads {
    dirs: Arc<Dirs>,
}

#[derive(Debug)]
struct Dirs {
    download_dir: PathBuf,
    incomplete_dir: PathBuf,
    username_subfolders: bool,
}

impl LocalDownloads {
    pub fn new(
        download_dir: PathBuf,
        incomplete_dir: PathBuf,
        username_subfolders: bool,
    ) -> Result<Self, String> {
        for dir in [&download_dir, &incomplete_dir] {
            if dir.to_str().is_none() {
                return Err(format!(
                    "download directory {} is invalid: destinations are stored as text, so \
                     the path must be valid UTF-8",
                    dir.display()
                ));
            }
        }
        Ok(Self {
            dirs: Arc::new(Dirs {
                download_dir,
                incomplete_dir,
                username_subfolders,
            }),
        })
    }
}

impl Dirs {
    fn incomplete_path(&self, download: &Download) -> PathBuf {
        files::incomplete_file_path(
            &self.incomplete_dir,
            &download.username,
            &download.virtual_path,
            files::basename_byte_limit(&self.incomplete_dir),
        )
    }
}

fn basename(destination: &Path, download: &Download) -> String {
    files::download_basename(
        &download.virtual_path,
        files::basename_byte_limit(destination),
    )
}

fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> BoxFuture<io::Result<T>> {
    Box::pin(async move {
        tokio::task::spawn_blocking(work)
            .await
            .unwrap_or_else(|error| {
                Err(io::Error::other(format!("storage task panicked: {error}")))
            })
    })
}

impl DownloadStore for LocalDownloads {
    fn destination(&self, request: &DownloadRequest<'_>) -> String {
        files::folder_destination(
            &self.dirs.download_dir,
            self.dirs.username_subfolders,
            request.username,
            request.virtual_path,
            request.root,
        )
        .display()
        .to_string()
    }

    fn existing(&self, downloads: Vec<Download>) -> BoxFuture<Vec<io::Result<Option<String>>>> {
        let count = downloads.len();
        let found = blocking(move || {
            Ok(downloads
                .iter()
                .map(|download| {
                    let destination = PathBuf::from(&download.destination);
                    let basename = basename(&destination, download);
                    Ok(
                        files::complete_file_path(&destination, &basename, download.size)
                            .map(|path| path.display().to_string()),
                    )
                })
                .collect())
        });
        Box::pin(async move {
            found.await.unwrap_or_else(|error| {
                (0..count)
                    .map(|_| Err(io::Error::new(error.kind(), error.to_string())))
                    .collect()
            })
        })
    }

    fn open(&self, download: Download, restart: bool) -> BoxFuture<io::Result<OpenedDownload>> {
        let dirs = self.dirs.clone();
        blocking(move || {
            fs::create_dir_all(&dirs.incomplete_dir)?;
            let path = dirs.incomplete_path(&download);
            let oversized = fs::metadata(&path).is_ok_and(|partial| partial.len() > download.size);
            let (file, offset) = files::open_incomplete(&path, restart || oversized)?;
            Ok(OpenedDownload {
                writer: Box::new(tokio::fs::File::from_std(file)),
                offset,
            })
        })
    }

    fn finish(&self, download: Download) -> BoxFuture<io::Result<String>> {
        let dirs = self.dirs.clone();
        blocking(move || {
            let destination = PathBuf::from(&download.destination);
            let basename = basename(&destination, &download);
            files::place_download(&destination, &dirs.incomplete_path(&download), &basename)
                .map(|path| path.display().to_string())
        })
    }

    fn discard(&self, download: Download) -> BoxFuture<io::Result<()>> {
        let dirs = self.dirs.clone();
        blocking(
            move || match fs::remove_file(dirs.incomplete_path(&download)) {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                result => result,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    async fn write(
        store: &LocalDownloads,
        download: &Download,
        restart: bool,
        bytes: &[u8],
    ) -> u64 {
        let mut opened = store.open(download.clone(), restart).await.unwrap();
        opened.writer.write_all(bytes).await.unwrap();
        opened.writer.shutdown().await.unwrap();
        opened.offset
    }

    #[tokio::test]
    async fn the_local_store_resumes_places_and_discards_partials() {
        let base =
            std::env::temp_dir().join(format!("soulseek-local-downloads-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let store =
            LocalDownloads::new(base.join("complete"), base.join("incomplete"), true).unwrap();
        let destination = store.destination(&DownloadRequest {
            username: "peer",
            virtual_path: "share\\Album\\song.mp3",
            root: Some("share\\Album"),
        });
        assert_eq!(
            PathBuf::from(&destination),
            base.join("complete/peer/Album")
        );
        let download = Download {
            username: "peer".into(),
            virtual_path: "share\\Album\\song.mp3".into(),
            size: 6,
            destination,
        };

        assert_eq!(
            store.existing(vec![download.clone()]).await[0]
                .as_ref()
                .unwrap(),
            &None
        );
        assert_eq!(write(&store, &download, false, b"abc").await, 0);
        assert_eq!(write(&store, &download, false, b"").await, 3);
        assert_eq!(write(&store, &download, true, b"abcdefgh").await, 0);
        assert_eq!(write(&store, &download, false, b"abcdef").await, 0);

        let location = store.finish(download.clone()).await.unwrap();
        assert_eq!(
            PathBuf::from(&location),
            base.join("complete/peer/Album/song.mp3")
        );
        assert_eq!(std::fs::read(&location).unwrap(), b"abcdef");
        assert_eq!(
            store.existing(vec![download.clone()]).await[0]
                .as_ref()
                .unwrap(),
            &Some(location)
        );

        assert_eq!(write(&store, &download, false, b"xyz").await, 0);
        store.discard(download.clone()).await.unwrap();
        store.discard(download).await.unwrap();
        assert_eq!(
            std::fs::read_dir(base.join("incomplete")).unwrap().count(),
            0
        );
    }

    #[test]
    fn directories_must_be_text() {
        use std::os::unix::ffi::OsStrExt;
        let odd = PathBuf::from(std::ffi::OsStr::from_bytes(b"/data/\xff"));
        assert!(
            LocalDownloads::new(odd.clone(), PathBuf::from("/data/incomplete"), false).is_err()
        );
        assert!(LocalDownloads::new(PathBuf::from("/data"), odd, false).is_err());
        assert!(LocalDownloads::new("/data".into(), "/data/incomplete".into(), false).is_ok());
    }
}
