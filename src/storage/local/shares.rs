use std::io::{self, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::AsyncSeekExt;

use super::cache;
use super::scan::{self, WalkError, key_path};
use super::{ScanMode, SharedFolder};
use crate::storage::{
    OpenedShare, ScanError, ScanProgress, ShareCatalog, ShareFilters, ShareKey, ShareSource,
    check_share_name,
};
use crate::types::BoxFuture;

#[derive(Debug, Clone)]
pub struct LocalShares {
    config: Arc<Config>,
}

#[derive(Debug)]
struct Config {
    folders: Vec<SharedFolder>,
    filters: ShareFilters,
    cache: PathBuf,
    mode: ScanMode,
}

impl LocalShares {
    pub fn new(
        folders: Vec<SharedFolder>,
        filters: ShareFilters,
        cache: PathBuf,
        mode: ScanMode,
    ) -> Result<Self, String> {
        for share in &folders {
            check_share_name(&share.virtual_name)?;
        }
        Ok(Self {
            config: Arc::new(Config {
                folders,
                filters,
                cache,
                mode,
            }),
        })
    }
}

fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ScanError> + Send + 'static,
) -> BoxFuture<Result<T, ScanError>> {
    Box::pin(async move {
        tokio::task::spawn_blocking(work)
            .await
            .unwrap_or_else(|error| {
                Err(ScanError::Panicked {
                    reason: error.to_string(),
                })
            })
    })
}

impl ShareSource for LocalShares {
    fn load(&self) -> BoxFuture<Result<Option<ShareCatalog>, ScanError>> {
        let config = self.config.clone();
        blocking(move || {
            if config.folders.is_empty() {
                return Ok(None);
            }
            let cached = cache::load(&config.cache);
            if cached.folders().is_empty() {
                return Ok(None);
            }
            Ok(Some(scan::restrict(&cached, &config.folders)))
        })
    }

    fn scan(&self, progress: ScanProgress) -> BoxFuture<Result<ShareCatalog, ScanError>> {
        let config = self.config.clone();
        blocking(move || {
            if config.folders.is_empty() {
                return Ok(ShareCatalog::new());
            }
            let cached = cache::load(&config.cache);
            let catalog = scan::walk(
                &config.folders,
                &config.filters,
                config.mode,
                &cached,
                &progress,
            )
            .map_err(|error| match error {
                WalkError::Cancelled => ScanError::Superseded,
                error => ScanError::Source(Box::new(error)),
            })?;
            drop(cached);
            cache::save(&config.cache, &catalog);
            Ok(catalog)
        })
    }

    fn size(&self, key: ShareKey) -> BoxFuture<io::Result<u64>> {
        Box::pin(async move {
            let metadata = tokio::fs::metadata(key_path(key.folder(), key.file())).await?;
            Ok(metadata.len())
        })
    }

    fn open(&self, key: ShareKey, offset: u64) -> BoxFuture<io::Result<OpenedShare>> {
        Box::pin(async move {
            let mut file = tokio::fs::File::open(key_path(key.folder(), key.file())).await?;
            let size = file.metadata().await?.len();
            file.seek(SeekFrom::Start(offset)).await?;
            Ok(OpenedShare {
                reader: Box::new(file),
                size,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use tokio::io::AsyncReadExt;

    use super::*;

    fn no_filters() -> ShareFilters {
        ShareFilters::new(Vec::new()).unwrap()
    }

    fn share(virtual_name: &str, path: PathBuf) -> SharedFolder {
        SharedFolder {
            virtual_name: virtual_name.into(),
            path,
            buddy_only: false,
        }
    }

    #[tokio::test]
    async fn a_local_source_scans_persists_and_reads_its_files() {
        let base =
            std::env::temp_dir().join(format!("soulseek-local-shares-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("music")).unwrap();
        std::fs::write(base.join("music/song.mp3"), b"0123456789").unwrap();
        let source = LocalShares::new(
            vec![share("Music", base.join("music"))],
            no_filters(),
            base.join("catalog.gz"),
            ScanMode::Full,
        )
        .unwrap();

        assert!(source.load().await.unwrap().is_none());
        let progress = ScanProgress::new(Arc::new(AtomicBool::new(false)), |_| {});
        let scanned = source.scan(progress).await.unwrap();
        let loaded = source
            .load()
            .await
            .unwrap()
            .expect("the scan was persisted");
        for catalog in [&scanned, &loaded] {
            let folder = &catalog.folders()[0];
            assert_eq!(folder.virtual_path(), "Music");
            assert_eq!(catalog.files(folder)[0].name(), "song.mp3");
        }

        let folder = &scanned.folders()[0];
        let key = ShareKey::new(folder.key(), scanned.files(folder)[0].key());
        assert_eq!(source.size(key.clone()).await.unwrap(), 10);
        let mut opened = source.open(key, 4).await.unwrap();
        assert_eq!(opened.size, 10);
        let mut rest = Vec::new();
        opened.reader.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, b"456789");
    }

    #[tokio::test]
    async fn renaming_a_share_keeps_its_attribute_cache() {
        let base =
            std::env::temp_dir().join(format!("soulseek-local-rename-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("music")).unwrap();
        let song = base.join("music/tone.flac");
        std::fs::write(&song, vec![0u8; 300]).unwrap();
        let catalog = base.join("catalog.gz");
        let progress = || ScanProgress::new(Arc::new(AtomicBool::new(false)), |_| {});
        let scan_as = |name: &str| {
            LocalShares::new(
                vec![share(name, base.join("music"))],
                no_filters(),
                catalog.clone(),
                ScanMode::Full,
            )
            .unwrap()
        };

        let first = scan_as("Music").scan(progress()).await.unwrap();
        let folder = &first.folders()[0];
        let mut cached = ShareCatalog::new();
        cached
            .push_folder(
                "Music".into(),
                folder.key().to_vec(),
                false,
                vec![crate::storage::CatalogFile::new(
                    "tone.flac".into(),
                    b"tone.flac".to_vec(),
                    300,
                    first.files(folder)[0].modified(),
                    crate::types::FileAttributes {
                        bitrate: Some(1411),
                        bit_depth: Some(16),
                        ..Default::default()
                    },
                )],
            )
            .unwrap();
        cache::save(&catalog, &cached);

        let renamed = scan_as("Tunes").scan(progress()).await.unwrap();
        let folder = &renamed.folders()[0];
        assert_eq!(folder.virtual_path(), "Tunes");
        assert_eq!(renamed.files(folder)[0].attributes().bit_depth, Some(16));
    }
}
