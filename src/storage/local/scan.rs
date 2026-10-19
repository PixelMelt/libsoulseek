use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::UNIX_EPOCH;

use tracing::{info, warn};

use super::SharedFolder;
use super::audio::{has_audio_extension, is_missing_vbr, read_attributes};
use crate::storage::{CatalogFile, ScanProgress, ShareCatalog};
use crate::types::FileAttributes;

#[derive(Debug, thiserror::Error)]
pub(super) enum WalkError {
    #[error("cannot {action} {path}: {error}")]
    Io {
        action: &'static str,
        path: PathBuf,
        error: std::io::Error,
    },
    #[error("duplicate virtual folder name {name}")]
    DuplicateVirtualName { name: String },
    #[error("scan cancelled")]
    Cancelled,
}

fn io_error<P: Into<PathBuf>>(
    action: &'static str,
    path: impl FnOnce() -> P,
) -> impl FnOnce(std::io::Error) -> WalkError {
    move |error| WalkError::Io {
        action,
        path: path().into(),
        error,
    }
}

const BACKSLASH_SENTINEL: &str = "@@BACKSLASH@@";
const ATTRIBUTE_WORKER_CAP: usize = 12;
const PROGRESS_INTERVAL: u64 = 1000;

type AttributeCache<'a> = HashMap<&'a [u8], HashMap<&'a [u8], &'a CatalogFile>>;

pub(super) fn key_path(folder: &[u8], file: &[u8]) -> PathBuf {
    Path::new(OsStr::from_bytes(folder)).join(OsStr::from_bytes(file))
}

struct Walk<'a> {
    share_filters: HashSet<&'a str>,
    cache: AttributeCache<'a>,
    progress: Progress<'a>,
    virtual_paths: HashSet<String>,
    catalog: ShareCatalog,
    misses: Vec<(u32, PathBuf)>,
}

struct Progress<'a> {
    count: AtomicU64,
    scan: &'a ScanProgress,
}

impl Progress<'_> {
    fn add(&self) {
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(PROGRESS_INTERVAL) {
            self.scan.files(count);
        }
    }
}

pub(super) fn restrict(catalog: &ShareCatalog, shared_folders: &[SharedFolder]) -> ShareCatalog {
    let roots: Vec<(&str, PathBuf, bool)> = shared_folders
        .iter()
        .filter_map(|shared| {
            fs::canonicalize(&shared.path)
                .ok()
                .map(|root| (shared.virtual_name.as_str(), root, shared.buddy_only))
        })
        .collect();
    let mut restricted = ShareCatalog::new();
    for folder in catalog.folders() {
        let root_name = folder
            .virtual_path()
            .split_once('\\')
            .map_or(folder.virtual_path(), |(root, _)| root);
        let real_path = Path::new(OsStr::from_bytes(folder.key()));
        let Some((_, _, buddy_only)) = roots
            .iter()
            .find(|(name, root, _)| *name == root_name && real_path.starts_with(root))
        else {
            continue;
        };
        restricted
            .push_folder(
                folder.virtual_path().to_owned(),
                folder.key().to_vec(),
                *buddy_only,
                catalog.files(folder).to_vec(),
            )
            .expect("a restricted folder keeps its valid catalog entries");
    }
    restricted
}

pub(super) fn walk(
    shared_folders: &[SharedFolder],
    share_filters: &[String],
    previous: Option<&ShareCatalog>,
    progress: &ScanProgress,
) -> Result<ShareCatalog, WalkError> {
    let mut virtual_names = HashSet::new();
    for shared in shared_folders {
        if !virtual_names.insert(shared.virtual_name.as_str()) {
            return Err(WalkError::DuplicateVirtualName {
                name: shared.virtual_name.clone(),
            });
        }
    }
    let cache = previous
        .into_iter()
        .flat_map(|catalog| {
            catalog.folders().iter().map(|folder| {
                let files = catalog
                    .files(folder)
                    .iter()
                    .map(|file| (file.key(), file))
                    .collect();
                (folder.key(), files)
            })
        })
        .collect();
    let mut walk = Walk {
        share_filters: share_filters.iter().map(String::as_str).collect(),
        cache,
        progress: Progress {
            count: AtomicU64::new(0),
            scan: progress,
        },
        virtual_paths: HashSet::new(),
        catalog: ShareCatalog::new(),
        misses: Vec::new(),
    };
    for shared in shared_folders {
        walk.root(shared)?;
    }
    let Walk {
        mut catalog,
        misses,
        progress: counter,
        ..
    } = walk;

    let attribute_reads = misses.len();
    read_missing_attributes(&mut catalog, misses, &counter);
    if progress.is_cancelled() {
        return Err(WalkError::Cancelled);
    }
    info!(
        folders = catalog.folders().len(),
        files = catalog.file_count(),
        cache_hits = catalog.file_count() - attribute_reads,
        attribute_reads,
        "share scan complete"
    );
    Ok(catalog)
}

impl Walk<'_> {
    fn root(&mut self, shared: &SharedFolder) -> Result<(), WalkError> {
        let root = fs::canonicalize(&shared.path)
            .map_err(io_error("resolve shared folder", || &shared.path))?;
        let mut visited = HashSet::new();
        let mut stack = vec![(root.clone(), root, shared.virtual_name.clone())];
        while let Some((real_dir, canonical_dir, virtual_dir)) = stack.pop() {
            if self.progress.scan.is_cancelled() {
                return Err(WalkError::Cancelled);
            }
            if visited.contains(&canonical_dir) {
                warn!(path = %real_dir.display(), target = %canonical_dir.display(), "skipping folder already shared under this share");
                continue;
            }
            if !self.virtual_paths.insert(virtual_dir.clone()) {
                warn!(path = %real_dir.display(), virtual_path = %virtual_dir, "skipping folder with a duplicate virtual path");
                continue;
            }
            visited.insert(canonical_dir.clone());
            let entries = fs::read_dir(&real_dir).map_err(io_error("scan folder", || &real_dir))?;
            let cached_files = self.cache.get(real_dir.as_os_str().as_bytes());
            let mut names = HashSet::new();
            let mut files = Vec::new();
            for entry in entries {
                let entry = entry.map_err(io_error("scan folder", || &real_dir))?;
                let real_name = entry.file_name();
                let name = real_name.to_string_lossy().into_owned();
                if name.starts_with('.') || self.share_filters.contains(name.as_str()) {
                    continue;
                }
                let entry_type = entry
                    .file_type()
                    .map_err(io_error("stat", || entry.path()))?;
                let target = if entry_type.is_symlink() {
                    match fs::metadata(entry.path()) {
                        Ok(metadata) => Some(metadata),
                        Err(error) => {
                            warn!(path = %entry.path().display(), %error, "skipping unresolvable symlink in shared folder");
                            continue;
                        }
                    }
                } else {
                    None
                };
                let file_type = target.as_ref().map_or(entry_type, fs::Metadata::file_type);
                let name = name.replace('\\', BACKSLASH_SENTINEL);
                if file_type.is_dir() {
                    let canonical = if target.is_some() {
                        fs::canonicalize(entry.path()).map_err(io_error("stat", || entry.path()))?
                    } else {
                        canonical_dir.join(&real_name)
                    };
                    stack.push((entry.path(), canonical, format!("{virtual_dir}\\{name}")));
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                if !names.insert(name.clone()) {
                    warn!(path = %entry.path().display(), folder = %virtual_dir, name, "skipping file with a duplicate virtual path");
                    continue;
                }
                let metadata = match target {
                    Some(metadata) => metadata,
                    None => entry
                        .metadata()
                        .map_err(io_error("stat", || entry.path()))?,
                };
                let size = metadata.len();
                let mtime = unix_mtime(&metadata);
                let attributes = if size <= 128 || !has_audio_extension(&name) {
                    Some(FileAttributes::default())
                } else {
                    cached_files
                        .and_then(|files| files.get(real_name.as_bytes()))
                        .filter(|cached| {
                            cached.size() == size
                                && cached.modified() == mtime
                                && *cached.attributes() != FileAttributes::default()
                                && !is_missing_vbr(cached.attributes())
                        })
                        .map(|cached| cached.attributes().clone())
                };
                let known = attributes.is_some();
                if known {
                    self.progress.add();
                }
                let attributes = attributes.unwrap_or_default();
                files.push((
                    CatalogFile::new(name, real_name.into_vec(), size, mtime, attributes),
                    known,
                ));
            }
            files.sort_by(|(a, _), (b, _)| a.name().cmp(b.name()));
            let first_index = self.catalog.file_count();
            for (offset, (file, known)) in files.iter().enumerate() {
                if !known {
                    let path = key_path(real_dir.as_os_str().as_bytes(), file.key());
                    self.misses.push(((first_index + offset) as u32, path));
                }
            }
            let files = files.into_iter().map(|(file, _)| file).collect();
            self.catalog
                .push_folder(
                    virtual_dir,
                    real_dir.into_os_string().into_vec(),
                    shared.buddy_only,
                    files,
                )
                .expect("the walk produces valid catalog folders");
        }
        Ok(())
    }
}

fn read_missing_attributes(
    catalog: &mut ShareCatalog,
    misses: Vec<(u32, PathBuf)>,
    progress: &Progress,
) {
    if misses.is_empty() {
        return;
    }
    let workers = std::thread::available_parallelism()
        .map_or(4, usize::from)
        .min(ATTRIBUTE_WORKER_CAP)
        .min(misses.len());
    let cursor = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let misses = &misses;
        let cursor = &cursor;
        let (results, received) = std::sync::mpsc::sync_channel(workers * 2);
        for _ in 0..workers {
            let results = results.clone();
            scope.spawn(move || {
                while !progress.scan.is_cancelled() {
                    let position = cursor.fetch_add(1, Ordering::Relaxed);
                    let Some((file_index, path)) = misses.get(position) else {
                        break;
                    };
                    let attributes = read_attributes(path);
                    progress.add();
                    results.send((*file_index, attributes)).unwrap();
                }
            });
        }
        drop(results);
        for (file_index, attributes) in received {
            catalog.set_attributes(file_index, attributes);
        }
    });
}

fn unix_mtime(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .expect("mtime unavailable on this platform")
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::storage::CatalogFolder;
    use crate::storage::local::cache;

    fn progress(cancelled: bool) -> ScanProgress {
        ScanProgress::new(Arc::new(AtomicBool::new(cancelled)), |_| {})
    }

    fn scan(
        shared_folders: &[SharedFolder],
        share_filters: &[String],
        cache_path: &Path,
        cancelled: bool,
    ) -> Result<ShareCatalog, WalkError> {
        let previous = cache::load(cache_path);
        let catalog = walk(
            shared_folders,
            share_filters,
            Some(&previous),
            &progress(cancelled),
        )?;
        cache::save(cache_path, &catalog);
        Ok(catalog)
    }

    fn music_share(path: PathBuf) -> Vec<SharedFolder> {
        vec![SharedFolder {
            virtual_name: "Music".into(),
            path,
            buddy_only: false,
        }]
    }

    fn scan_music(path: PathBuf, cache_path: &Path) -> ShareCatalog {
        scan(&music_share(path), &[], cache_path, false).unwrap()
    }

    fn folder<'a>(catalog: &'a ShareCatalog, virtual_path: &str) -> Option<&'a CatalogFolder> {
        catalog
            .folders()
            .iter()
            .find(|folder| folder.virtual_path() == virtual_path)
    }

    fn lookup<'a>(
        catalog: &'a ShareCatalog,
        virtual_path: &str,
    ) -> Option<(PathBuf, &'a CatalogFile)> {
        let (directory, name) = virtual_path.rsplit_once('\\')?;
        let folder = folder(catalog, directory)?;
        let file = catalog
            .files(folder)
            .iter()
            .find(|file| file.name() == name)?;
        Some((key_path(folder.key(), file.key()), file))
    }

    fn counts(catalog: &ShareCatalog) -> (usize, usize) {
        (catalog.folders().len(), catalog.file_count())
    }

    fn temp_base() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let base = std::env::temp_dir().join(format!(
            "soulseek-shares-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    fn cache_path(base: &Path) -> PathBuf {
        base.join("share-catalog.gz")
    }

    fn write_wav(path: &Path) {
        let data = vec![0u8; 44100 * 2];
        let mut bytes = Vec::new();
        bytes.extend(b"RIFF");
        bytes.extend(((36 + data.len()) as u32).to_le_bytes());
        bytes.extend(b"WAVEfmt ");
        bytes.extend(16u32.to_le_bytes());
        bytes.extend(1u16.to_le_bytes());
        bytes.extend(1u16.to_le_bytes());
        bytes.extend(44100u32.to_le_bytes());
        bytes.extend(88200u32.to_le_bytes());
        bytes.extend(2u16.to_le_bytes());
        bytes.extend(16u16.to_le_bytes());
        bytes.extend(b"data");
        bytes.extend((data.len() as u32).to_le_bytes());
        bytes.extend(data);
        fs::write(path, bytes).unwrap();
    }

    fn cached_attributes(cache_path: &Path, real_dir: &Path, name: &str) -> FileAttributes {
        let catalog = cache::load(cache_path);
        let real_dir = fs::canonicalize(real_dir).unwrap();
        let folder = catalog
            .folders()
            .iter()
            .find(|folder| folder.key() == real_dir.as_os_str().as_bytes())
            .expect("folder in cache");
        catalog
            .files(folder)
            .iter()
            .find(|file| file.key() == name.as_bytes())
            .expect("file in cache")
            .attributes()
            .clone()
    }

    fn cached_song(name: &str, mtime_offset: u64, vbr: Option<u32>) -> (PathBuf, PathBuf) {
        let base = temp_base();
        let dir = base.join("music");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(name), b"g".repeat(300)).unwrap();
        let mtime = unix_mtime(&fs::metadata(dir.join(name)).unwrap()) + mtime_offset;
        let attributes = FileAttributes {
            bitrate: Some(320),
            vbr,
            ..Default::default()
        };
        let mut catalog = ShareCatalog::new();
        catalog
            .push_folder(
                "Music".into(),
                fs::canonicalize(&dir).unwrap().into_os_string().into_vec(),
                false,
                vec![CatalogFile::new(
                    name.into(),
                    name.as_bytes().to_vec(),
                    300,
                    mtime,
                    attributes,
                )],
            )
            .unwrap();
        let cache_path = cache_path(&base);
        cache::save(&cache_path, &catalog);
        (dir, cache_path)
    }

    fn test_shares(base: &Path) -> Vec<SharedFolder> {
        let album = base.join("public/Sample Album");
        fs::create_dir_all(&album).unwrap();
        fs::write(album.join("First Song.flac"), b"x".repeat(300)).unwrap();
        fs::write(album.join("Second Tune.ogg"), b"y".repeat(300)).unwrap();
        let secret = base.join("secret");
        fs::create_dir_all(&secret).unwrap();
        fs::write(secret.join("hidden song.wav"), b"z".repeat(300)).unwrap();
        vec![
            SharedFolder {
                virtual_name: "Public".into(),
                path: base.join("public"),
                buddy_only: false,
            },
            SharedFolder {
                virtual_name: "Private".into(),
                path: secret,
                buddy_only: true,
            },
        ]
    }

    #[test]
    fn shares_scan_into_virtual_folders_with_their_buddy_flags() {
        let base = temp_base();
        let catalog = scan(&test_shares(&base), &[], &cache_path(&base), false).unwrap();

        let (path, file) = lookup(&catalog, "Public\\Sample Album\\First Song.flac").unwrap();
        assert_eq!(file.size(), 300);
        assert!(path.ends_with("public/Sample Album/First Song.flac"));
        assert!(
            !folder(&catalog, "Public\\Sample Album")
                .unwrap()
                .buddy_only()
        );
        assert!(folder(&catalog, "Private").unwrap().buddy_only());
        assert!(lookup(&catalog, "Private\\hidden song.wav").is_some());
        assert_eq!(counts(&catalog), (3, 3));
    }

    #[test]
    fn case_colliding_paths_are_both_shared() {
        let base = temp_base();
        let lower = base.join("music/moe shop - pure");
        let upper = base.join("music/Moe Shop - Pure");
        fs::create_dir_all(&lower).unwrap();
        fs::create_dir_all(&upper).unwrap();
        fs::write(lower.join("Crush.mp3"), b"a".repeat(200)).unwrap();
        fs::write(upper.join("Crush.mp3"), b"b".repeat(300)).unwrap();

        let catalog = scan_music(base.join("music"), &cache_path(&base));

        assert_eq!(counts(&catalog), (3, 2));
        let (path, file) = lookup(&catalog, "Music\\Moe Shop - Pure\\Crush.mp3").unwrap();
        assert_eq!(file.size(), 300);
        assert!(path.starts_with(&upper));
        let (path, file) = lookup(&catalog, "Music\\moe shop - pure\\Crush.mp3").unwrap();
        assert_eq!(file.size(), 200);
        assert!(path.starts_with(&lower));
    }

    #[test]
    fn attributes_are_read_and_cached() {
        let base = temp_base();
        let dir = base.join("music");
        fs::create_dir_all(&dir).unwrap();
        write_wav(&dir.join("tone.wav"));
        let cache_path = cache_path(&base);

        let catalog = scan_music(dir.clone(), &cache_path);

        let attributes = lookup(&catalog, "Music\\tone.wav").unwrap().1.attributes();
        assert_eq!(attributes.sample_rate, Some(44100));
        assert_eq!(attributes.bit_depth, Some(16));
        assert_eq!(attributes.length, Some(1));
        assert_eq!(
            cached_attributes(&cache_path, &dir, "tone.wav").sample_rate,
            Some(44100)
        );
    }

    #[test]
    fn cache_hit_skips_reading_attributes() {
        let (dir, cache_path) = cached_song("song.flac", 0, Some(0));

        let catalog = scan_music(dir, &cache_path);

        let attributes = lookup(&catalog, "Music\\song.flac").unwrap().1.attributes();
        assert_eq!(attributes.bitrate, Some(320));
    }

    #[test]
    fn changed_file_invalidates_cache_entry() {
        let (dir, cache_path) = cached_song("song.flac", 1, None);

        let catalog = scan_music(dir.clone(), &cache_path);

        let attributes = lookup(&catalog, "Music\\song.flac").unwrap().1.attributes();
        assert_eq!(*attributes, FileAttributes::default());
        assert_eq!(
            cached_attributes(&cache_path, &dir, "song.flac"),
            FileAttributes::default()
        );
    }

    #[test]
    fn restrict_applies_the_share_configuration_to_a_cached_catalog() {
        let base = temp_base();
        let shares = test_shares(&base);
        let catalog = walk(&shares, &[], None, &progress(false)).unwrap();
        assert_eq!(catalog.folders().len(), 3);

        let public_only = restrict(&catalog, &shares[..1]);
        assert_eq!(counts(&public_only), (2, 2));
        assert!(
            public_only
                .folders()
                .iter()
                .all(|folder| folder.virtual_path().starts_with("Public"))
        );
        assert!(lookup(&public_only, "Public\\Sample Album\\First Song.flac").is_some());
        assert!(lookup(&public_only, "Private\\hidden song.wav").is_none());

        let flipped = restrict(
            &catalog,
            &[SharedFolder {
                virtual_name: "Public".into(),
                path: base.join("public"),
                buddy_only: true,
            }],
        );
        assert!(flipped.folders().iter().all(CatalogFolder::buddy_only));

        let renamed = restrict(
            &catalog,
            &[SharedFolder {
                virtual_name: "Elsewhere".into(),
                path: base.join("public"),
                buddy_only: false,
            }],
        );
        assert!(renamed.folders().is_empty());
    }

    #[test]
    fn progress_notifies_every_interval() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scan = ScanProgress::new(Arc::new(AtomicBool::new(false)), {
            let seen = seen.clone();
            move |count| seen.lock().unwrap().push(count)
        });
        let progress = Progress {
            count: AtomicU64::new(0),
            scan: &scan,
        };
        for _ in 0..(PROGRESS_INTERVAL * 2 + 1) {
            progress.add();
        }
        assert_eq!(
            *seen.lock().unwrap(),
            vec![PROGRESS_INTERVAL, PROGRESS_INTERVAL * 2]
        );
    }

    #[test]
    fn share_filters_skip_exact_names() {
        let base = temp_base();
        let dir = base.join("music");
        let covers = dir.join("Covers");
        fs::create_dir_all(&covers).unwrap();
        fs::write(dir.join("song.mp3"), b"g".repeat(300)).unwrap();
        fs::write(dir.join("Thumbs.db"), b"x").unwrap();
        fs::write(covers.join("front.mp3"), b"y".repeat(300)).unwrap();

        let catalog = scan(
            &music_share(dir),
            &["Thumbs.db".into(), "Covers".into()],
            &cache_path(&base),
            false,
        )
        .unwrap();

        assert_eq!(counts(&catalog), (1, 1));
        assert!(lookup(&catalog, "Music\\song.mp3").is_some());
    }

    #[test]
    fn backslash_in_directory_names_is_sanitized() {
        let base = temp_base();
        let dir = base.join("music");
        let weird = dir.join("a\\b");
        fs::create_dir_all(&weird).unwrap();
        fs::write(weird.join("song.mp3"), b"g".repeat(300)).unwrap();

        let catalog = scan_music(dir, &cache_path(&base));

        assert!(lookup(&catalog, "Music\\a@@BACKSLASH@@b\\song.mp3").is_some());
    }

    #[test]
    fn cancelled_scan_stops() {
        let base = temp_base();
        let dir = base.join("music");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("song.mp3"), b"g".repeat(300)).unwrap();

        let result = scan(&music_share(dir), &[], &cache_path(&base), true);
        assert!(matches!(result, Err(WalkError::Cancelled)));
    }

    #[test]
    fn cached_lossy_attributes_without_vbr_are_reread() {
        let (dir, cache_path) = cached_song("song.mp3", 0, None);

        let catalog = scan_music(dir, &cache_path);

        let attributes = lookup(&catalog, "Music\\song.mp3").unwrap().1.attributes();
        assert_eq!(*attributes, FileAttributes::default());
    }

    #[test]
    fn lossy_equal_folder_names_are_skipped_not_fatal() {
        let base = temp_base();
        let music = base.join("music");
        for raw in [&b"album\xff"[..], &b"album\xfe"[..]] {
            let dir = music.join(OsStr::from_bytes(raw));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("track.flac"), b"t".repeat(10)).unwrap();
        }

        let catalog = scan_music(music, &cache_path(&base));

        assert_eq!(counts(&catalog), (2, 1));
        assert!(lookup(&catalog, "Music\\album\u{fffd}\\track.flac").is_some());
    }

    #[test]
    fn lossy_equal_file_names_are_shared_once() {
        let base = temp_base();
        let music = base.join("music");
        fs::create_dir_all(&music).unwrap();
        fs::write(
            music.join(OsStr::from_bytes(b"song\xff.flac")),
            b"a".repeat(10),
        )
        .unwrap();
        fs::write(
            music.join(OsStr::from_bytes(b"song\xfe.flac")),
            b"b".repeat(20),
        )
        .unwrap();

        let catalog = scan_music(music, &cache_path(&base));

        assert_eq!(counts(&catalog), (1, 1));
    }

    #[test]
    fn symlinks_are_followed_with_loop_protection() {
        use std::os::unix::fs::symlink;
        let base = temp_base();
        let music = base.join("music");
        let outside = base.join("outside");
        fs::create_dir_all(music.join("real")).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("linked.flac"), b"l".repeat(30)).unwrap();
        fs::write(outside.join("single.flac"), b"s".repeat(40)).unwrap();
        symlink(&outside, music.join("linked dir")).unwrap();
        symlink(outside.join("single.flac"), music.join("real/single.flac")).unwrap();
        symlink(&music, music.join("real/loop")).unwrap();
        symlink(base.join("missing"), music.join("dangling")).unwrap();

        let catalog = scan_music(music.clone(), &cache_path(&base));

        let (path, file) = lookup(&catalog, "Music\\linked dir\\linked.flac").unwrap();
        assert_eq!(file.size(), 30);
        assert_eq!(
            path,
            fs::canonicalize(&music)
                .unwrap()
                .join("linked dir/linked.flac")
        );
        assert_eq!(
            lookup(&catalog, "Music\\real\\single.flac")
                .unwrap()
                .1
                .size(),
            40
        );
        assert!(folder(&catalog, "Music\\real\\loop").is_none());
        assert_eq!(counts(&catalog), (3, 3));
    }
}
