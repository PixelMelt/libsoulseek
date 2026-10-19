use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Write};
use std::ops::Range;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use tracing::{error, warn};

use crate::storage::{CatalogFile, CatalogFolder, ShareCatalog};
use crate::types::FileAttributes;

const FORMAT_VERSION: u32 = 2;
const DECODE_BUFFER: usize = 64 * 1024;

#[derive(Deserialize)]
struct StoredCatalog {
    folders: Vec<StoredFolder>,
    files: Vec<StoredFile>,
}

#[derive(Deserialize)]
struct StoredFolder {
    virtual_path: String,
    #[serde(rename = "virtual_path_lower")]
    _virtual_path_lower: String,
    real_path: Vec<u8>,
    files: Range<u32>,
    buddy_only: bool,
}

#[derive(Deserialize)]
struct StoredFile {
    name: String,
    #[serde(rename = "name_lower")]
    _name_lower: String,
    real_name: OsString,
    size: u64,
    mtime: u64,
    attributes: FileAttributes,
}

struct CatalogRecord<'a>(&'a ShareCatalog);

struct Records<F>(F);

impl<F, I> Serialize for Records<F>
where
    F: Fn() -> I,
    I: Iterator<Item: Serialize>,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq((self.0)())
    }
}

struct Bytes<'a>(&'a [u8]);

impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

#[derive(Serialize)]
struct FolderRecord<'a> {
    virtual_path: &'a str,
    virtual_path_lower: &'a str,
    real_path: Bytes<'a>,
    files: Range<u32>,
    buddy_only: bool,
}

#[derive(Serialize)]
struct FileRecord<'a> {
    name: &'a str,
    name_lower: &'a str,
    real_name: &'a OsStr,
    size: u64,
    mtime: u64,
    attributes: &'a FileAttributes,
}

impl<'a> From<&'a CatalogFolder> for FolderRecord<'a> {
    fn from(folder: &'a CatalogFolder) -> Self {
        Self {
            virtual_path: folder.virtual_path(),
            virtual_path_lower: folder.virtual_path_lower(),
            real_path: Bytes(folder.key()),
            files: folder.file_range(),
            buddy_only: folder.buddy_only(),
        }
    }
}

impl<'a> From<&'a CatalogFile> for FileRecord<'a> {
    fn from(file: &'a CatalogFile) -> Self {
        Self {
            name: file.name(),
            name_lower: file.name_lower(),
            real_name: OsStr::from_bytes(file.key()),
            size: file.size(),
            mtime: file.modified(),
            attributes: file.attributes(),
        }
    }
}

impl Serialize for CatalogRecord<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let catalog = self.0;
        let mut record = serializer.serialize_struct("ShareCatalog", 2)?;
        record.serialize_field(
            "folders",
            &Records(|| catalog.folders().iter().map(FolderRecord::from)),
        )?;
        record.serialize_field(
            "files",
            &Records(|| catalog.all_files().iter().map(FileRecord::from)),
        )?;
        record.end()
    }
}

pub(super) fn load(path: &Path) -> ShareCatalog {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return ShareCatalog::new(),
        Err(error) => {
            warn!(path = %path.display(), %error, "cannot open share catalog cache, starting empty");
            return ShareCatalog::new();
        }
    };
    match decode(&mut BufReader::new(GzDecoder::new(BufReader::new(file)))) {
        Ok(catalog) => catalog,
        Err(error) => {
            warn!(path = %path.display(), %error, "corrupt share catalog cache, starting empty");
            ShareCatalog::new()
        }
    }
}

pub(super) fn save(path: &Path, catalog: &ShareCatalog) {
    static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "tmp{}",
        TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let result = write(&tmp, catalog).and_then(|()| fs::rename(&tmp, path));
    if let Err(error) = result {
        error!(path = %path.display(), %error, "cannot persist share catalog cache");
        if let Err(error) = fs::remove_file(&tmp)
            && error.kind() != ErrorKind::NotFound
        {
            warn!(path = %tmp.display(), %error, "cannot remove partial share catalog cache");
        }
    }
}

fn write(path: &Path, catalog: &ShareCatalog) -> io::Result<()> {
    let mut writer = BufWriter::new(GzEncoder::new(File::create(path)?, Compression::fast()));
    encode(&mut writer, catalog)?;
    writer
        .into_inner()
        .map_err(|error| error.into_error())?
        .finish()?
        .flush()?;
    Ok(())
}

fn encode(writer: &mut impl Write, catalog: &ShareCatalog) -> io::Result<()> {
    postcard::to_io(&(FORMAT_VERSION, CatalogRecord(catalog)), writer)
        .map(drop)
        .map_err(io::Error::other)
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

fn decode(reader: &mut impl Read) -> io::Result<ShareCatalog> {
    let mut buffer = vec![0; DECODE_BUFFER];
    let (version, rest) =
        postcard::from_io::<u32, _>((reader, &mut buffer)).map_err(io::Error::other)?;
    if version != FORMAT_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("share catalog cache version {version}, expected {FORMAT_VERSION}"),
        ));
    }
    let (stored, _) = postcard::from_io::<StoredCatalog, _>(rest).map_err(io::Error::other)?;
    let mut files = stored.files.into_iter();
    let mut consumed = 0u32;
    let mut catalog = ShareCatalog::new();
    for folder in stored.folders {
        if folder.files.start != consumed || folder.files.end < consumed {
            return Err(invalid(
                "share catalog cache folders do not cover their files in order",
            ));
        }
        consumed = folder.files.end;
        let expected = folder.files.len();
        let folder_files: Vec<CatalogFile> = files
            .by_ref()
            .take(expected)
            .map(|file| {
                CatalogFile::new(
                    file.name,
                    file.real_name.into_vec(),
                    file.size,
                    file.mtime,
                    file.attributes,
                )
            })
            .collect();
        if folder_files.len() != expected {
            return Err(invalid(
                "share catalog cache folder references missing files",
            ));
        }
        catalog
            .push_folder(
                folder.virtual_path,
                folder.real_path,
                folder.buddy_only,
                folder_files,
            )
            .map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
    }
    if files.next().is_some() {
        return Err(invalid("share catalog cache has files outside any folder"));
    }
    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[derive(Serialize)]
    struct PreviousCatalog {
        folders: Vec<PreviousFolder>,
        files: Vec<PreviousFile>,
        #[serde(skip)]
        _folders_by_path: Vec<u32>,
    }

    #[derive(Serialize)]
    struct PreviousFolder {
        virtual_path: Box<str>,
        virtual_path_lower: Box<str>,
        #[serde(serialize_with = "previous_path_bytes")]
        real_path: PathBuf,
        files: Range<u32>,
        buddy_only: bool,
    }

    #[derive(Serialize)]
    struct PreviousFile {
        name: Box<str>,
        name_lower: Box<str>,
        real_name: OsString,
        size: u64,
        mtime: u64,
        attributes: FileAttributes,
    }

    fn previous_path_bytes<S: Serializer>(path: &Path, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(path.as_os_str().as_bytes())
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "soulseek-catalog-{}-{}.gz",
            std::process::id(),
            name
        ))
    }

    fn sample() -> ShareCatalog {
        let mut catalog = ShareCatalog::new();
        catalog
            .push_folder(
                "Music\\Album".into(),
                b"/music/album".to_vec(),
                true,
                vec![
                    CatalogFile::new(
                        "song.mp3".into(),
                        b"song.mp3".to_vec(),
                        300,
                        1234567890,
                        FileAttributes {
                            bitrate: Some(320),
                            length: Some(210),
                            ..Default::default()
                        },
                    ),
                    CatalogFile::new(
                        "odd\u{fffd}name.flac".into(),
                        b"odd\xffname.flac".to_vec(),
                        1,
                        2,
                        FileAttributes::default(),
                    ),
                ],
            )
            .unwrap();
        catalog
            .push_folder("Music".into(), b"/music".to_vec(), false, Vec::new())
            .unwrap();
        catalog
    }

    fn assert_same(left: &ShareCatalog, right: &ShareCatalog) {
        assert_eq!(left.folders().len(), right.folders().len());
        for (left_folder, right_folder) in left.folders().iter().zip(right.folders()) {
            assert_eq!(left_folder.virtual_path(), right_folder.virtual_path());
            assert_eq!(left_folder.key(), right_folder.key());
            assert_eq!(left_folder.buddy_only(), right_folder.buddy_only());
            let left_files = left.files(left_folder);
            let right_files = right.files(right_folder);
            assert_eq!(left_files.len(), right_files.len());
            for (left_file, right_file) in left_files.iter().zip(right_files) {
                assert_eq!(left_file.name(), right_file.name());
                assert_eq!(left_file.key(), right_file.key());
                assert_eq!(left_file.size(), right_file.size());
                assert_eq!(left_file.modified(), right_file.modified());
                assert_eq!(left_file.attributes(), right_file.attributes());
            }
        }
    }

    #[test]
    fn roundtrip() {
        let path = temp_path("roundtrip");
        let catalog = sample();
        save(&path, &catalog);
        assert_same(&load(&path), &catalog);
    }

    #[test]
    fn caches_written_before_the_storage_split_still_load() {
        let catalog = sample();
        let previous = PreviousCatalog {
            folders: catalog
                .folders()
                .iter()
                .map(|folder| PreviousFolder {
                    virtual_path: folder.virtual_path().into(),
                    virtual_path_lower: folder.virtual_path_lower().into(),
                    real_path: PathBuf::from(OsStr::from_bytes(folder.key())),
                    files: folder.file_range(),
                    buddy_only: folder.buddy_only(),
                })
                .collect(),
            files: catalog
                .all_files()
                .iter()
                .map(|file| PreviousFile {
                    name: file.name().into(),
                    name_lower: file.name_lower().into(),
                    real_name: OsStr::from_bytes(file.key()).to_owned(),
                    size: file.size(),
                    mtime: file.modified(),
                    attributes: file.attributes().clone(),
                })
                .collect(),
            _folders_by_path: Vec::new(),
        };
        let previous_bytes = postcard::to_stdvec(&(FORMAT_VERSION, previous)).unwrap();
        let mut current_bytes = Vec::new();
        encode(&mut current_bytes, &catalog).unwrap();
        assert_eq!(previous_bytes, current_bytes);
        assert_same(&decode(&mut previous_bytes.as_slice()).unwrap(), &catalog);
    }

    #[test]
    fn write_failure_is_an_error_not_a_panic() {
        struct Full;
        impl Write for Full {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(ErrorKind::StorageFull, "disk full"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        assert!(encode(&mut Full, &sample()).is_err());
    }

    #[test]
    fn missing_file_is_empty() {
        assert!(load(&temp_path("missing")).folders().is_empty());
    }

    #[test]
    fn corrupt_file_is_empty() {
        let path = temp_path("corrupt");
        fs::write(&path, b"not gzip at all").unwrap();
        assert!(load(&path).folders().is_empty());
    }
}
