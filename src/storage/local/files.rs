use std::ffi::CString;
use std::fs;
use std::io::Seek;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use tracing::debug;

use crate::storage::{DEFAULT_BASENAME_BYTE_LIMIT, candidate_basenames};

pub(super) fn basename_byte_limit(dir: &Path) -> usize {
    let dir = CString::new(dir.as_os_str().as_bytes()).expect("download path without interior nul");
    let limit = unsafe { libc::pathconf(dir.as_ptr(), libc::_PC_NAME_MAX) };
    if limit <= 0 {
        return DEFAULT_BASENAME_BYTE_LIMIT;
    }
    limit as usize
}

fn candidates<'a>(dir: &'a Path, basename: &'a str) -> impl Iterator<Item = PathBuf> + 'a {
    candidate_basenames(basename).map(|candidate| dir.join(candidate))
}

pub(super) fn complete_file_path(dir: &Path, basename: &str, size: u64) -> Option<PathBuf> {
    candidates(dir, basename)
        .map_while(|candidate| Some((fs::metadata(&candidate).ok()?.len(), candidate)))
        .find_map(|(len, candidate)| (len == size).then_some(candidate))
}

pub(super) fn open_incomplete(
    incomplete_path: &Path,
    truncate: bool,
) -> std::io::Result<(fs::File, u64)> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(incomplete_path)?;
    if truncate {
        file.set_len(0)?;
    }
    let offset = file.seek(std::io::SeekFrom::End(0))?;
    Ok((file, offset))
}

pub(super) fn place_download(
    destination_dir: &Path,
    incomplete_path: &Path,
    basename: &str,
) -> std::io::Result<PathBuf> {
    fs::create_dir_all(destination_dir)?;
    let (destination, mut claimed) = claim_destination(destination_dir, basename)?;
    let installed = fs::rename(incomplete_path, &destination).or_else(|error| {
        if error.kind() != std::io::ErrorKind::CrossesDevices {
            return Err(error);
        }
        let mut source = fs::File::open(incomplete_path)?;
        std::io::copy(&mut source, &mut claimed)?;
        claimed.sync_all()
    });
    match installed {
        Ok(()) => {
            if incomplete_path.exists()
                && let Err(error) = fs::remove_file(incomplete_path)
            {
                debug!(
                    incomplete_path = %incomplete_path.display(),
                    %error,
                    "cannot remove incomplete file after copy"
                );
            }
            Ok(destination)
        }
        Err(error) => {
            let _ = fs::remove_file(&destination);
            Err(error)
        }
    }
}

fn claim_destination(
    destination_dir: &Path,
    basename: &str,
) -> std::io::Result<(PathBuf, fs::File)> {
    for candidate in candidates(destination_dir, basename) {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    unreachable!("destination candidates never run out")
}
