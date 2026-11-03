# 35. A fast scan lists shares without reading their files

Status: Accepted (refines ADR 0030)
Nicotine+: Diverges: Nicotine+ always reads the audio attributes of new and changed files; `ScanMode::Full` keeps that behaviour

## Context

The walk opens and probes every new or changed audio file larger than 128 bytes to read its attributes. On slow or remote storage, or on a small CPU, that dominates the scan, while listing the files only needs the directory walk.

## Decision

`LocalShares::new` takes a `ScanMode`. `Full` is the walk as before. `Fast` walks the same folders with the same filters and keeps a cached entry's attributes when it passes the existing check (same size and mtime, not all unknown, a lossy file's VBR flag known), but never opens a file: every other file is listed with `FileAttributes::default()`. Progress counts every file and the catalog is saved as after a full scan.

## Consequences

The cache check already treats all-unknown attributes as missing, so the next `Full` scan reads exactly the files a fast scan left unread, and a fast scan after a full one keeps what was read. A cached entry that fails the check, such as a changed file, has no attributes until a full scan. Until then searches and browses serve those files without bitrate, length or sample rate. The scan-complete log reports no attribute reads after a fast scan and counts the unread files as cache hits.
