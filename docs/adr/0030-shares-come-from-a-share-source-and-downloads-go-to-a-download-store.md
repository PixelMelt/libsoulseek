# 30. Shares come from a ShareSource and downloads go to a DownloadStore

Status: Accepted (refines ADR 0012, ADR 0014, ADR 0020, ADR 0021 and ADR 0025)
Nicotine+: Diverges: Nicotine+ shares and downloads local files only; `LocalShares` and `LocalDownloads` keep its behaviour, and discarding a cleared download's partial matches its stale incomplete-file cleanup

## Context

The library assumed local POSIX files end to end: the client walked `std::fs` to build the share catalog, persisted it, opened real paths for uploads, chose incomplete names, collision suffixes and folder layout for downloads, and `NetworkCommand::{DownloadFile, UploadFile}` carried `std::fs::File`. A consumer could not share from object storage, an archive, a database or memory, and could not decide where or how downloads land.

## Decision

`storage` is a public module that owns the plug-in contract and the local implementations. `ClientBootstrap` takes `shares: Arc<dyn ShareSource>` and `downloads: Arc<dyn DownloadStore>`; `RuntimeConfig` no longer names folders or directories.

- `ShareSource` has `load` (what can be served now, `None` when there is nothing yet; like the cache in ADR 0013 it fails open, so an error or an invalid catalog is logged and the job still scans), `scan(progress)` (rebuild; `progress` carries counts and the cancel flag), `size(key)` and `open(key, offset)` (a reader positioned at the offset plus the file's current size). Every method returns a boxed `'static` future; a blocking source does its own `spawn_blocking`.
- A source builds a `ShareCatalog`, whose `push_folder` enforces the virtual path rules (backslash-separated non-empty components without `/`; file names without `/` or `\`; no duplicate name in a folder); the index refuses a duplicate folder path. Each folder and file carries opaque key bytes and `modified`, which only the source interprets (`LocalShares` persists the mtime there and reads it back from its cache). `ShareKey` is the (folder key, file key) pair, so keys never collide by concatenation, and its lossy text is an upload's `file_path`.
- `DownloadStore` has `destination(request)` (sync policy, persisted as `TransferSnapshot.destination`), `existing(downloads)` (one call per enqueue batch, answers in request order), `open(download, restart)` (a writer plus the offset to resume from), `finish(download)` (returns the final location, `file_path`) and `discard(download)`. The store derives a partial's identity from the `Download`, so only `destination` is persisted.
- `LocalShares` (folders, filters, cache path) and `LocalDownloads` (download dir, incomplete dir, username subfolders) are the previous behaviour moved behind the traits, including the v2 catalog cache format byte for byte and the walk reading the whole cache file as its attribute cache, so renaming a share re-reads nothing. `LocalDownloads::new` refuses a directory whose path is not UTF-8, because destinations are text.
- `Client::set_shares` replaces the source and starts a reload; `Client::set_downloads` replaces the store for later enqueues. Stores are immutable, so a configuration change builds a new one. A download keeps the store chosen at its enqueue through retries and recovery; a download seeded at boot belongs to the bootstrap store. The index remembers the source that produced it.
- Installing an index re-validates uploads by key: an upload whose path now resolves to the same key continues and adopts the new index's source, so replacing a `LocalShares` to add a folder cancels no one; any other upload is denied as ADR 0021 says.
- `discard` runs when a download that has not finished is cleared, once no file connection or placement for that download is still in flight: at once when none is, otherwise when the last of them closes or completes, including a connection an abort or failure already let go. A server drop closes connections without reporting them, so the discards waiting only on connections run then. Enqueueing the download again before that cancels its pending discards, so the new attempt resumes from the partial.
- `open` returns an offset no larger than the download's size; `LocalDownloads` restarts a partial that has grown past it.

## Consequences

A consumer shares from and downloads to any storage by implementing two traits; newkitine keeps the local behaviour by constructing the local implementations. Sources that replace one another share one key namespace: if two sources use the same key for different files, an upload resolved through the first continues from the second. A consumer that changes stores across a restart must migrate stored destinations itself. A source that ignores cancellation holds back the next scan job, as the walk always did.
