# 20. The share catalog is persisted and served at boot

Status: Accepted (supersedes ADR 0003 in part, ADR 0017 and ADR 0018; refines newkitine ADR 0033; refined by ADR 0030)
Nicotine+: Matches: Nicotine+ persists its share databases, serves them at login, and keeps serving the old list during a rescan

## Context

The index was rebuilt from a disk walk at every boot and dropped on every share-config change. On the reference 850k-file share the walk takes minutes, and during that window we advertised 0/0, answered nothing, and could not resolve uploads. Five decisions existed purely to survive that window: inbound request deferral with an overflow policy (ADR 0017), deferral of our own outbound queue requests so we would not ask peers for files while advertising zero shares, the `awaiting_share_index` gate threaded through the download paths, the opt-in boot scan (newkitine ADR 0033), and wholesale upload revocation on share edits (ADR 0003). That is an epicycle cluster built around a missing cache.

## Decision

The `LocalShares` walk produces a `ShareCatalog` (folders keyed by their real paths with buddy flags; files keyed by their real names with sizes, mtimes and audio attributes). After every successful walk the catalog is saved to the cache path `LocalShares` was built with (postcard under gzip; ADR 0013 covers failure); newkitine keeps it as `share-catalog.gz` next to its config file. When the job loads (boot, and every `set_shares`) `LocalShares::load` reads that cache, restricts it to the current configuration (a folder survives when its root virtual name is configured and its real path lies under that share's canonical path; the buddy flag comes from the configuration) and the client installs it immediately. When the job scans, the walk reads the cache file as its attribute cache (real path + size + mtime) and the walk's catalog installs when it finishes. The old index keeps serving until a replacement installs. There is no request deferral anywhere: with no index (first run, or an empty cache) requests are answered "File not shared.", exactly like a fresh Nicotine+.

## Consequences

Boot serves shares in seconds instead of minutes; after the first scan we never advertise 0/0 again. The deferral subsystem (`pending_requests`, `PENDING_REQUEST_LIMIT`, `awaiting_share_index`, the `defer_requests` parameters, `pending_queue_requests`, `flush_queued_requests`, the cache save-slot task) is gone. The privacy window after removing a share is the cache-restrict install time (seconds), not the walk time. Changing `share_filters` is applied by the walk only, so the restricted install may briefly serve filtered names. A rename of a share's virtual name drops it from the restricted install and re-reads its attributes on the walk. The legacy `scan-cache.json.gz` is no longer read.
