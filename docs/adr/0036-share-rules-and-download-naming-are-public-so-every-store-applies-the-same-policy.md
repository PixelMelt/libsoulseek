# 36. Share rules and download naming are public so every store applies the same policy

Status: Accepted (refines ADR 0030 and ADR 0032)
Nicotine+: No counterpart: Nicotine+ has local storage only; the rules themselves are unchanged

## Context

ADR 0030 made shares and downloads pluggable, but the share name and filter rules stayed inside `LocalShares::new` and the walk, and the download layout (cleaned folder components, basename byte limit, incomplete names, collision suffixes) stayed private to `LocalDownloads`. A consumer writing another source or store, such as a WebDAV share, had to copy them and keep the copies in step, or its shares and downloads would quietly differ from local ones: a filtered name shared, a partial not found again, a file landing under another name.

## Decision

`storage` exports the policy as functions over strings, with no filesystem access.

- `check_share_name` is the share name rule. `ShareFilters::new` validates filters and `ShareFilters::skips` is the walk's rule: a name starting with `.` or equal to a filter. `LocalShares::new` takes a `ShareFilters` and checks each share name with `check_share_name`; the walk calls `skips`. `escape_backslashes` is the walk's rename of a backslash in a real name to `@@BACKSLASH@@`, since a backslash would split the virtual path.
- `ScanProgress::add_file` counts scanned files and reports every 1000th, so every source reports progress at the same cadence and a clone shares the count with its source.
- `download_folder` returns the cleaned folder components for a `DownloadRequest`, `download_basename` the cleaned basename within a byte limit, `incomplete_basename` the partial's name for a `Download`, `candidate_basenames` the collision candidates in order, and `DEFAULT_BASENAME_BYTE_LIMIT` the 255-byte limit used when the real one is unknown. `LocalDownloads` composes them and keeps the `pathconf` lookup and all file work.

## Consequences

A consumer validates share names with `check_share_name` (constructing `LocalShares` still checks them) and filters by building a `ShareFilters`, and any store it writes gets names identical to `LocalDownloads`. The rules are public API: changing one is a breaking change for every store, and changing the incomplete name orphans existing partials in every store at once. Nothing makes a consumer's source or store apply them.
