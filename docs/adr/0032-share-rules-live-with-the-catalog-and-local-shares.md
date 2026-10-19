# 32. Share rules live with the catalog and LocalShares

Status: Accepted (supersedes ADR 0029 in part)
Nicotine+: No counterpart

## Context

ADR 0029 put the share name and filter rules in `RuntimeConfig::validate`. Shares left `RuntimeConfig` with ADR 0030, and the virtual path rule applies to every source's catalog, not only to local folders.

## Decision

`ShareCatalog::push_folder` enforces the virtual path rules for every source. `LocalShares::new` rejects a share name that is empty or contains a slash or backslash and a filter that is empty or contains a path separator, so a consumer validates them by constructing it. `RuntimeConfig::validate` keeps the transfer and search rules. Duplicate share names stay a walk error reported as `ShareScanFailed`.

## Consequences

A consumer runs `RuntimeConfig::validate` and `LocalShares::new` wherever it builds its configuration, before persisting it. A consumer source that builds an invalid catalog fails its scan instead of serving paths remote clients cannot request.
