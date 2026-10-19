# 12. Download placement runs on a blocking worker with a Placing phase

Status: Accepted (refined by ADR 0030 and ADR 0031)
Nicotine+: No counterpart

## Context

The client actor must never do unbounded filesystem work.

## Decision

On completion a transfer enters `TransferPhase::Placing` (externally still "transferring"); the transfer's `DownloadStore::finish` runs as a spawned future and the result returns as a `StorageDone` message (ADR 0031). `LocalDownloads` renames on a blocking worker and copies only on a cross-device rename failure; other errors fail the transfer loudly. Placing counts as active (blocks duplicate enqueue) but is excluded from the disconnect reset, so placement survives a server drop.

## Consequences

A slow destination filesystem stalls one transfer, not the actor.
