# 22. A file can be denied to one peer for a time

Status: Accepted
Nicotine+: No counterpart

## Context

newkitine caps how often one peer may download one file (newkitine ADR 0036). Convicting the peer as a whole blocked humans whose clients had only retried a stalled transfer, so the cap has to act on one file without touching the peer's other requests or its restriction.

## Decision

`Client::deny_file(username, virtual_path, ttl)` records a denial that expires after the TTL and fails any active copy of that file for that peer, queued or transferring. Until it expires, queue requests for that file are rejected with `TransferRejectReason::REPEATED`, a custom reason Nicotine+ aborts on rather than retrying. `Client::clear_file_denials` drops every denial for a peer. A denial applies to buddies too, and writes no restriction.

## Consequences

A broken client looping on one file is stopped at that file and told why, and every other file the peer requests is unaffected. When to deny and when to forgive is the consumer's policy.
