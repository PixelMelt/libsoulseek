# 34. A file denial carries the consumer's reason

Status: Accepted (refines ADR 0022)
Nicotine+: No counterpart

## Context

ADR 0022 made file denial a consumer policy but left its peer-facing text in the library.

## Decision

`Client::deny_file` takes and stores the consumer's reason. Queue refusals and active-transfer failures use that exact text. It must be a custom reason: a standard reason such as `Queued` tells a peer to retry instead of aborting.

## Consequences

The library enforces the scoped denial and the consumer owns its explanation.
