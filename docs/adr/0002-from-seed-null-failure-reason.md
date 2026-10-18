# 2. TransferPhase::from_seed maps a missing failure reason to an empty reason

Status: Accepted
Nicotine+: No counterpart

## Context

Transfers are seeded from the consumer's persisted snapshots (`ClientBootstrap.transfers`). A failed snapshot may legitimately carry no `failure_reason`: newkitine has rows written before the column existed, and failed rows whose reason was never recorded.

## Decision

`from_seed` uses `unwrap_or_default` for the reason of a failed seed.

## Consequences

Mapping a missing reason to an empty one handles real data, not a failed code path. Making it an error would crash boot on a valid store. A required reason would be the stricter form; it has not been worth a schema change for an empty string.
