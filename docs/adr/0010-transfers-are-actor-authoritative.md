# 10. Transfers are actor-authoritative; the consumer's store is a projection of them

Status: Accepted
Nicotine+: No counterpart

## Context

Transfer transitions originate on the network side (peer messages), where the actor must decide immediately. Routing every transition through the consumer's store before acting, as newkitine does for its durable lists (newkitine ADR 0003), would put a round-trip on the hot path.

## Decision

The client actor owns transfer state and emits every transition as an authoritative `TransferSnapshot` on the `TransferWork` stream; removals are `TransferWork::Removed`. The consumer persists them in order and seeds the actor from that store at boot through `ClientBootstrap.transfers`. The stream is bounded and its overflow is fatal (ADR 0007).

A restart interrupts transfers, and the consumer settles each stored snapshot with `TransferSnapshot::settle_after_restart` before seeding it, persisting the ones it changes: a download that was transferring is queued again, and an upload that was queued or transferring fails as "interrupted by restart", because the peer's queue request died with the old process. Seeding an unsettled snapshot panics. The actor does not settle seeds itself, since that would emit one update per interrupted transfer at boot into the bounded stream.

## Consequences

A crash between acknowledgement and persistence loses at most the in-window transitions, which is accepted and bounded. Consumer-owned prepare/persist/commit was rejected across four audit rounds: it adds a round-trip on the hot path and a second owner for retry and queue logic. newkitine ADR 0015 records how newkitine persists the stream.
