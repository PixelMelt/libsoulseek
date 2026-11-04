# 5. NetworkHandle::send is sync try_send + panic; Client methods are async awaited

Status: Accepted in part (superseded by ADR 0038 for full queues: actor-to-actor sends now hold input back instead of panicking)
Nicotine+: No counterpart

## Context

The client actor and the network actor form a command cycle; awaiting inside that cycle deadlocks. The consumer → client boundary has no cycle.

## Decision

Actor-to-actor sends are `try_send` with a panic on Full. `Client` methods await with real backpressure.

## Consequences

Queue overflow between actors is a fatal pacing bug, not a recoverable condition. The consumer gets backpressure where backpressure is safe.

The panic on a full queue was wrong: overflow is not only a pacing bug, because peers set the pace of network events, and a busy client can send more commands than the queue holds, such as one queue-position request per uploader. ADR 0038 keeps the decision that the client never awaits the network actor, and replaces the panic with an in-order outbox that holds the client's network input back until the network actor catches up.
