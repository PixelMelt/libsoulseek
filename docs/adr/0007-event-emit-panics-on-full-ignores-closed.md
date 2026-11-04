# 7. Event emit() panics on Full, ignores Closed

Status: Superseded by ADR 0038
Nicotine+: No counterpart

## Context

Consumer death is the consumer's to detect: newkitine's supervision exits the process when its event loop ends. A closed channel during shutdown is not an error.

## Decision

`emit` ignores Closed and panics on Full.

## Consequences

A full event queue means the consumer cannot keep up with the client actor; losing events would silently diverge state, so the panic is the loud option.

This was the wrong call. A full event queue does not mean the consumer is broken: peers set the pace, and an ordinary burst of search answers on a 2-core host filled the queue and killed the process. ADR 0038 holds the producer's input back until the consumer catches up, so no event is lost and nothing panics.
