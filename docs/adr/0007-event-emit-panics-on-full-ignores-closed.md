# 7. Event emit() panics on Full, ignores Closed

Status: Accepted
Nicotine+: No counterpart

## Context

Consumer death is the consumer's to detect: newkitine's supervision exits the process when its event loop ends. A closed channel during shutdown is not an error.

## Decision

`emit` ignores Closed and panics on Full.

## Consequences

A full event queue means the consumer cannot keep up with the client actor; losing events would silently diverge state, so the panic is the loud option.
