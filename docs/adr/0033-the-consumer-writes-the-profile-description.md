# 33. The consumer writes the profile description

Status: Accepted (supersedes ADR 0019)
Nicotine+: No counterpart

## Context

Profile text needs consumer-owned data such as lifetime transfer statistics. Keeping templates in the library prevents that without expanding library configuration around data it does not own.

## Decision

The client accepts a `ProfileSource` and calls it synchronously on the actor with an in-memory `ProfileRequest`. No I/O may run there. An async future would add in-flight state, while an event/reply would make the actor wait on the consumer. Ban status reaches the source as the ban reason. The library still owns zeroed protocol fields for banned users. Pictures remain `None` until a consumer needs one.

## Consequences

Consumers own profile wording and variables. The library owns only the request data and protocol response behavior.
