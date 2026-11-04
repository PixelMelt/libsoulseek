# 37. The network raises the open file limit and caps its sockets

Status: Accepted (refines ADR 0011)
Nicotine+: Matches the socket cap; diverges in only ever raising the soft file limit

## Context

Every peer that answers a search opens a connection to us, and nothing bounded how many sockets the network actor held open. A container starts with a soft open file limit of 1024, so a burst of search answers on top of normal upload traffic ran the process out of file descriptors. After that, sockets failed to open, and so did files: shares, downloads and the consumer's own files and connections. Nicotine+ handles this in its network thread. It raises `RLIMIT_NOFILE` to the hard limit, at most 10240, and holds at most two thirds of that, up to 2048, as sockets.

## Decision

`network::spawn` raises the soft open file limit to the hard limit, at most 10240, and never lowers either limit. The socket budget is two thirds of the resulting soft limit, at most 2048. It counts every peer, file and distributed connection, the listening socket and the server connection. When the budget is spent, the actor does what Nicotine+'s `_check_connections` does:

- It stops accepting. Incoming connections wait in the listen backlog.
- It holds outgoing connection attempts in `pending_outgoing`. It retries them as soon as a closed connection frees a socket, before an incoming connection can take that socket, and again on each sweep. A held attempt is dropped when its init has expired, failed, or got a connection some other way. A server disconnect clears the list.
- On each sweep, it closes established peer connections that are idle: no partly received frame and no unwritten send. A frame the reader drops, such as an unsolicited response, still counts as received. File and distributed connections, and connections to ourselves, are never closed this way.

Nicotine+ sets both the soft and hard limits to `min(hard, 10240)`. A library should not lower the hard limit of the process that embeds it, nor shrink a soft limit the operator raised, so only the soft limit is raised, and only when it is lower.

## Consequences

The process keeps about a third of its descriptors for files whatever the peers do. Raising the limit is a process-wide side effect of starting the network. A consumer that wants a lower limit sets it after `network::spawn`, and the budget does not follow, because it is computed once, at spawn. While at the cap, outgoing peer messages wait for a free socket before connecting, and idle peer connections close sooner than the 60 s idle timeout. A failed `getrlimit` or `setrlimit` panics, because the actor cannot size its budget without the limit.
