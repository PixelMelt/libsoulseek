# 38. A full queue between actors holds input back instead of panicking

Status: Accepted (supersedes ADR 0007, and ADR 0005 in part)
Nicotine+: Diverges: Nicotine+ hands events to its main thread through an unbounded queue; the connection count and the search result cap match it

## Context

ADR 0005 and ADR 0007 made a full queue between actors a panic, on the premise that it can only mean a pacing bug. The premise is false, because peers set the pace, not us. A survey running four searches at a time on a 2-core host drew thousands of answers in a few seconds. The client actor could not drain the 4096 network events fast enough, and `network event queue overflowed` killed the process after 160 searches. Each answer cost four events, and two of them were connection counts.

The same run showed a second fault. The server connection task awaited its event send inside the branch that reads frames, so while the network actor was slow it stopped writing too. Its 1024 queued server messages overflowed and the server connection was dropped.

## Decision

Every bounded queue from a socket to the consumer applies backpressure:

- The network actor, the client actor's event and transfer-work queues, and `NetworkHandle` send through an `Outbox`. An item that does not fit waits in the outbox, and later items queue behind it, so order is kept. Nothing is dropped and nothing panics on a full queue. A closed receiver empties the outbox, as `emit` ignored Closed before.
- While its outbox holds anything, the network actor stops reading connection events and stops accepting. The client actor stops reading network events while its event outbox, transfer outbox or `NetworkHandle` backlog holds anything. Each actor waits for room in the receiving channel, then flushes.
- Commands are never held back. The client never waits on the network actor, which ADR 0005 required to avoid the command cycle deadlocking, and newkitine never waits on the client. The network actor's sweep and ping keep running. The client actor's sweep pauses while the client is backed up, and its missed ticks are delayed rather than burst. Otherwise its retries and timeouts would keep adding to the outbox for as long as the consumer stalled.
- A connection task holds an undelivered event as pending and keeps serving its control queue and writes while it waits for room. A peer connection's idle deadline pauses while an event is pending, and delivering an event counts as activity. A pending event is still delivered before `Closed`.

The holdup travels down the chain: the consumer, then the client actor, then the network actor, then the connection tasks, which stop reading their sockets, and finally TCP, which slows the peers. Each outbox stays bounded, because once it holds anything its actor reads no more of the input that feeds it. What still reaches it is the output of the handler that filled the channel, plus that of commands, which the consumer drives, and of the network actor's sweep, which only expires work already in flight.

The volume of events also drops. The connection count is reported on the 2 s sweep when it has changed, and when the server connection closes, rather than on every open and close. Nicotine+ reports it once a second. `Client::stop_search_results` stops a search's responses at the network layer, where the response header is checked, as Nicotine+'s `remove_allowed_token` does once a search has enough results.

## Consequences

A slow consumer slows Soulseek traffic instead of crashing the process. If it stays slow for long, peers and the server may give up on us, as they would on any client that stops reading. `NetworkHandle::send` panics only when the network actor has terminated. A raw `NetworkHandle` user drains its backlog on each later send. The client actor also flushes it as soon as there is room.
