# 31. Storage work never runs on the client actor

Status: Accepted (refines ADR 0012)
Nicotine+: No counterpart

## Context

The actor called `std::fs` directly in four places: the already-downloaded check at enqueue, opening the incomplete file at file init, the size check when an upload activates, and opening the shared file at file init. Against local disk that cost microseconds; against a remote store each is a network round trip, and a folder enqueue would have blocked the actor for a thousand of them.

## Decision

Every storage call is a spawned future whose result returns to the actor as a `StorageDone` message, checked for staleness before it applies. A transfer has at most one check, size request or placement in flight; a peer's offer can start the open while a check is still out.

- Enqueue, retry and recovery admit a download in `TransferPhase::Checking(epoch)` (status Queued, active), watch its uploader and emit its update as before. Check answers apply in the order the checks were made, so the queue requests sent from them reach a peer in enqueue order, one batch per uploader (downloads left for the next login are requested by recovery instead, after it); an answer applies only to transfers still checking under its epoch. A location finishes the transfer; `None` queues it and sends the request while logged in, and otherwise leaves it for the next login; an error, a panic, a wrong number of answers or no answer within 60 seconds fails it with the local file error prefix, which I/O recovery retries. A peer's transfer request is accepted while checking and the answer is then ignored; queue denials and upload failures are ignored.
- Downloads seeded at boot as Queued are checked the same way, without emitting an update unless the file is found, so a download whose placement finished just before a crash, or that arrived by other means, finishes instead of downloading again. A boot check that fails leaves its downloads queued to be requested, with one warning, rather than failing them all at once. Only found files emit, which keeps ADR 0010's bound on boot traffic.
- Activating an upload takes the slot and token and asks the source for the size; the answer applies only while that token is still waiting without a connection, then the update is emitted, `TransferRequest` sent and the request timeout restarted, or the peer is told "File read error." and the transfer fails. A size that never arrives times out like an unanswered request.
- File transfers open inside the network file connection. `DownloadFile` carries an `OpenDownload` opener and the size; the connection opens the partial, sends its offset (or finishes at once when the partial is complete), reports the offset as progress, and shuts the writer down when it ends, reporting completion only after a successful shutdown of a complete file. A store that returns an offset past the end of the file fails the transfer. `UploadFile` carries an `OpenUpload` opener; the connection refuses an offset beyond the file, then opens at the offset, and the client's opener fails when the file's size changed. Opens, reads, writes and shutdowns are bounded by the peer idle timeout; an upload open also stops on close.
- Placement keeps the Placing phase of ADR 0012, calls the transfer's store and carries an epoch, so a placement that finishes after its transfer was cleared and enqueued again applies to nothing.
- The boot scan job starts before the startup connect, so a client that connects at startup also waits for its loaded shares (ADR 0020's deferral).

## Consequences

A server drop detaches every connection, since the network then closes them without reporting. A slow store stalls the transfers that use it, never the actor; a store that never answers a check holds back the checks after it for at most the 60 second bound. Because the connection closes only after its writer shut down, discarding a cleared download's partial waits for `FileConnectionClosed`. A download open is not cancelled by close, so a timed-out open can still leave a partial behind, and a discard that runs at a server drop can race an open still in flight; the store must tolerate resuming from such a partial.
