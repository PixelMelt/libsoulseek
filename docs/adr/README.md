# Architecture decision records

These calls were made deliberately, with the reasoning recorded. Audits keep re-flagging them from fresh context. Do not re-open an accepted one without new evidence: "a different reviewer would have chosen differently" is not new evidence. A superseded record stays in place and says why the earlier call was wrong, so the same mistake is not rediscovered.

Each record has a `Nicotine+` line stating whether the decision matches the original client, diverges from it, or has no counterpart. The divergences are the port's deliberate choices.

To add one: next number, one decision per file, sections Context / Decision / Consequences, and mark anything it supersedes in both records.

Records 0001 to 0028 began in newkitine and were renumbered here. newkitine's index maps each old number to its record here; `newkitine ADR NNNN` names a record that stayed there.

| ADR | Title | Status | Nicotine+ |
|---|---|---|---|
| [0001](0001-conncontrol-stays-one-enum.md) | ConnControl stays one enum | Accepted | No counterpart |
| [0002](0002-from-seed-null-failure-reason.md) | TransferPhase::from_seed maps a missing failure reason to an empty reason | Accepted | No counterpart |
| [0003](0003-share-config-change-invalidates-the-catalog-immediately.md) | Share config change invalidates the catalog immediately | Superseded by ADR 0020 and ADR 0021 | Diverged: Nicotine+ keeps serving the old shares during a rescan |
| [0004](0004-buddy-only-shares-are-fully-hidden-from-non-buddies.md) | Buddy-only shares are fully hidden from non-buddies | Accepted | Diverges: Nicotine+ sends a locked private section in browses and locked search results |
| [0005](0005-network-send-sync-client-methods-async.md) | NetworkHandle::send is sync try_send + panic; Client methods are async awaited | Accepted | No counterpart |
| [0006](0006-owner-internal-sibling-imports-do-not-route-through-barrels.md) | Owner-internal sibling imports do not route through barrels | Accepted | No counterpart |
| [0007](0007-event-emit-panics-on-full-ignores-closed.md) | Event emit() panics on Full, ignores Closed | Accepted | No counterpart |
| [0008](0008-reconnect-backoff.md) | Reconnect backoff: 5 s base + 0–10 s jitter, doubling to a 300 s cap | Accepted | Matches in shape: Nicotine+ picks a random 5–15 s, then doubles to 300 s |
| [0009](0009-known-deliberate-protocol-divergences-from-nicotine.md) | Known-deliberate protocol divergences from Nicotine+ | Accepted | Diverges (listed below) |
| [0010](0010-transfers-are-actor-authoritative.md) | Transfers are actor-authoritative; the consumer's store is a projection of them | Accepted | No counterpart |
| [0011](0011-resource-bounds-are-explicit-constants-not-configuration.md) | Resource bounds are explicit constants, not configuration | Accepted | No counterpart |
| [0012](0012-download-placement-on-blocking-worker.md) | Download placement runs on a blocking worker with a Placing phase | Accepted (refined by ADR 0030 and ADR 0031) | No counterpart |
| [0013](0013-the-share-catalog-cache-is-fail-open-by-design.md) | The share catalog cache is fail-open by design | Accepted | Matches: Nicotine+ rescans when its share databases fail to load |
| [0014](0014-scan-lifecycle-vocabulary.md) | Scan lifecycle vocabulary | Accepted (refined by ADR 0030) | No counterpart |
| [0015](0015-browse-responses-are-encoded-once-per-index-not-per-request.md) | Browse responses are encoded once per index, not per request | Accepted | Matches: Nicotine+ builds compressed share lists at scan time and drops repeat requests within 0.4 s |
| [0016](0016-search-answering-stays-inline-on-the-client-actor.md) | Search answering stays inline on the client actor | Accepted | No counterpart |
| [0017](0017-deferred-request-overflow-answers-too-many-files.md) | Deferred-request overflow answers "Too many files" | Superseded by ADR 0020 | No counterpart |
| [0018](0018-the-attribute-cache-is-skipped-when-the-scan-changed-nothing.md) | The attribute cache is skipped when the scan changed nothing | Superseded by ADR 0020 | No counterpart |
| [0019](0019-the-description-template-is-parsed-not-validated-then-rendered.md) | The description template is parsed, not validated then rendered | Superseded by ADR 0033 | No counterpart |
| [0020](0020-the-share-catalog-is-persisted-and-served-at-boot.md) | The share catalog is persisted and served at boot | Accepted (supersedes ADR 0003 in part, ADR 0017 and ADR 0018; refines newkitine ADR 0033; refined by ADR 0030) | Matches: Nicotine+ persists its share databases, serves them at login, and keeps serving the old list during a rescan |
| [0021](0021-installing-an-index-re-validates-active-uploads.md) | Installing an index re-validates active uploads | Accepted (supersedes ADR 0003; refined by ADR 0030) | Stricter than Nicotine+, which does not re-check uploads against a new share list |
| [0022](0022-a-file-can-be-denied-to-one-peer-for-a-time.md) | A file can be denied to one peer for a time | Accepted (refined by ADR 0034) | No counterpart |
| [0023](0023-speed-limits-are-one-shared-budget-per-direction.md) | Speed limits are one shared budget per direction | Accepted | Matches the default total limit; Nicotine+ divides it by the active transfer count instead of sharing a budget |
| [0024](0024-peer-connection-close-is-immediate-and-surfaces-unsent-messages.md) | Peer connection close is immediate and surfaces unsent messages | Accepted | Matches: Nicotine+ clears closing buffers, re-routes later sends and reports unprocessed messages |
| [0025](0025-the-share-walk-follows-symlinks-and-skips-colliding-paths.md) | The share walk follows symlinks and skips colliding paths | Accepted (refines ADR 0013 and ADR 0020; refined by ADR 0030) | Matches: Nicotine+ follows symlinks and skips already-shared decoded paths |
| [0026](0026-the-client-actor-owns-outgoing-search-state.md) | The client actor owns outgoing search state | Accepted | Matches: Nicotine+ keeps per-token search requests, one token per wish, and shows wish results on the first match |
| [0027](0027-download-recovery-resumes-recoverable-failures-only.md) | Download recovery resumes recoverable failures only | Accepted | Diverges: Nicotine+ re-enqueues every failed download when its uploader comes online |
| [0028](0028-client-commands-are-closures-run-on-the-actor.md) | Client commands are closures run on the actor | Accepted | No counterpart |
| [0029](0029-the-library-owns-its-configuration-rules.md) | The library owns its configuration rules | Accepted (superseded in part by ADR 0032) | No counterpart |
| [0030](0030-shares-come-from-a-share-source-and-downloads-go-to-a-download-store.md) | Shares come from a ShareSource and downloads go to a DownloadStore | Accepted (refines ADR 0012, ADR 0014, ADR 0020, ADR 0021 and ADR 0025) | Diverges: Nicotine+ shares and downloads local files only; the local implementations keep its behaviour |
| [0031](0031-storage-work-never-runs-on-the-client-actor.md) | Storage work never runs on the client actor | Accepted (refines ADR 0012) | No counterpart |
| [0032](0032-share-rules-live-with-the-catalog-and-local-shares.md) | Share rules live with the catalog and LocalShares | Accepted (supersedes ADR 0029 in part) | No counterpart |
| [0033](0033-the-consumer-writes-the-profile-description.md) | The consumer writes the profile description | Accepted (supersedes ADR 0019) | No counterpart |
| [0034](0034-a-file-denial-carries-the-consumers-reason.md) | A file denial carries the consumer's reason | Accepted (refines ADR 0022) | No counterpart |
