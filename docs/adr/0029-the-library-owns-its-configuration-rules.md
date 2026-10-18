# 29. The library owns its configuration rules

Status: Accepted
Nicotine+: No counterpart

## Context

The rules a `RuntimeConfig` must satisfy carry protocol and client knowledge: remote clients rewrite slashes in virtual paths, share filters match exact names, search answers cap at 300 (ADR 0009), zero upload slots or a zero queue file limit leave nothing to serve, the queue size limit is multiplied out to bytes and the slot count is reported as a `u32`. newkitine checked them in its settings code, so another consumer would have had to rediscover them.

## Decision

`RuntimeConfig::validate` holds these rules and returns the first violation as a message. A consumer runs it wherever it builds a `RuntimeConfig`, before persisting or applying it. `Client::spawn` and `Client::apply_config` trust their caller and do not check again. Bounds that exist only for a consumer's storage or UI, such as newkitine's name, path and message lengths, stay with that consumer.

## Consequences

The rules have one owner and one set of tests. A consumer that skips `validate` gets the behaviour the rules prevent: download requests that never match, filters that never match, searches never answered or answered past the cap, uploads that never start, a slot count that wraps, a queue that refuses everything, a queue size limit whose byte count overflows, or banned peers told nothing. Duplicate share names are not a `validate` rule: the walk rejects them as a `ShareScanFailed`, which is how newkitine has always reported them, so a stored config that boots today keeps booting.
