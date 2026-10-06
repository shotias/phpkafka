# Maintained native CRC32C fork

This fork starts at upstream swoole/phpkafka v1.2.5,
f42bfa7c97b1f2c751e2a956e597b970407faad1. The Composer package remains
longlang/phpkafka, with its existing public API and autoload namespace.

Modified by shotias on 2026-10-05: composer.json requires PHP 8.4 or newer
and removes google/crc32; src/Protocol/ProtocolUtil.php retains the native
hash('crc32c', ...) path and removes the obsolete fallback and cached object.
The upstream Apache-2.0 license and attribution are retained in LICENSE.
Other dependency constraints, including inherited development tools, are unchanged.

Run: php tests/native_crc32c.php

This standalone PHP 8.4+ check requires no dependency installation. It checks
fixed hexadecimal/raw CRC32C vectors, repeated calls, a fixed binary RecordBatch
encoding, field round-trips, and CRC-specific corruption rejection. The batch
fixture was captured from the pinned upstream source and independently checked
using the prior Google package's pure PHP CRC32C implementation.

These checks cover the changed checksum boundary, not broker integration or the
upstream suite. Inherited development tools are not certified for PHP 8.4 by
this change. Maintain the narrow patch, review upstream changes before adopting
them, and pin reviewed immutable releases in consuming projects.


## Confirmed Kafka outcomes (2026-10-06)

Modified by shotias: native producer and offset commits now reject exhausted
broker errors and incomplete/duplicate/foreign partition responses. Metadata
and rejoin attempts are finite, using the existing producer/group retry budgets.
Successful nonzero-acks sends require success for every requested partition.
Acks=0 remains the explicit unconfirmed native API; production callers must use
acks=-1 and finite connect/send/receive timeouts.

Fetched messages expose additive getOffset() and assignment provenance. Manual
consumption is sequential: one outstanding message remains available until ACK
is confirmed. ACK commits that record's actual next offset, updates the local
position only after confirmation, and rejects constructed, mutated, foreign,
closed-consumer or stale-assignment messages. Repeating the most recently
confirmed object's ACK is a no-op. Existing message setters remain, but changing
source identity makes ACK invalid. Concurrent ACK attempts reject; callers must
observe completion. Rejoin discards stale buffers and resumes durable positions;
a failed callback/uncertain ACK cannot silently pass its pending record.

RecordBatch now decodes every complete batch in a record set, checks individual
lengths/CRC and exposes getBatches(); existing first-batch getters and single-
batch pack bytes remain compatible. Record body lengths and key/value/header
boundaries are validated, including distinct null and empty key/value data.
Batch and record iteration are bounded by the supplied encoded lengths. Existing
compression extension/decompression limits are not certified by this patch.

The supported transport profile is non-transactional records with non-empty
batches. Ordinary idle fetches remain valid. Empty compacted batches,
transactional/control batches and OFFSET_OUT_OF_RANGE fail visibly instead of
silently selecting retention/reset behavior. Non-empty compacted offset gaps are
handled using actual baseOffset+offsetDelta. The legacy public updateListOffsets
helper remains unchanged for API compatibility but is no longer invoked
automatically; selecting any explicit reset policy requires downstream review.
The inherited timestamp primitive is signed32-bit, despite Kafka's varlong
format; values outside that supported range reject. Nullable header values
cannot be represented by the inherited string-only RecordHeader API and reject
rather than becoming empty strings. Full transactional/compaction policy,
64-bit timestamp deltas and nullable-header API changes remain outside this fix.

Run both standalone checks:
    php tests/native_crc32c.php
    php tests/confirmed_outcomes.php

The latter executes the actual native methods with scripted protocol responses
and strict failure controls: confirmation/exhaustion, offset gaps, multi-batch
framing, malformed records, failed callback restart, ACK loss/reentrancy,
mutation/provenance rejection, rejoin races and finite retries. It does not prove
real broker behavior, TLS, production replication, application outbox/inbox
atomicity or graceful Hyperf drain. Consuming applications must run rebuilt
real-broker crash/offset controls before adoption. No inherited development
dependency was upgraded or newly installed for these standalone checks.
