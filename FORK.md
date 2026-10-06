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

## Maintained-fork CI

The active native CI uses the same reviewed PHP 8.4.25 public image digest as
the consuming project and a pinned checkout action on Ubuntu 24.04. Test
execution has no network and mounts the candidate source read-only. It lints
the native/test PHP files and runs both standalone checks above with strict
exit propagation. It installs no Composer dependencies.

The inherited PHP 7.1–8.1/Kafka matrix, coding-standard and PHPStan workflows
were retired because their runtime/toolchain assumptions do not match this
PHP 8.4+ fork. Their checks are not represented as passing or replaced by
equivalent coverage. The maintained workflow does not exercise Swoole, real
Kafka, inherited PHPUnit, formatting or static analysis. Real-broker/runtime
acceptance remains the consuming project's separate responsibility.

## Client response and lifecycle repair

Temporary bootstrap clients close in a finally block; failed cached-client
connection setup also closes the unpublished client. Both native clients
register only requests which expect a response, reject incomplete writes and
pending correlation-ID reuse, clear registrations after every receive outcome,
and close unsafe connections after transport or parsing failure. Synchronous
split send/recv remains FIFO: a reply for a different pending ID rejects and
closes the connection rather than being attributed to the wrong request.

Response frames reuse the native 5 MiB read limit and require a complete
correlation header. Response bodies always run the existing native unpacker,
including empty bodies, and must consume exactly their frame. This avoids the
factory's empty-data convenience path manufacturing a default response.

The Swoole reader uses native Coroutine/Channel facilities. No-response sends
allocate no receive channel. Unknown or duplicate replies fail visibly without
an unbounded channel push. Closing invalidates registrations and the connection
generation before waking waiters, closes the socket, and joins a different
receiver coroutine for at most one second. A receiver does not join itself.
Reconnect refuses a still-live receiver, including reconnect attempted from its
exception callback, because the inherited socket wrapper reuses mutable native
connection state. Suspended old send/recv cleanup cannot clear a new generation's
registration or close its socket. Exception callbacks run after connection
cleanup; they must return promptly, and callback exceptions are not swallowed.
The native join API is verified against Swoole 6.2.2; older Swoole compatibility
is not claimed. Request timeout configuration remains the caller's obligation.

Additional runtime check (requires real Swoole; absence exits nonzero):
    php tests/client_lifecycle.php

This check uses actual Swoole coroutines, channels, joins and loopback TCP sockets
with scripted wire replies, plus explicitly identified socket fault injection
for partial writes and scheduling races. It covers idle-reader cleanup,
no-response sends, missing/duplicate/malformed replies, parsing errors, native
socket reconnect, old-generation cleanup, concurrent waiters, and bounded refusal
to replace a socket while its old receiver remains alive. It is not a real
Kafka or production TLS acceptance test. The PHP-only CI continues to run the
two standalone non-Swoole checks and syntax-lints this runtime check; it does not
claim to execute it.

The underlying SwooleSocket also captures the native socket for each send/recv.
After native IO yields, it checks ownership before reading errors, appending to
the current buffer or closing the current connection. A superseded native
socket is closed separately and its operation fails. Close detaches the socket
and clears its buffer before native close can resume another coroutine. Actual
native backpressure and delayed-read controls replace the wrapper connection
while an old operation is suspended and verify the replacement socket and its
buffer survive. This adds no writer pool or connection engine. SyncClient's
StreamSocket remains a synchronous adapter; no Swoole hook behavior is claimed
for that adapter.

## Failed consumer construction cleanup (2026-10-06)

Consumer construction now owns cleanup when bootstrap metadata, topic metadata,
coordinator discovery or group joining fails before an instance can be returned.
It stops consumption, clears any acquired heartbeat timer and closes all stored
Broker clients without sending LeaveGroup for a partially initialized member.
The original failure object is rethrown even when cleanup also fails.

Broker close detaches its active client collection, attempts every client close
and then reports the first cleanup failure. Failed closes remain owned in a
separate pending-cleanup collection for later explicit retry; successful closes
are removed. A newer active connection created while cleanup yields is not
overwritten by an older failed close. Reentrant/concurrent close attempts reject
while the current close owns its snapshot. Temporary bootstrap and unpublished connection
cleanup likewise preserve an already-active connection/metadata failure; a
cleanup-only failure remains visible. Both native clients preserve primary
transport/protocol errors across failing cleanup. The Swoole receiver still
notifies waiters and its configured callback with the original read error;
callback exceptions remain visible. Successful construction, ACK behavior and
the existing bounded native close/join implementation are unchanged.

The existing standalone checks add constructor stage, multiple-client, timer,
cleanup precedence and explicit-close controls. The Swoole check additionally
constructs the actual Consumer against a scripted TCP coordinator failure and
requires its owned socket/receiver to close or become collectible. These checks
prove native resource ownership and failure precedence, not real Kafka or Hyperf
process restart. Consuming applications must separately rebuild and verify that
failed construction allows their native process supervisor to restart.
