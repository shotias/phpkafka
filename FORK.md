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
