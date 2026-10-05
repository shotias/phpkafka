<?php

declare(strict_types=1);

// Standalone regression check for the shotias PHP 8.4+ fork; no Composer install.
use longlang\phpkafka\Exception\CRC32Exception;
use longlang\phpkafka\Protocol\ProtocolUtil;
use longlang\phpkafka\Protocol\RecordBatch\Record;
use longlang\phpkafka\Protocol\RecordBatch\RecordBatch;
use longlang\phpkafka\Protocol\RecordBatch\RecordHeader;

spl_autoload_register(static function (string $class): void {
    $prefix = 'longlang\\phpkafka\\';
    if (str_starts_with($class, $prefix)) {
        require dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
    }
});

$checks = 0;
$check = static function (bool $condition, string $label) use (&$checks): void {
    if (!$condition) {
        throw new RuntimeException($label);
    }
    ++$checks;
};

$check(PHP_VERSION_ID >= 80400, 'PHP 8.4+ is required');
$check(in_array('crc32c', hash_algos(), true), 'Native CRC32C must be available');
$source = dirname(__DIR__) . '/src/Protocol/ProtocolUtil.php';
$check((new ReflectionClass(ProtocolUtil::class))->getFileName() === realpath($source), 'Load the checked-out fork source');

// RFC 3720 Appendix B.4's four binary examples, expressed as numeric CRC hex
// rather than the RFC's little-endian iSCSI wire order:
// https://www.rfc-editor.org/rfc/rfc3720#appendix-B.4
$vectors = [
    ['', '00000000'],
    ['123456789', 'e3069283'],
    [str_repeat("\0", 32), '8a9136aa'],
    [str_repeat("\xff", 32), '62a8ab43'],
    [implode('', array_map(chr(...), range(0, 31))), '46dd794e'],
    [implode('', array_map(chr(...), range(31, 0))), '113fdb5c'],
];
foreach (array_merge($vectors, array_reverse($vectors)) as $index => [$input, $hex]) {
    $check(ProtocolUtil::crc32c($input) === $hex, "CRC32C hex vector $index");
    $check(ProtocolUtil::crc32c($input, true) === hex2bin($hex), "CRC32C raw vector $index");
}

// Frozen upstream v1.2.5 fixture, independently checked with Google\CRC32\PHP.
// The 4-byte record-set length prefix precedes Kafka's documented batch layout:
// https://kafka.apache.org/40/implementation/message-format/
$wireHex = '0000005d000000000000002a00000051000000030226c4d2cf'
    . '0000000000000000018bcfe568000000018bcfe56807000000000000000900020000001100000001'
    . '3e000e00086b6579000e0076616c7565ff020a74726163650e66697874757265';
$wire = hex2bin($wireHex);
$record = (new Record())->setTimestampDelta(7)->setOffsetDelta(0)
    ->setKey("key\0")->setValue("\0value\xff")
    ->setHeaders([(new RecordHeader())->setHeaderKey('trace')->setValue('fixture')]);
$batch = (new RecordBatch())->setBaseOffset(42)->setPartitionLeaderEpoch(3)
    ->setFirstTimestamp(1700000000000)->setMaxTimestamp(1700000000007)
    ->setProducerId(9)->setProducerEpoch(2)->setBaseSequence(17)->setRecords([$record]);
$check(bin2hex($batch->pack()) === $wireHex, 'RecordBatch encoding must preserve fixed upstream bytes');
$check(ProtocolUtil::crc32c(substr($wire, 25)) === '26c4d2cf', 'RecordBatch CRC covers attributes through last record');
$check(substr($wire, 21, 4) === hex2bin('26c4d2cf'), 'RecordBatch CRC has the expected wire byte order');

$decoded = new RecordBatch();
$decoded->unpack($wire, $size);
$check($size === 97 && count($decoded->getRecords()) === 1, 'RecordBatch consumes exactly the fixed frame');
$check([
    $decoded->getBaseOffset(), $decoded->getBatchLength(), $decoded->getPartitionLeaderEpoch(),
    $decoded->getMagic(), $decoded->getCrc(), $decoded->getAttributes()->getValue(),
    $decoded->getLastOffsetDelta(), $decoded->getFirstTimestamp(), $decoded->getMaxTimestamp(),
    $decoded->getProducerId(), $decoded->getProducerEpoch(), $decoded->getBaseSequence(),
] === [42, 81, 3, 2, 650433231, 0, 0, 1700000000000, 1700000000007, 9, 2, 17], 'RecordBatch fields round-trip');
$decodedRecord = $decoded->getRecords()[0];
$check([
    $decodedRecord->getLength(), $decodedRecord->getAttributes(), $decodedRecord->getTimestampDelta(),
    $decodedRecord->getOffsetDelta(), $decodedRecord->getKey(), $decodedRecord->getValue(),
] === [31, 0, 7, 0, "key\0", "\0value\xff"], 'Binary record fields round-trip');
$check(count($decodedRecord->getHeaders()) === 1
    && $decodedRecord->getHeaders()[0]->getHeaderKey() === 'trace'
    && $decodedRecord->getHeaders()[0]->getValue() === 'fixture', 'Record header round-trips');
$check($decoded->pack() === $wire, 'Decoded RecordBatch re-encodes identically');

foreach ([21, 96] as $offset) {
    $corrupted = $wire;
    $corrupted[$offset] = chr(ord($corrupted[$offset]) ^ 1);
    $rejected = false;
    try {
        (new RecordBatch())->unpack($corrupted);
    } catch (CRC32Exception $exception) {
        $rejected = true;
    }
    $check($rejected, "Corruption at byte $offset must raise CRC32Exception");
}

echo json_encode([
    'status' => 'PASS', 'checks' => $checks, 'php' => PHP_VERSION,
    'source_sha256' => hash_file('sha256', $source),
    'record_batch_wire_bytes' => strlen($wire), 'record_batch_crc_hex' => '26c4d2cf',
], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR), PHP_EOL;
