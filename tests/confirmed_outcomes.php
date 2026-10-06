<?php
declare(strict_types=1);

// Standalone native dependency regressions. Scripted responses are not broker acceptance.
use longlang\phpkafka\Broker;
use longlang\phpkafka\Client\ClientInterface;
use longlang\phpkafka\Client\SyncClient;
use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use longlang\phpkafka\Consumer\OffsetManager;
use longlang\phpkafka\Group\GroupManager;
use longlang\phpkafka\Group\Struct\ConsumerGroupMemberAssignment;
use longlang\phpkafka\Group\Struct\ConsumerGroupTopic;
use longlang\phpkafka\Producer\Producer;
use longlang\phpkafka\Producer\ProducerConfig;
use longlang\phpkafka\Producer\ProduceMessage;
use longlang\phpkafka\Protocol as P;
use longlang\phpkafka\Protocol\AbstractRequest;
use longlang\phpkafka\Protocol\AbstractResponse;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\RecordBatch\Record;
use longlang\phpkafka\Protocol\RecordBatch\RecordBatch;
use longlang\phpkafka\Protocol\Type\Int32;
use longlang\phpkafka\Protocol\Type\VarInt;
use longlang\phpkafka\Timer\NoopTimer;

spl_autoload_register(static function (string $class): void {
    $prefix = 'longlang\\phpkafka\\';
    if (str_starts_with($class, $prefix)) {
        require dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
    }
});
set_error_handler(static function (int $level, string $message): never {
    throw new ErrorException($message, 0, $level);
});
$checks = 0;
function check(bool $condition, string $label): void {
    global $checks;
    if (!$condition) { throw new RuntimeException($label); }
    ++$checks;
}
function rejection(callable $call, string $class, string $label, ?int $code = null): Throwable {
    try { $call(); } catch (Throwable $e) {
        check($e instanceof $class && ($code === null || $e->getCode() === $code), $label . ': unexpected exception ' . $e::class);
        return $e;
    }
    throw new RuntimeException($label . ': unexpectedly succeeded');
}
function property(object $object, string $name, mixed $value): void {
    (new ReflectionProperty($object, $name))->setValue($object, $value);
}
final class ResponseClient extends SyncClient {
    public array $responses = [];
    public array $requests = [];
    public function send(AbstractRequest $request, ?P\RequestHeader\RequestHeader $header = null, bool $hasResponse = true): int {
        $this->requests[] = clone $request;
        return count($this->requests);
    }
    public function recv(?int $correlationId, ?P\ResponseHeader\ResponseHeader &$header = null): AbstractResponse {
        if (!$this->responses) { throw new LogicException('Response fixture exhausted'); }
        $result = array_shift($this->responses);
        if ($result instanceof Closure) { $result = $result($this->requests[$correlationId - 1]); }
        if ($result instanceof Throwable) { throw $result; }
        return $result;
    }
}
final class ResponseBroker extends Broker {
    public function __construct(private ResponseClient $responseClient) {}
    public function getClient(?int $brokerId = null): ClientInterface { return $this->responseClient; }
    public function getClientByBrokerId(int $brokerId): ClientInterface { return $this->responseClient; }
    public function getBrokerIdByTopic(string $topic, int $partition): ?int { return 1; }
    public function getTopicsMeta($topics = null): array {
        return [(new P\Metadata\MetadataResponseTopic())->setName('probe')->setPartitions([
            (new P\Metadata\MetadataResponsePartition())->setPartitionIndex(0)->setLeaderId(1),
            (new P\Metadata\MetadataResponsePartition())->setPartitionIndex(1)->setLeaderId(1),
        ])];
    }
}
function produceResponse(array $codes): P\Produce\ProduceResponse {
    $parts = [];
    foreach ($codes as $index => $code) {
        $parts[] = (new P\Produce\PartitionProduceResponse())->setPartitionIndex($index)->setErrorCode($code);
    }
    return (new P\Produce\ProduceResponse())->setResponses([
        (new P\Produce\TopicProduceResponse())->setName('probe')->setPartitions($parts),
    ]);
}
function offsetResponse(int $code = 0, int $index = 0): P\OffsetCommit\OffsetCommitResponse {
    return (new P\OffsetCommit\OffsetCommitResponse())->setTopics([
        (new P\OffsetCommit\OffsetCommitResponseTopic())->setName('probe')->setPartitions([
            (new P\OffsetCommit\OffsetCommitResponsePartition())->setPartitionIndex($index)->setErrorCode($code),
        ]),
    ]);
}
function producer(int $retries = 2): array {
    $config = (new ProducerConfig())->setUpdateBrokers(false)->setBrokers([1 => 'unused:9092'])
        ->setAcks(-1)->setProduceRetry($retries)->setProduceRetrySleep(0);
    $client = new ResponseClient('unused', 9092);
    $producer = new Producer($config);
    property($producer, 'broker', new ResponseBroker($client));
    return [$producer, $client];
}
function batch(int $base, array $deltas): RecordBatch {
    $records = [];
    foreach ($deltas as $delta) {
        $records[] = (new Record())->setOffsetDelta($delta)->setKey('aggregate')->setValue('record-' . ($base + $delta));
    }
    return (new RecordBatch())->setBaseOffset($base)->setLastOffsetDelta($deltas ? max($deltas) : 0)->setRecords($records);
}
function combined(RecordBatch ...$batches): string {
    $bytes = '';
    foreach ($batches as $batch) { $bytes .= substr($batch->pack(), 4); }
    return Int32::pack(strlen($bytes)) . $bytes;
}
function decoded(string $wire): RecordBatch {
    $batch = new RecordBatch();
    $batch->unpack($wire, $size);
    check($size === strlen($wire), 'Record set consumes declared bytes');
    return $batch;
}
function fetchResponse(RecordBatch $records, int $partition = 0, int $error = 0): P\Fetch\FetchResponse {
    return (new P\Fetch\FetchResponse())->setTopics([
        (new P\Fetch\FetchableTopicResponse())->setName('probe')->setPartitions([
            (new P\Fetch\FetchablePartitionResponse())->setPartitionIndex($partition)->setErrorCode($error)->setRecords($records),
        ]),
    ]);
}
function consumer(int $position = 0, int $retry = 2, bool $autoCommit = false): array {
    $config = (new ConsumerConfig())->setAutoCommit($autoCommit)->setOffsetRetry($retry)
        ->setTopic('probe')->setGroupId('group')->setGroupRetry(2)->setGroupRetrySleep(0)->setGroupHeartbeat(1000000000);
    $client = new ResponseClient('unused', 9092);
    $broker = new ResponseBroker($client);
    $manager = new OffsetManager($broker, 1, 'probe', [0, 1], 'group', '', 'member', 1);
    property($manager, 'offsets', [0 => $position, 1 => 0]);
    property($manager, 'metadatas', [0 => '', 1 => '']);
    $consumer = (new ReflectionClass(Consumer::class))->newInstanceWithoutConstructor();
    property($consumer, 'config', $config);
    property($consumer, 'broker', $broker);
    property($consumer, 'offsetManagers', ['probe' => $manager]);
    property($consumer, 'fetchOptions', [1 => ['probe' => [0]]]);
    property($consumer, 'lastHeartbeatTime', microtime(true));
    property($consumer, 'timer', new NoopTimer());
    property($consumer, 'memberId', 'member');
    property($consumer, 'generationId', 1);
    $coordinator = (new P\FindCoordinator\FindCoordinatorResponse())->setNodeId(1);
    property($consumer, 'coordinator', $coordinator);
    $group = new GroupManager($broker);
    property($group, 'findCoordinatorResponse', $coordinator);
    property($consumer, 'groupManager', $group);
    return [$consumer, $manager, $client];
}
function rejoinResponses(ResponseClient $client, int $position): void {
    $client->responses[] = (new P\JoinGroup\JoinGroupResponse())->setMemberId('member')->setLeader('other')->setGenerationId(2);
    $assignment = (new ConsumerGroupMemberAssignment())->setTopics([
        (new ConsumerGroupTopic())->setTopicName('probe')->setPartitions([0]),
    ]);
    $client->responses[] = (new P\SyncGroup\SyncGroupResponse())->setAssignment($assignment->pack());
    $client->responses[] = (new P\OffsetFetch\OffsetFetchResponse())->setTopics([
        (new P\OffsetFetch\OffsetFetchResponseTopic())->setName('probe')->setPartitions([
            (new P\OffsetFetch\OffsetFetchResponsePartition())->setPartitionIndex(0)->setCommittedOffset($position),
            (new P\OffsetFetch\OffsetFetchResponsePartition())->setPartitionIndex(1)->setCommittedOffset(0),
        ]),
    ]);
}
$kafkaError = \longlang\phpkafka\Exception\KafkaErrorException::class;
check(PHP_VERSION_ID >= 80400, 'PHP 8.4+');
check((new ReflectionClass(Producer::class))->getFileName() === realpath(dirname(__DIR__) . '/src/Producer/Producer.php'), 'Load baked fork source');

foreach ([ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, ErrorCode::LEADER_NOT_AVAILABLE] as $code) {
    foreach ([0, 2] as $retry) {
        [$producer, $client] = producer($retry);
        $client->responses = array_fill(0, $retry + 1, produceResponse([0 => $code]));
        rejection(fn () => $producer->send('probe', 'value', 'key', [], 0), $kafkaError, 'Producer exhaustion throws', $code);
        check(count($client->requests) === $retry + 1, 'Producer bounded attempts');
    }
}
[$producer, $client] = producer();
$client->responses = [produceResponse([0 => 0, 1 => 5]), produceResponse([1 => 0])];
$producer->sendBatch([new ProduceMessage('probe', 'a', 'key', [], 0), new ProduceMessage('probe', 'b', 'key', [], 1)]);
check(count($client->requests) === 2 && count($client->requests[1]->getTopics()[0]->getPartitions()) === 1, 'Retry only failed partition');
check($client->requests[1]->getTopics()[0]->getPartitions()[0]->getPartitionIndex() === 1, 'Correct retried partition');
foreach ([
    produceResponse([]),
    produceResponse([1 => 0]),
    (new P\Produce\ProduceResponse())->setResponses([produceResponse([0 => 0])->getResponses()[0], produceResponse([0 => 0])->getResponses()[0]]),
] as $response) {
    [$producer, $client] = producer();
    $client->responses = [$response];
    rejection(fn () => $producer->send('probe', 'value', 'key', [], 0), UnexpectedValueException::class, 'Missing/foreign/duplicate producer response');
}
[$producer, $client] = producer();
$client->responses = [produceResponse([0 => 29])];
rejection(fn () => $producer->send('probe', 'value', 'key', [], 0), $kafkaError, 'Fatal producer response', 29);

$wire = combined(batch(40, [0, 2]), batch(100, [3, 7]));
$records = decoded($wire);
check(count($records->getBatches()) === 2, 'Two native batches decoded');
check($records->getBaseOffset() === 40 && $records->getBatches()[1]->getBaseOffset() === 100, 'Original batch bases retained');
check($records->getBatches()[1]->getRecords()[1]->getOffsetDelta() === 7, 'Original delta retained');
check($records->pack() === $wire, 'Multi-batch round trip');
foreach ([
    substr($wire, 0, -1),
    Int32::pack(strlen($wire)) . substr($wire, 4),
    Int32::pack(strlen($wire) - 2) . substr($wire, 4) . "\0\0",
    substr_replace($wire, Int32::pack(48), 12, 4),
    substr_replace($wire, Int32::pack(PHP_INT_MAX & 0x7fffffff), 12, 4),
] as $bad) {
    rejection(fn () => (new RecordBatch())->unpack($bad), UnexpectedValueException::class, 'Malformed outer/batch framing rejected');
}
$bad = $wire;
$bad[strlen($bad) - 1] = chr(ord($bad[strlen($bad) - 1]) ^ 1);
rejection(fn () => (new RecordBatch())->unpack($bad), \longlang\phpkafka\Exception\CRC32Exception::class, 'Second batch CRC checked');
function malformedRecord(string $body): void {
    $bytes = VarInt::pack(strlen($body)) . $body;
    rejection(fn () => (new Record())->unpack($bytes), UnexpectedValueException::class, 'Malformed inner record');
}
malformedRecord("\0\0\0" . VarInt::pack(50) . 'x' . VarInt::pack(-1) . "\0");
malformedRecord("\0\0\0" . VarInt::pack(-1) . VarInt::pack(50) . "x\0");
malformedRecord("\0\0\0" . VarInt::pack(-1) . VarInt::pack(-1) . "\2" . VarInt::pack(50) . "x\0");
malformedRecord("\0\0\0" . VarInt::pack(-1) . VarInt::pack(-1) . "\0extra");
malformedRecord("\0" . "\x80\x80\x80\x80\x10" . "\0\1\1\0");
$empty = (new Record())->setKey('')->setValue('');
$roundTrip = new Record();
$roundTrip->unpack($empty->pack());
check($roundTrip->getKey() === '' && $roundTrip->getValue() === '', 'Empty fields preserved separately from null');

[$consumer, $manager, $client] = consumer(41);
$client->responses = [fetchResponse($records), offsetResponse(), offsetResponse(), offsetResponse()];
$message = $consumer->consume();
check($message !== null && $message->getOffset() === 42, 'Already committed earlier batch record filtered');
check($consumer->consume() === $message, 'Outstanding message cannot be passed');
$consumer->ack($message);
check($manager->getFetchOffset(0) === 43, 'ACK commits actual gap offset plus one');
$count = count($client->requests);
$consumer->ack($message);
check(count($client->requests) === $count, 'Duplicate ACK uses actual confirmed object without IO');
$forged = new ConsumeMessage($consumer, 'probe', 0, null, null, [], 42, 0);
rejection(fn () => $consumer->ack($forged), LogicException::class, 'Forged earlier ACK rejected');
$next = $consumer->consume();
check($next !== null && $next->getOffset() === 103, 'Next batch absolute offset');
$consumer->ack($next);
$next = $consumer->consume();
check($next !== null && $next->getOffset() === 107, 'Compacted gap retained');
$consumer->ack($next);
check($manager->getFetchOffset(0) === 108, 'Last actual next offset confirmed');

foreach ([ErrorCode::REQUEST_TIMED_OUT, ErrorCode::TOPIC_AUTHORIZATION_FAILED] as $code) {
    [$consumer, $manager, $client] = consumer(40);
    $client->responses[] = fetchResponse(decoded(combined(batch(42, [0]))));
    $message = $consumer->consume();
    $client->responses = array_fill(0, $code === ErrorCode::REQUEST_TIMED_OUT ? 3 : 1, offsetResponse($code));
    rejection(fn () => $consumer->ack($message), $kafkaError, 'ACK failure visible', $code);
    check($manager->getFetchOffset(0) === 40 && $consumer->consume() === $message, 'Unconfirmed ACK retains position and message');
    $client->responses[] = offsetResponse();
    $consumer->ack($message);
    check($manager->getFetchOffset(0) === 43, 'Retry same actual position after ACK failure');
}
[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(decoded(combined(batch(0, [0, 1])))), new RuntimeException('lost ACK')];
$message = $consumer->consume();
rejection(fn () => $consumer->ack($message), RuntimeException::class, 'Lost ACK visible');
check($manager->getFetchOffset(0) === 0 && $consumer->consume() === $message, 'Lost ACK cannot pass first message');
$client->responses[] = offsetResponse();
$consumer->ack($message);
check($client->requests[1]->getTopics()[0]->getPartitions()[0]->getCommittedOffset() === 1
    && $client->requests[2]->getTopics()[0]->getPartitions()[0]->getCommittedOffset() === 1, 'Lost ACK retries exact same next offset');

[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(decoded(combined(batch(0, [0, 2])))), offsetResponse(), offsetResponse()];
$visits = [];
property($consumer, 'consumeCallback', static function (ConsumeMessage $message) use ($consumer, &$visits): void {
    $visits[] = $message->getOffset();
    if (count($visits) === 1) { throw new RuntimeException('first effect failed'); }
    $consumer->ack($message);
    if ($message->getOffset() === 2) { $consumer->stop(); }
});
rejection(fn () => $consumer->start(), RuntimeException::class, 'Callback failure');
$consumer->start();
check($visits === [0, 0, 2] && $manager->getFetchOffset(0) === 3, 'Restart redelivers failed first record before next buffered record');

[$consumer, $manager, $client] = consumer(0, 0, true);
$client->responses = [fetchResponse(decoded(combined(batch(0, [0])))), offsetResponse()];
property($consumer, 'consumeCallback', static function (ConsumeMessage $message) use ($consumer): void { $consumer->stop(); });
$consumer->start();
check($manager->getFetchOffset(0) === 1, 'Existing autoCommit path remains positive');

foreach (['topic', 'partition', 'consumer'] as $mutation) {
    [$consumer, $manager, $client] = consumer();
    $client->responses = [fetchResponse(decoded(combined(batch(0, [0]))))];
    $message = $consumer->consume();
    if ($mutation === 'topic') { $message->setTopic('other'); }
    elseif ($mutation === 'partition') { $message->setPartition(1); }
    else { $message->setConsumer(consumer()[0]); }
    rejection(fn () => $consumer->ack($message), LogicException::class, 'Mutated source coordinate rejected');
    check(count($client->requests) === 1, 'Mutation cannot send offset commit');
}
[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(decoded(combined(batch(0, [0, 1]))))];
$message = $consumer->consume();
$client->responses[] = static function () use ($consumer, $message): AbstractResponse {
    rejection(fn () => $consumer->ack($message), LogicException::class, 'Concurrent same-message ACK rejected');
    check($consumer->consume() === $message, 'Concurrent ACK cannot pass pending record');
    return offsetResponse();
};
$consumer->ack($message);
check($consumer->consume()->getOffset() === 1, 'Next pending record remains reachable after reentrant ACK');

foreach ([fetchResponse(batch(0, [0]), 1), (new P\Fetch\FetchResponse())->setTopics([])] as $response) {
    [$consumer, $manager, $client] = consumer();
    $client->responses = [$response];
    rejection(fn () => $consumer->consume(), UnexpectedValueException::class, 'Unassigned/missing fetch response rejected');
}
[$consumer, $manager, $client] = consumer(40);
$client->responses = [fetchResponse(batch(0, []), 0, ErrorCode::OFFSET_OUT_OF_RANGE)];
rejection(fn () => $consumer->consume(), $kafkaError, 'Out of range requires explicit reset policy', ErrorCode::OFFSET_OUT_OF_RANGE);
check($manager->getFetchOffset(0) === 40 && count($client->requests) === 1, 'Out of range does not reset or query latest/earliest');

[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(decoded(combined(batch(0, [0, 1]))))];
$old = $consumer->consume();
rejoinResponses($client, 0);
$consumer->rejoin();
rejection(fn () => $consumer->ack($old), LogicException::class, 'Stale assignment ACK rejected');
$client->responses[] = fetchResponse(decoded(combined(batch(0, [0, 1]))));
check($consumer->consume()->getOffset() === 0, 'Rejoin redelivers durable offset, discards stale buffer');
[$consumer, $manager, $client] = consumer();
$client->responses = array_fill(0, 3, (new P\JoinGroup\JoinGroupResponse())->setErrorCode(ErrorCode::REBALANCE_IN_PROGRESS));
rejection(fn () => $consumer->rejoin(), $kafkaError, 'Rejoin exhaustion visible', ErrorCode::REBALANCE_IN_PROGRESS);
check(count($client->requests) === 3, 'Rejoin attempts bounded by existing groupRetry');

[$consumer, $manager, $client] = consumer();
$client->responses[] = static function () use ($consumer): AbstractResponse {
    $consumer->rejoin();
    return fetchResponse(batch(500, [0]));
};
rejoinResponses($client, 0);
check($consumer->consume() === null, 'Old fetch response discarded after rejoin during IO');
$client->responses[] = fetchResponse(batch(0, [0]));
check($consumer->consume()->getOffset() === 0, 'Fresh assignment fetch starts from confirmed position');

[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(batch(0, [0]))];
$message = $consumer->consume();
$client->responses[] = static function () use ($consumer): AbstractResponse {
    $consumer->rejoin();
    return offsetResponse();
};
rejoinResponses($client, 0);
rejection(fn () => $consumer->ack($message), LogicException::class, 'Old ACK result cannot clear a rejoined assignment');
$client->responses[] = fetchResponse(batch(0, [0]));
check($consumer->consume()->getOffset() === 0, 'Uncertain prior-assignment ACK remains replayable');

[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(batch(0, [0]))];
$message = $consumer->consume();
$client->responses[] = (new P\LeaveGroup\LeaveGroupResponse());
$consumer->close();
rejection(fn () => $consumer->ack($message), LogicException::class, 'Closed consumer ACK rejected');
rejection(fn () => $consumer->consume(), LogicException::class, 'Closed consumer cannot reconnect through consume');

foreach ([0, 2] as $retry) {
    $config = (new ProducerConfig())->setProduceRetry($retry)->setProduceRetrySleep(0);
    $client = new ResponseClient('unused', 9092);
    $broker = new Broker($config);
    $response = (new P\Metadata\MetadataResponse())->setTopics([
        (new P\Metadata\MetadataResponseTopic())->setName('probe')->setErrorCode(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION),
    ]);
    $client->responses = array_fill(0, $retry + 1, $response);
    rejection(fn () => $broker->updateMetadata(['probe'], $client), $kafkaError, 'Metadata exhaustion throws', ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    check(count($client->requests) === $retry + 1, 'Metadata attempts finite');
}
$client = new ResponseClient('unused', 9092);
$broker = new Broker((new ProducerConfig())->setProduceRetry(1)->setProduceRetrySleep(0));
$client->responses = [
    (new P\Metadata\MetadataResponse())->setTopics([(new P\Metadata\MetadataResponseTopic())->setName('probe')->setErrorCode(5)]),
    (new P\Metadata\MetadataResponse())->setTopics([(new P\Metadata\MetadataResponseTopic())->setName('probe')]),
];
$broker->updateMetadata(['probe'], $client);
check(count($broker->getTopicsMeta()) === 1, 'Metadata positive retry caches one confirmed topic');
$client->responses = [(new P\Metadata\MetadataResponse())->setTopics([])];
rejection(fn () => $broker->updateMetadata(['probe'], $client), UnexpectedValueException::class, 'Missing metadata topic rejected');


foreach ([offsetResponse(0, 1), (new P\OffsetCommit\OffsetCommitResponse())->setTopics([]),
    (new P\OffsetCommit\OffsetCommitResponse())->setTopics([offsetResponse()->getTopics()[0], offsetResponse()->getTopics()[0]])] as $response) {
    [$consumer, $manager, $client] = consumer();
    $client->responses = [fetchResponse(batch(0, [0])), $response];
    $message = $consumer->consume();
    rejection(fn () => $consumer->ack($message), UnexpectedValueException::class, 'Missing/foreign/duplicate ACK response');
    check($manager->getFetchOffset(0) === 0 && $consumer->consume() === $message, 'Malformed ACK cannot release pending message');
}
$idle = new RecordBatch();
$idle->unpack(Int32::pack(0));
check($idle->getBatches() === [], 'Empty record set has no fabricated batch');
[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse($idle)];
check($consumer->consume() === null, 'Ordinary idle fetch stays valid');
foreach (['empty', 'control', 'transactional'] as $kind) {
    $recordBatch = batch(0, $kind === 'empty' ? [] : [0]);
    if ($kind === 'control') { $recordBatch->getAttributes()->setIsControlBatch(true); }
    if ($kind === 'transactional') { $recordBatch->getAttributes()->setIsTransactional(true); }
    [$consumer, $manager, $client] = consumer();
    $client->responses = [fetchResponse(decoded($recordBatch->pack()))];
    rejection(fn () => $consumer->consume(), UnexpectedValueException::class, 'Unsupported batch policy visible');
    check($manager->getFetchOffset(0) === 0, 'Unsupported batch cannot advance offset');
}
[$consumer, $manager, $client] = consumer();
$duplicate = fetchResponse(batch(0, [0]));
$topic = $duplicate->getTopics()[0];
$topic->setPartitions([$topic->getPartitions()[0], $topic->getPartitions()[0]]);
$client->responses = [$duplicate];
rejection(fn () => $consumer->consume(), UnexpectedValueException::class, 'Duplicate fetched partition rejected');
[$consumer, $manager, $client] = consumer();
$client->responses = [fetchResponse(decoded(combined(batch(0, [0]), batch(0, [0]))))];
rejection(fn () => $consumer->consume(), UnexpectedValueException::class, 'Duplicate batch offset cannot become a message');
foreach ([VarInt::MIN_VALUE, VarInt::MAX_VALUE] as $delta) {
    $record = (new Record())->setTimestampDelta($delta);
    $read = new Record(); $read->unpack($record->pack());
    check($read->getTimestampDelta() === $delta, 'Supported timestamp boundary round trip');
}
foreach ([VarInt::MIN_VALUE - 1, VarInt::MAX_VALUE + 1] as $delta) {
    rejection(fn () => (new Record())->setTimestampDelta($delta)->pack(), InvalidArgumentException::class, 'Out-of-range timestamp pack rejects');
}
malformedRecord("\0\0\0\1\1\2\0\1");
$gzip = batch(42, [0, 1]);
$gzip->getAttributes()->setCompression(\longlang\phpkafka\Protocol\RecordBatch\Enum\Compression::GZIP);
check(count(decoded($gzip->pack())->getRecords()) === 2, 'Existing small gzip batch remains compatible');
[$consumer, $manager, $client] = consumer();
$client->responses[] = static function () use ($consumer): AbstractResponse {
    $consumer->close();
    return (new P\JoinGroup\JoinGroupResponse())->setMemberId('member')->setLeader('other')->setGenerationId(2);
};
$client->responses[] = new P\LeaveGroup\LeaveGroupResponse();
rejection(fn () => $consumer->rejoin(), LogicException::class, 'Close during rejoin prevents assignment publication');
check(count($client->requests) === 2, 'Close during rejoin performs no subsequent sync/offset/heartbeat requests');
rejection(fn () => $consumer->consume(), LogicException::class, 'Closed suspended rejoin cannot reopen consumer');

echo json_encode(['status' => 'PASS', 'checks' => $checks, 'php' => PHP_VERSION,
    'scope' => 'Native methods with scripted responses, no real broker acceptance',
    'sources' => array_map(static fn (string $file): string => hash_file('sha256', dirname(__DIR__) . '/' . $file), [
        'src/Producer/Producer.php', 'src/Consumer/Consumer.php', 'src/Consumer/ConsumeMessage.php',
        'src/Consumer/OffsetManager.php', 'src/Broker.php', 'src/Protocol/RecordBatch/RecordBatch.php', 'src/Protocol/RecordBatch/Record.php',
    ]),
], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR), PHP_EOL;
