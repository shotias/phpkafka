<?php

declare(strict_types=1);

namespace longlang\phpkafka\Consumer;

use InvalidArgumentException;
use longlang\phpkafka\Broker;
use longlang\phpkafka\Consumer\Assignor\PartitionAssignorInterface;
use longlang\phpkafka\Consumer\Struct\ConsumerGroupMemberMetadata;
use longlang\phpkafka\Exception\KafkaErrorException;
use longlang\phpkafka\Group\CoordinatorType;
use longlang\phpkafka\Group\GroupManager;
use longlang\phpkafka\Group\ProtocolType;
use longlang\phpkafka\Group\Struct\ConsumerGroupMemberAssignment;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\Fetch\FetchableTopic;
use longlang\phpkafka\Protocol\Fetch\FetchPartition;
use longlang\phpkafka\Protocol\Fetch\FetchRequest;
use longlang\phpkafka\Protocol\Fetch\FetchResponse;
use longlang\phpkafka\Protocol\FindCoordinator\FindCoordinatorResponse;
use longlang\phpkafka\Protocol\JoinGroup\JoinGroupRequestProtocol;
use longlang\phpkafka\Timer\TimerInterface;
use longlang\phpkafka\Util\KafkaUtil;

class Consumer
{
    /**
     * @var ConsumerConfig
     */
    protected $config;

    /**
     * @var Broker
     */
    protected $broker;

    /**
     * @var callable|null
     */
    protected $consumeCallback;

    /**
     * @var GroupManager
     */
    protected $groupManager;

    /**
     * @var OffsetManager[]
     */
    protected $offsetManagers = [];

    /**
     * @var string
     */
    protected $memberId;

    /**
     * @var int
     */
    protected $generationId;

    /**
     * @var bool
     */
    private $started = false;

    /**
     * @var ConsumeMessage[]
     */
    private $messages = [];

    /**
     * @var bool
     */
    private $swooleHeartbeat;

    /**
     * @var float
     */
    private $lastHeartbeatTime = 0;

    /**
     * @var int|null
     */
    private $heartbeatTimerId;

    /**
     * @var ConsumerGroupMemberAssignment
     */
    private $consumerGroupMemberAssignment;

    /**
     * @var PartitionAssignorInterface
     */
    private $assignor;

    /**
     * @var FindCoordinatorResponse
     */
    private $coordinator;

    /**
     * @var array
     */
    protected $fetchOptions = [];

    /**
     * @var int
     */
    protected $emptyMessageCountInLoop = 0;

    /**
     * @var TimerInterface
     */
    protected $timer;

    private ?ConsumeMessage $pendingMessage = null;

    /** @var array{string, int, int}|null */
    private ?array $pendingPosition = null;

    private int $assignmentId = 0;

    private ?ConsumeMessage $lastAcknowledged = null;

    private bool $acknowledging = false;

    private bool $closed = false;

    private bool $rejoining = false;

    private bool $needsRejoin = false;

    public function __construct(ConsumerConfig $config, ?callable $consumeCallback = null)
    {
        $this->config = $config;
        $this->consumeCallback = $consumeCallback;

        $timerClass = KafkaUtil::getTimerClass($config->getTimer());
        $this->timer = new $timerClass();

        $this->broker = $broker = new Broker($config);
        if ($config->getUpdateBrokers()) {
            $broker->updateBrokers();
        } else {
            $broker->setBrokers($config->getBroker());
        }

        $this->groupManager = $groupManager = new GroupManager($broker);
        $groupId = $config->getGroupId();

        $this->broker->updateMetadata($config->getTopic());

        // findCoordinator
        $this->coordinator = $groupManager->findCoordinator($groupId, CoordinatorType::GROUP, $config->getGroupRetry(), $config->getGroupRetrySleep());

        $this->rejoin();
    }

    public function rejoin(): void
    {
        if ($this->closed) {
            throw new \LogicException('Consumer is closed');
        }
        if ($this->rejoining) {
            throw new \RuntimeException('Consumer rejoin already in progress');
        }
        $retry = $this->config->getGroupRetry();
        if ($retry < 0) {
            throw new InvalidArgumentException('groupRetry must not be negative');
        }
        $this->rejoining = true;
        $this->needsRejoin = true;
        $assignmentId = ++$this->assignmentId;
        $this->messages = [];
        $this->pendingMessage = null;
        $this->pendingPosition = null;
        $this->lastAcknowledged = null;
        $this->offsetManagers = [];
        $this->fetchOptions = [];
        try {
            for ($attempt = 0; ; ++$attempt) {
                try {
                    $this->stopHeartbeat();

                    $config = $this->config;
                    $groupManager = $this->groupManager;
                    $groupId = $config->getGroupId();
                    $topics = $config->getTopic();

                    $metadata = new ConsumerGroupMemberMetadata();
                    $metadata->setTopics($config->getTopic());
                    $metadataContent = $metadata->pack();
                    $protocolName = 'group';
                    $protocols = [
                        (new JoinGroupRequestProtocol())->setName($protocolName)->setMetadata($metadataContent),
                    ];

                    // joinGroup
                    $response = $groupManager->joinGroup($groupId, $config->getMemberId(), ProtocolType::CONSUMER, $config->getGroupInstanceId(), $protocols, (int) ($config->getSessionTimeout() * 1000), (int) ($config->getRebalanceTimeout() * 1000), $config->getGroupRetry(), $config->getGroupRetrySleep());
                    $this->ensureAssignment($assignmentId);
                    $this->memberId = $response->getMemberId();
                    $this->generationId = $response->getGenerationId();

                    // syncGroup
                    if ($this->groupManager->isLeader()) {
                        $assignorClass = $config->getPartitionAssignmentStrategy();
                        /** @var PartitionAssignorInterface $assignor */
                        $assignor = $this->assignor = new $assignorClass();
                        $assignments = $assignor->assign($this->broker->getTopicsMeta(), $this->groupManager->getJoinGroupResponse()->getMembers());
                        $response = $groupManager->syncGroup($groupId, $config->getGroupInstanceId(), $this->memberId, $this->generationId, $protocolName, ProtocolType::CONSUMER, $assignments, $config->getGroupRetry(), $config->getGroupRetrySleep());
                    } else {
                        $response = $groupManager->syncGroup($groupId, $config->getGroupInstanceId(), $this->memberId, $this->generationId, $protocolName, ProtocolType::CONSUMER, [], $config->getGroupRetry(), $config->getGroupRetrySleep());
                    }

                    $this->ensureAssignment($assignmentId);
                    $this->consumerGroupMemberAssignment = $consumerGroupMemberAssignment = new ConsumerGroupMemberAssignment();
                    $data = $response->getAssignment();
                    if ('' !== $data) {
                        $consumerGroupMemberAssignment->unpack($data);
                    }

                    $this->initFetchOptions();

                    foreach ($topics as $topic) {
                        $this->offsetManagers[$topic] = $offsetManager = new OffsetManager($this->broker, $this->coordinator->getNodeId(), $topic, $this->getPartitions($topic), $groupId, $config->getGroupInstanceId(), $this->memberId, $this->generationId);
                        $offsetManager->updateOffsets($config->getOffsetRetry());
                        $this->ensureAssignment($assignmentId);
                    }

                    $this->ensureAssignment($assignmentId);
                    $this->startHeartbeat();
                    $this->needsRejoin = false;
                    return;
                } catch (KafkaErrorException $exception) {
                    if (ErrorCode::REBALANCE_IN_PROGRESS !== $exception->getCode() || $attempt >= $retry) {
                        throw $exception;
                    }
                }
            }
        } finally {
            $this->rejoining = false;
        }
    }

    private function ensureAssignment(int $assignmentId): void
    {
        if ($this->closed || $assignmentId !== $this->assignmentId) {
            throw new \LogicException('Consumer assignment changed during rejoin');
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->stop();
        ++$this->assignmentId;
        $this->messages = [];
        $this->pendingMessage = null;
        $this->pendingPosition = null;
        $this->lastAcknowledged = null;
        $this->stopHeartbeat();
        try {
            $config = $this->config;
            $groupId = $config->getGroupId();
            if (null !== $groupId) {
                $this->groupManager->leaveGroup($groupId, $this->memberId, $config->getGroupInstanceId(), $config->getGroupRetry(), $config->getGroupRetrySleep());
            }
        } finally {
            $this->broker->close();
        }
    }

    public function start(): void
    {
        $consumeCallback = $this->consumeCallback;
        if (null === $consumeCallback) {
            throw new InvalidArgumentException('consumeCallback must not null');
        }
        $interval = (int) ($this->config->getInterval() * 1000000);
        $this->started = true;
        $autoCommit = $this->config->getAutoCommit();
        while ($this->started) {
            $message = $this->consume();
            if (null === $message) {
                if ($interval > 0 && $this->emptyMessageCountInLoop === \count($this->fetchOptions)) {
                    // When the empty message count is equal with the count of fetch options,
                    // We must sleep some micro seconds to avoid dead cycle.
                    usleep($interval);
                }
            } else {
                $consumeCallback($message);
                if ($autoCommit) {
                    $this->ack($message);
                }
            }
        }
    }

    public function stop(): void
    {
        $this->started = false;
    }

    public function consume(): ?ConsumeMessage
    {
        if ($this->closed) {
            throw new \LogicException('Consumer is closed');
        }
        if ($this->needsRejoin) {
            $this->rejoin();
        }
        // Do not pass an unconfirmed record, including after a callback or ACK exception.
        if ($this->pendingMessage !== null) {
            return $this->pendingMessage;
        }
        if ([] === $this->messages) {
            $this->fetchMessages();
        }
        $message = array_shift($this->messages);
        if ($message !== null) {
            $this->pendingMessage = $message;
            $this->pendingPosition = [$message->getTopic(), $message->getPartition(), $message->getOffset()];
        }
        return $message;
    }

    public function ack(ConsumeMessage $message): void
    {
        $offset = $message->getOffset();
        if (!$message->isFrom($this, $this->assignmentId)
            || $offset === null || $offset < 0 || $offset === PHP_INT_MAX || $this->needsRejoin || $this->closed) {
            throw new \LogicException('Cannot acknowledge a foreign, stale or unpositioned message');
        }
        if ($this->lastAcknowledged === $message) {
            return;
        }
        $partition = $message->getPartition();
        if ($this->acknowledging || $this->pendingMessage !== $message
            || $this->pendingPosition !== [$message->getTopic(), $partition, $offset]) {
            throw new \LogicException('Only the unchanged outstanding message can be acknowledged');
        }
        $offsetManager = $this->getOffsetManager($message->getTopic());
        $assignmentId = $this->assignmentId;
        $this->acknowledging = true;
        try {
            $offsetManager->commitOffset($partition, $offset + 1, $this->config->getOffsetRetry());
            if ($assignmentId !== $this->assignmentId || $this->pendingMessage !== $message) {
                throw new \LogicException('Consumer assignment changed during acknowledgement');
            }
            $this->lastAcknowledged = $message;
            $this->pendingMessage = null;
            $this->pendingPosition = null;
        } finally {
            $this->acknowledging = false;
        }
    }

    protected function initFetchOptions(): void
    {
        $fetchOptions = [];
        $config = $this->config;
        $broker = $this->broker;
        $topicsMeta = $broker->getTopicsMeta();
        foreach ($config->getTopic() as $topic) {
            $currentTopicMetaItem = null;
            foreach ($topicsMeta as $topicMetaItem) {
                if ($topicMetaItem->getName() === $topic) {
                    $currentTopicMetaItem = $topicMetaItem;
                    break;
                }
            }
            if (!$currentTopicMetaItem) {
                continue;
            }
            foreach ($this->getFetchPartitions($topic) as $partition) {
                foreach ($currentTopicMetaItem->getPartitions() as $topicsMetaItemPartition) {
                    if ($partition === $topicsMetaItemPartition->getPartitionIndex()) {
                        $fetchOptions[$topicsMetaItemPartition->getLeaderId()][$topic][] = $partition;
                        break;
                    }
                }
            }
        }
        $this->fetchOptions = $fetchOptions;
    }

    protected function fetchMessages(): void
    {
        $this->checkBeartbeat();

        $config = $this->config;
        $request = new FetchRequest();
        $request->setReplicaId($config->getReplicaId());
        $request->setMinBytes($config->getMinBytes());
        $request->setMaxBytes($config->getMaxBytes());
        $request->setMaxWait($config->getMaxWait());
        $request->setRackId($config->getRackId());
        $topics = [];
        $currentList = current($this->fetchOptions);
        if (false === $currentList) {
            $currentList = reset($this->fetchOptions);
            $this->emptyMessageCountInLoop = 0;
        }
        $nodeId = key($this->fetchOptions);
        next($this->fetchOptions);
        if (empty($this->fetchOptions)) {
            // avoid dead cycle.
            sleep(1);
        }
        if (!$currentList) {
            return;
        }
        foreach ($currentList as $topic => $partitions) {
            $fetchPartitions = [];
            foreach ($partitions as $partition) {
                $fetchPartitions[] = (new FetchPartition())->setPartitionIndex($partition)->setFetchOffset($this->getOffsetManager($topic)->getFetchOffset($partition));
            }
            $topics[] = (new FetchableTopic())->setName($topic)->setFetchPartitions($fetchPartitions);
        }
        $request->setTopics($topics);

        $assignmentId = $this->assignmentId;
        /** @var FetchResponse $response */
        $response = $this->broker->getClient($nodeId)->sendRecv($request);
        if ($assignmentId !== $this->assignmentId || $this->needsRejoin) {
            return;
        }
        $errorCode = $response->getErrorCode();
        switch ($errorCode) {
            case ErrorCode::REBALANCE_IN_PROGRESS:
                $this->rejoin();

                return;
            default:
                ErrorCode::check($errorCode);
        }

        $messages = [];
        $expected = [];
        foreach ($currentList as $topicName => $partitions) {
            $expected[$topicName] = array_fill_keys($partitions, true);
        }
        $seenTopics = [];
        foreach ($response->getTopics() as $topic) {
            if (!isset($expected[$topic->getName()]) || isset($seenTopics[$topic->getName()])) {
                throw new \UnexpectedValueException('Unexpected or duplicate fetch response topic');
            }
            $seenTopics[$topic->getName()] = true;
            foreach ($topic->getPartitions() as $partition) {
                $topicName = $topic->getName();
                $partitionIndex = $partition->getPartitionIndex();
                if (!isset($expected[$topicName][$partitionIndex])) {
                    throw new \UnexpectedValueException('Unexpected or duplicate fetch response partition');
                }
                unset($expected[$topicName][$partitionIndex]);
                $errorCode = $partition->getErrorCode();
                switch ($errorCode) {
                    case ErrorCode::UNKNOWN_TOPIC_OR_PARTITION:
                    case ErrorCode::LEADER_NOT_AVAILABLE:
                    case ErrorCode::NOT_LEADER_OR_FOLLOWER:
                    case ErrorCode::REPLICA_NOT_AVAILABLE:
                        $this->rejoin();

                        return;
                    default:
                        ErrorCode::check($errorCode);
                        $partitionIndex = $partition->getPartitionIndex();
                        $fetchOffset = $this->getOffsetManager($topic->getName())->getFetchOffset($partitionIndex);
                        $previousOffset = -1;
                        foreach ($partition->getRecords()->getBatches() as $batch) {
                            if (!$batch->getRecords() || $batch->getAttributes()->getIsControlBatch()
                                || $batch->getAttributes()->getIsTransactional()) {
                                throw new \UnexpectedValueException('Empty compacted or transactional batches require an explicit consumer policy');
                            }
                            foreach ($batch->getRecords() as $record) {
                                $base = $batch->getBaseOffset();
                                $delta = $record->getOffsetDelta();
                                if ($base < 0 || $delta < 0 || $base >= PHP_INT_MAX - $delta) {
                                    throw new \UnexpectedValueException('Invalid record offset');
                                }
                                $offset = $base + $delta;
                                if ($offset <= $previousOffset) {
                                    throw new \UnexpectedValueException('Non-increasing record offsets');
                                }
                                $previousOffset = $offset;
                                if ($offset < $fetchOffset) {
                                    continue;
                                }
                                $messages[] = new ConsumeMessage($this, $topic->getName(), $partitionIndex,
                                    $record->getKey(), $record->getValue(), $record->getHeaders(), $offset, $assignmentId);
                            }
                        }
                }
            }
        }
        foreach ($expected as $partitions) {
            if ($partitions) {
                throw new \UnexpectedValueException('Missing fetch response partition');
            }
        }
        $this->messages = $messages;
        if (empty($messages)) {
            ++$this->emptyMessageCountInLoop;
        }
    }

    public function getConfig(): ConsumerConfig
    {
        return $this->config;
    }

    public function getBroker(): Broker
    {
        return $this->broker;
    }

    protected function startHeartbeat(): void
    {
        $this->heartbeatTimerId = $this->timer->tick((int) ($this->config->getGroupHeartbeat() * 1000), function () {
            try {
                $this->heartbeat();
            } catch (KafkaErrorException $e) {
                $callback = $this->getConfig()->getExceptionCallback();
                if ($callback) {
                    $callback($e);
                }
            }
        });
    }

    protected function stopHeartbeat(): void
    {
        if ($this->heartbeatTimerId) {
            $this->timer->clear($this->heartbeatTimerId);
            $this->heartbeatTimerId = null;
        }
    }

    protected function heartbeat(): void
    {
        $config = $this->config;
        try {
            $this->groupManager->heartbeat($config->getGroupId(), $config->getGroupInstanceId(), $this->memberId, $this->generationId);
        } catch (KafkaErrorException $kafkaErrorException) {
            switch ($kafkaErrorException->getCode()) {
                case ErrorCode::REBALANCE_IN_PROGRESS:
                    $this->rejoin();
                    break;
                default:
                    throw $kafkaErrorException;
            }
        }
    }

    protected function checkBeartbeat(): void
    {
        $time = microtime(true);
        if ($time - $this->lastHeartbeatTime >= $this->config->getGroupHeartbeat()) {
            $this->lastHeartbeatTime = $time;
            $this->heartbeat();
        }
    }

    protected function getPartitions(string $topic): array
    {
        $partitions = [];
        foreach ($this->broker->getTopicsMeta() as $topicMeta) {
            if ($topicMeta->getName() === $topic) {
                foreach ($topicMeta->getPartitions() as $partition) {
                    $partitions[] = $partition->getPartitionIndex();
                }
                break;
            }
        }

        return $partitions;
    }

    /**
     * @return int[]
     */
    protected function getFetchPartitions(string $topic): array
    {
        $partitions = [];
        foreach ($this->consumerGroupMemberAssignment->getTopics() as $consumerGroupMemberAssignmentTopic) {
            if ($consumerGroupMemberAssignmentTopic->getTopicName() === $topic) {
                foreach ($consumerGroupMemberAssignmentTopic->getPartitions() as $partition) {
                    $partitions[] = $partition;
                }
                break;
            }
        }

        return $partitions;
    }

    public function getOffsetManager(string $topic): OffsetManager
    {
        if (!isset($this->offsetManagers[$topic])) {
            throw new \RuntimeException(sprintf('Topic %s does not exists', $topic));
        }

        return $this->offsetManagers[$topic];
    }
}
