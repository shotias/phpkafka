<?php

declare(strict_types=1);

namespace longlang\phpkafka\Consumer;

use longlang\phpkafka\Broker;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetPartition;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetRequest;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetResponse;
use longlang\phpkafka\Protocol\ListOffset\ListOffsetTopic;
use longlang\phpkafka\Protocol\OffsetCommit\OffsetCommitRequest;
use longlang\phpkafka\Protocol\OffsetCommit\OffsetCommitRequestPartition;
use longlang\phpkafka\Protocol\OffsetCommit\OffsetCommitRequestTopic;
use longlang\phpkafka\Protocol\OffsetCommit\OffsetCommitResponse;
use longlang\phpkafka\Protocol\OffsetFetch\OffsetFetchRequest;
use longlang\phpkafka\Protocol\OffsetFetch\OffsetFetchRequestTopic;
use longlang\phpkafka\Protocol\OffsetFetch\OffsetFetchResponse;
use longlang\phpkafka\Util\KafkaUtil;

class OffsetManager
{
    /**
     * @var Broker
     */
    protected $broker;

    /**
     * @var string
     */
    protected $topic;

    /**
     * @var int[]
     */
    protected $partitions;

    /**
     * @var string
     */
    protected $groupId;

    /**
     * @var string|null
     */
    protected $groupInstanceId;

    /**
     * @var string
     */
    protected $memberId;

    /**
     * @var int
     */
    protected $generationId;

    /**
     * offsets map.
     *
     * partition => offset
     *
     * @var int[]
     */
    private $offsets;

    /**
     * @var string[]
     */
    private $metadatas;

    /**
     * @var int
     */
    private $coordinatorNodeId;

    public function __construct(Broker $broker, int $coordinatorNodeId, string $topic, array $partitions, string $groupId, ?string $groupInstanceId, string $memberId, int $generationId)
    {
        $this->broker = $broker;
        $this->coordinatorNodeId = $coordinatorNodeId;
        $this->topic = $topic;
        $this->partitions = $partitions;
        $this->groupId = $groupId;
        $this->groupInstanceId = $groupInstanceId;
        $this->memberId = $memberId;
        $this->generationId = $generationId;
    }

    public function updateOffsets(int $retry = 0): void
    {
        $client = $this->broker->getClient($this->coordinatorNodeId);

        $request = new OffsetFetchRequest();
        $request->setGroupId($this->groupId);
        $request->setTopics([
            (new OffsetFetchRequestTopic())->setName($this->topic)->setPartitionIndexes($this->partitions),
        ]);

        /** @var OffsetFetchResponse $response */
        $response = KafkaUtil::retry($client, $request, $retry, 0);

        $metadatas = $offsets = [];
        if (count($response->getTopics()) !== 1) {
            throw new \UnexpectedValueException('Unexpected offset fetch topic count');
        }
        foreach ($response->getTopics() as $topic) {
            if ($topic->getName() !== $this->topic) {
                throw new \UnexpectedValueException('Unexpected offset fetch topic');
            }
            foreach ($topic->getPartitions() as $partition) {
                ErrorCode::check($partition->getErrorCode());
                $partitionIndex = $partition->getPartitionIndex();
                if (!in_array($partitionIndex, $this->partitions, true) || isset($offsets[$partitionIndex])) {
                    throw new \UnexpectedValueException('Unexpected offset fetch partition');
                }
                if ($partition->getCommittedOffset() < -1) {
                    throw new \UnexpectedValueException('Invalid committed offset');
                }
                $offsets[$partitionIndex] = max($partition->getCommittedOffset(), 0);
                $metadatas[$partitionIndex] = $partition->getMetadata();
            }
        }
        if (count($offsets) !== count($this->partitions)) {
            throw new \UnexpectedValueException('Missing offset fetch partition');
        }
        $this->offsets = $offsets;
        $this->metadatas = $metadatas;
    }

    public function updateListOffsets(array $partitions, int $retry = 0): void
    {
        $brokerPartitionMap = [];
        $broker = $this->broker;
        $topicName = $this->topic;
        foreach ($partitions as $partition) {
            $brokerPartitionMap[$broker->getBrokerIdByTopic($topicName, $partition)][] = $partition;
        }
        $topicsMeta = $broker->getTopicsMeta($topicName);
        foreach ($brokerPartitionMap as $brokerId => $partitions) {
            $request = new ListOffsetRequest();
            $topicPartitions = [];
            foreach ($partitions as $partition) {
                $topicPartitions[] = $listOffsetPartition = new ListOffsetPartition();
                foreach ($topicsMeta as $topicMeta) {
                    if ($topicMeta->getName() === $topicName) {
                        foreach ($topicMeta->getPartitions() as $partitionObject) {
                            if ($partition === $partitionObject->getPartitionIndex()) {
                                $listOffsetPartition->setCurrentLeaderEpoch($partitionObject->getLeaderEpoch());
                                break;
                            }
                        }
                        break;
                    }
                }
                $listOffsetPartition->setPartitionIndex($partition)->setTimestamp(-1);
            }
            $request->setTopics([
                (new ListOffsetTopic())->setName($topicName)->setPartitions($topicPartitions),
            ]);
            $client = $broker->getClientByBrokerId($brokerId);
            /** @var ListOffsetResponse $response */
            $response = KafkaUtil::retry($client, $request, $retry, 0);
            foreach ($response->getTopics() as $topic) {
                foreach ($topic->getPartitions() as $partition) {
                    $this->offsets[$partition->getPartitionIndex()] = $partition->getOffset() - 1;
                }
            }
        }
    }

    public function getBroker(): Broker
    {
        return $this->broker;
    }

    public function getTopic(): string
    {
        return $this->topic;
    }

    /**
     * @return int[]
     */
    public function getPartitions(): array
    {
        return $this->partitions;
    }

    /**
     * @return int[]
     */
    public function getOffsets(): array
    {
        return $this->offsets;
    }

    public function getGroupInstanceId(): ?string
    {
        return $this->groupInstanceId;
    }

    public function getMemberId(): string
    {
        return $this->memberId;
    }

    public function getFetchOffset(int $partition): int
    {
        if (!isset($this->offsets[$partition])) {
            throw new \RuntimeException(sprintf('Partition %s does not exists', $partition));
        }

        return $this->offsets[$partition];
    }

    public function addFetchOffset(int $partition, int $offset = 1): void
    {
        if (!isset($this->offsets[$partition])) {
            throw new \RuntimeException(sprintf('Partition %s does not exists', $partition));
        }
        $this->offsets[$partition] += $offset;
    }

    public function saveOffsets(int $partition, int $retry = 0): void
    {
        $this->commitOffset($partition, $this->getFetchOffset($partition), $retry);
    }

    /**
     * Commit an explicit next record offset; local position changes only after confirmation.
     */
    public function commitOffset(int $partition, int $offset, int $retry = 0): void
    {
        if ($retry < 0 || $offset < $this->getFetchOffset($partition)) {
            throw new \InvalidArgumentException('Invalid offset commit or retry budget');
        }
        $request = new OffsetCommitRequest();
        $request->setGroupId($this->groupId);
        $request->setGroupInstanceId($this->groupInstanceId);
        $request->setMemberId($this->memberId);
        $request->setGenerationId($this->generationId);
        $request->setTopics([
            (new OffsetCommitRequestTopic())->setName($this->topic)->setPartitions([
                (new OffsetCommitRequestPartition())->setPartitionIndex($partition)
                    ->setCommittedOffset($offset)->setCommitTimestamp((int) (microtime(true) * 1000))
                    ->setCommittedMetadata($this->metadatas[$partition]),
            ]),
        ]);
        for ($i = 0; ; ++$i) {
            /** @var OffsetCommitResponse $response */
            $response = $this->broker->getClientByBrokerId($this->coordinatorNodeId)->sendRecv($request);
            $topics = $response->getTopics();
            if (1 !== count($topics) || $topics[0]->getName() !== $this->topic) {
                throw new \UnexpectedValueException('Unexpected offset commit topic response');
            }
            $partitions = $topics[0]->getPartitions();
            if (1 !== count($partitions) || $partitions[0]->getPartitionIndex() !== $partition) {
                throw new \UnexpectedValueException('Unexpected offset commit partition response');
            }
            $errorCode = $partitions[0]->getErrorCode();
            if (ErrorCode::success($errorCode)) {
                $this->offsets[$partition] = $offset;
                return;
            }
            if ($i >= $retry || !ErrorCode::canRetry($errorCode)) {
                ErrorCode::check($errorCode);
            }
        }
    }

    public function getGroupId(): string
    {
        return $this->groupId;
    }

    public function getGenerationId(): int
    {
        return $this->generationId;
    }
}
