<?php

declare(strict_types=1);

namespace longlang\phpkafka\Producer;

use longlang\phpkafka\Broker;
use longlang\phpkafka\Producer\Partitioner\PartitionerInterface;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\Produce\PartitionProduceData;
use longlang\phpkafka\Protocol\Produce\ProduceRequest;
use longlang\phpkafka\Protocol\Produce\ProduceResponse;
use longlang\phpkafka\Protocol\Produce\TopicProduceData;
use longlang\phpkafka\Protocol\RecordBatch\Record;
use longlang\phpkafka\Protocol\RecordBatch\RecordBatch;
use longlang\phpkafka\Protocol\RecordBatch\RecordHeader;

class Producer
{
    /**
     * @var ProducerConfig
     */
    protected $config;

    /**
     * @var Broker
     */
    protected $broker;

    /**
     * @var PartitionerInterface
     */
    protected $partitioner;

    public function __construct(ProducerConfig $config)
    {
        $this->config = $config;
        $this->broker = $broker = new Broker($config);
        if ($config->getUpdateBrokers()) {
            $broker->updateBrokers();
        } else {
            $broker->setBrokers($config->getBrokers());
        }
        $class = $config->getPartitioner();
        $this->partitioner = new $class();
    }

    /**
     * @param RecordHeader[]|array $headers
     */
    public function send(string $topic, ?string $value, ?string $key = null, array $headers = [], ?int $partitionIndex = null): void
    {
        $message = new ProduceMessage($topic, $value, $key, $headers, $partitionIndex);
        $messages = [$message];
        $this->sendBatch($messages);
    }

    /**
     * @param ProduceMessage[] $messages
     */
    public function sendBatch(array $messages): void
    {
        $config = $this->config;
        $broker = $this->broker;
        $request = new ProduceRequest();
        $request->setAcks($acks = $config->getAcks());
        $recvTimeout = $config->getRecvTimeout();
        if ($recvTimeout < 0) {
            $request->setTimeoutMs(60000);
        } else {
            $request->setTimeoutMs((int) ($recvTimeout * 1000));
        }

        $timestamp = (int) (microtime(true) * 1000);
        /** @var TopicProduceData[][] $topicsMap */
        $topicsMap = [];
        $partitionsMap = [];
        $topics = [];
        foreach ($messages as $message) {
            $topics[] = $message->getTopic();
        }
        $topicsMeta = $broker->getTopicsMeta($topics);
        foreach ($messages as $message) {
            $topicName = $message->getTopic();
            $value = $message->getValue();
            $key = $message->getKey();
            $partitionIndex = $message->getPartitionIndex() ?? $this->partitioner->partition($topicName, $value, $key, $topicsMeta);
            $brokerId = $broker->getBrokerIdByTopic($topicName, $partitionIndex);
            if (isset($topicsMap[$brokerId][$topicName])) {
                /** @var TopicProduceData $topicData */
                $topicData = $topicsMap[$brokerId][$topicName];
                $partitions = $topicData->getPartitions();
            } else {
                $topicData = $topicsMap[$brokerId][$topicName] = new TopicProduceData();
                $topicData->setName($topicName);
                $partitions = [];
            }
            if (isset($partitionsMap[$topicName][$partitionIndex])) {
                /** @var PartitionProduceData $partition */
                $partition = $partitionsMap[$topicName][$partitionIndex];
                $recordBatch = $partition->getRecords();
                $records = $recordBatch->getRecords();
            } else {
                $partition = $partitions[] = $partitionsMap[$topicName][$partitionIndex] = new PartitionProduceData();
                $partition->setPartitionIndex($partitionIndex);
                $partition->setRecords($recordBatch = new RecordBatch());
                $recordBatch->setProducerId($config->getProducerId());
                $recordBatch->setProducerEpoch($config->getProducerEpoch());
                $recordBatch->setPartitionLeaderEpoch($config->getPartitionLeaderEpoch());
                $recordBatch->setFirstTimestamp($timestamp);
                $recordBatch->setMaxTimestamp($timestamp);
                $recordBatch->setLastOffsetDelta(-1);
                $records = [];
            }
            $offsetDelta = $recordBatch->getLastOffsetDelta() + 1;
            $recordBatch->setLastOffsetDelta($offsetDelta);
            $record = $records[] = new Record();
            $record->setKey($key);
            $record->setValue($value);
            $headers = [];
            foreach ($message->getHeaders() as $key => $value) {
                if ($value instanceof RecordHeader) {
                    $headers[] = $value;
                // @phpstan-ignore-next-line
                } else {
                    $headers[] = (new RecordHeader())->setHeaderKey($key)->setValue($value);
                }
            }
            $record->setHeaders($headers);
            $record->setOffsetDelta($offsetDelta);
            $record->setTimestampDelta(((int) (microtime(true) * 1000)) - $timestamp);
            $recordBatch->setRecords($records);

            $topicData->setPartitions($partitions);
        }
        $produceRetry = $config->getProduceRetry();
        if ($produceRetry < 0) {
            throw new \InvalidArgumentException('produceRetry must not be negative');
        }
        $produceRetrySleep = $config->getProduceRetrySleep();
        foreach ($topicsMap as $brokerId => $topics) {
            for ($retryCount = 0; ; ++$retryCount) {
                $request->setTopics(array_values($topics));
                $hasResponse = 0 !== $acks;
                $client = $broker->getClient($brokerId);
                $correlationId = $client->send($request, null, $hasResponse);
                if (!$hasResponse) {
                    break;
                }
                /** @var ProduceResponse $response */
                $response = $client->recv($correlationId);
                $expected = [];
                foreach ($topics as $topic) {
                    foreach ($topic->getPartitions() as $partition) {
                        $expected[$topic->getName()][$partition->getPartitionIndex()] = true;
                    }
                }
                $retryTopics = [];
                $seenTopics = [];
                foreach ($response->getResponses() as $topicResponse) {
                    $name = $topicResponse->getName();
                    if (!isset($topics[$name]) || isset($seenTopics[$name])) {
                        throw new \UnexpectedValueException('Unexpected or duplicate produce response topic');
                    }
                    $seenTopics[$name] = true;
                    foreach ($topicResponse->getPartitions() as $partition) {
                        $name = $topicResponse->getName();
                        $index = $partition->getPartitionIndex();
                        if (!isset($expected[$name][$index])) {
                            throw new \UnexpectedValueException('Unexpected or duplicate produce response partition');
                        }
                        unset($expected[$name][$index]);
                        $errorCode = $partition->getErrorCode();
                        if (ErrorCode::UNKNOWN_TOPIC_OR_PARTITION === $errorCode || ErrorCode::LEADER_NOT_AVAILABLE === $errorCode) {
                            if ($retryCount >= $produceRetry) {
                                ErrorCode::check($errorCode);
                            }
                            $retryTopics[$name][$index] = true;
                        } else {
                            ErrorCode::check($errorCode);
                        }
                    }
                }
                foreach ($expected as $partitions) {
                    if ($partitions) {
                        throw new \UnexpectedValueException('Missing produce response partition');
                    }
                }
                if (!$retryTopics) {
                    break;
                }
                foreach ($topics as $name => $topic) {
                    $partitions = array_filter($topic->getPartitions(), static function ($partition) use ($retryTopics, $name) {
                        return isset($retryTopics[$name][$partition->getPartitionIndex()]);
                    });
                    if ($partitions) {
                        $topic->setPartitions(array_values($partitions));
                    } else {
                        unset($topics[$name]);
                    }
                }
                if ($produceRetrySleep > 0) {
                    usleep((int) ($produceRetrySleep * 1000000));
                }
            }
        }
    }

    public function close(): void
    {
        $this->broker->close();
    }

    public function getConfig(): ProducerConfig
    {
        return $this->config;
    }

    public function getBroker(): Broker
    {
        return $this->broker;
    }
}
