<?php

declare(strict_types=1);

namespace longlang\phpkafka\Consumer;

use longlang\phpkafka\Protocol\RecordBatch\RecordHeader;

class ConsumeMessage
{
    /**
     * @var Consumer
     */
    protected $consumer;

    /**
     * @var string
     */
    protected $topic;

    /**
     * @var int
     */
    protected $partition;

    /**
     * @var string|null
     */
    protected $key;

    /**
     * @var string|null
     */
    protected $value;

    /**
     * @var RecordHeader[]
     */
    protected $headers;

    /**
     * @param RecordHeader[] $headers
     */
    private readonly Consumer $sourceConsumer;

    private readonly string $sourceTopic;

    private readonly int $sourcePartition;

    public function __construct(
        Consumer $consumer,
        string $topic,
        int $partition,
        ?string $key,
        ?string $value,
        array $headers,
        private readonly ?int $offset = null,
        private readonly ?int $assignmentId = null
    )
    {
        $this->sourceConsumer = $consumer;
        $this->sourceTopic = $topic;
        $this->sourcePartition = $partition;
        $this->consumer = $consumer;
        $this->topic = $topic;
        $this->partition = $partition;
        $this->key = $key;
        $this->value = $value;
        $this->headers = $headers;
    }

    public function isFrom(Consumer $consumer, int $assignmentId): bool
    {
        return $this->sourceConsumer === $consumer && $this->consumer === $consumer
            && $this->sourceTopic === $this->topic && $this->sourcePartition === $this->partition
            && $this->assignmentId === $assignmentId;
    }

    public function getOffset(): ?int
    {
        return $this->offset;
    }

    public function getAssignmentId(): ?int
    {
        return $this->assignmentId;
    }

    public function getConsumer(): Consumer
    {
        return $this->consumer;
    }

    public function setConsumer(Consumer $consumer): self
    {
        $this->consumer = $consumer;

        return $this;
    }

    public function getTopic(): string
    {
        return $this->topic;
    }

    public function setTopic(string $topic): self
    {
        $this->topic = $topic;

        return $this;
    }

    public function getPartition(): int
    {
        return $this->partition;
    }

    public function setPartition(int $partition): self
    {
        $this->partition = $partition;

        return $this;
    }

    public function getKey(): ?string
    {
        return $this->key;
    }

    public function setKey(?string $key): self
    {
        $this->key = $key;

        return $this;
    }

    public function getValue(): ?string
    {
        return $this->value;
    }

    public function setValue(?string $value): self
    {
        $this->value = $value;

        return $this;
    }

    /**
     * @return RecordHeader[]
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }

    /**
     * @param RecordHeader[] $headers
     */
    public function setHeaders(array $headers): self
    {
        $this->headers = $headers;

        return $this;
    }
}
