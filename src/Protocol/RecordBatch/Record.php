<?php

declare(strict_types=1);

namespace longlang\phpkafka\Protocol\RecordBatch;

use longlang\phpkafka\Protocol\AbstractStruct;
use longlang\phpkafka\Protocol\Type\Int8;
use longlang\phpkafka\Protocol\Type\VarInt;
use longlang\phpkafka\Protocol\Type\VarIntCompactArray;

class Record extends AbstractStruct
{
    /**
     * @var int
     */
    protected $length = 0;

    /**
     * @var int
     */
    protected $attributes = 0;

    /**
     * @var int
     */
    protected $timestampDelta = 0;

    /**
     * @var int
     */
    protected $offsetDelta = 0;

    /**
     * @var string|null
     */
    protected $key = null;

    /**
     * @var string|null
     */
    protected $value = null;

    /**
     * @var RecordHeader[]
     */
    protected $headers = [];

    public function __construct()
    {
    }

    public function getFlexibleVersions(): array
    {
        return [];
    }

    public function pack(int $apiVersion = 0): string
    {
        if ($this->timestampDelta < VarInt::MIN_VALUE || $this->timestampDelta > VarInt::MAX_VALUE) {
            throw new \InvalidArgumentException('Timestamp delta exceeds the native signed 32-bit range');
        }
        $data = '';
        $data .= Int8::pack($this->attributes);
        $data .= VarInt::pack($this->timestampDelta);
        $data .= VarInt::pack($this->offsetDelta);
        if (null === $this->key) {
            $data .= VarInt::pack(-1);
        } else {
            $data .= VarInt::pack(\strlen($this->key)) . $this->key;
        }
        if (null === $this->value) {
            $data .= VarInt::pack(-1);
        } else {
            $data .= VarInt::pack(\strlen($this->value)) . $this->value;
        }
        $data .= VarInt::pack(\count($this->headers));
        foreach ($this->headers as $header) {
            $data .= $header->pack($apiVersion);
        }

        $this->length = $length = \strlen($data);

        return VarInt::pack($length) . $data;
    }

    public function unpack(string $data, ?int &$size = null, int $apiVersion = 0): void
    {
        $cursor = 0;
        $this->length = $length = self::readVarInt($data, $cursor);
        if ($length < 6 || $length > strlen($data) - $cursor) {
            throw new \UnexpectedValueException('Invalid record body length');
        }
        $size = $cursor + $length;
        $body = substr($data, $cursor, $length);
        $cursor = 1;
        $this->attributes = Int8::unpack($body);
        // The inherited native VarInt primitive supports signed 32-bit deltas only.
        $this->timestampDelta = self::readVarInt($body, $cursor);
        $this->offsetDelta = self::readVarInt($body, $cursor);
        $this->key = self::readBytes($body, $cursor);
        $this->value = self::readBytes($body, $cursor);
        $count = self::readVarInt($body, $cursor);
        if ($count < 0 || $count > intdiv(strlen($body) - $cursor, 2)) {
            throw new \UnexpectedValueException('Invalid record header count');
        }
        $this->headers = [];
        for ($i = 0; $i < $count; ++$i) {
            $key = self::readBytes($body, $cursor);
            $value = self::readBytes($body, $cursor);
            if ($key === null || $value === null) {
                throw new \UnexpectedValueException('Null record headers are unsupported by the native header API');
            }
            $this->headers[] = (new RecordHeader())->setHeaderKey($key)->setValue($value);
        }
        if ($cursor !== strlen($body)) {
            throw new \UnexpectedValueException('Unexpected trailing record body bytes');
        }
    }

    private static function readVarInt(string $data, int &$cursor): int
    {
        $start = $cursor;
        for ($i = 0; $i < 5; ++$i) {
            if (!isset($data[$cursor])) {
                throw new \UnexpectedValueException('Truncated record varint');
            }
            $byte = ord($data[$cursor++]);
            if ($i === 4 && $byte > 15) {
                throw new \UnexpectedValueException('Record varint exceeds the native 32-bit range');
            }
            if (($byte & 128) === 0) {
                return VarInt::unpack(substr($data, $start, $cursor - $start));
            }
        }
        throw new \UnexpectedValueException('Invalid record varint');
    }

    private static function readBytes(string $data, int &$cursor): ?string
    {
        $length = self::readVarInt($data, $cursor);
        if ($length < -1 || $length > strlen($data) - $cursor) {
            throw new \UnexpectedValueException('Invalid record field length');
        }
        if ($length === -1) {
            return null;
        }
        $value = substr($data, $cursor, $length);
        $cursor += $length;
        return $value;
    }

    public function toArray(): array
    {
        $array = [
            'length'         => $this->length,
            'attributes'     => $this->attributes,
            'timestampDelta' => $this->timestampDelta,
            'offsetDelta'    => $this->offsetDelta,
            'key'            => $this->key,
            'value'          => $this->value,
        ];
        $headers = [];
        foreach ($this->headers as $header) {
            $headers[] = $header->toArray();
        }
        $array['headers'] = $headers;

        return $array;
    }

    public function getLength(): int
    {
        return $this->length;
    }

    public function getAttributes(): int
    {
        return $this->attributes;
    }

    public function setAttributes(int $attributes): self
    {
        $this->attributes = $attributes;

        return $this;
    }

    public function getTimestampDelta(): int
    {
        return $this->timestampDelta;
    }

    public function setTimestampDelta(int $timestampDelta): self
    {
        $this->timestampDelta = $timestampDelta;

        return $this;
    }

    public function getOffsetDelta(): int
    {
        return $this->offsetDelta;
    }

    public function setOffsetDelta(int $offsetDelta): self
    {
        $this->offsetDelta = $offsetDelta;

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
