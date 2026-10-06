<?php

declare(strict_types=1);

namespace longlang\phpkafka\Client;

use InvalidArgumentException;
use longlang\phpkafka\Config\CommonConfig;
use longlang\phpkafka\Exception\SocketException;
use longlang\phpkafka\Protocol\AbstractRequest;
use longlang\phpkafka\Protocol\AbstractResponse;
use longlang\phpkafka\Protocol\KafkaRequest;
use longlang\phpkafka\Protocol\RequestHeader\RequestHeader;
use longlang\phpkafka\Protocol\ResponseHeader\ResponseHeader;
use longlang\phpkafka\Protocol\Type\Int32;
use longlang\phpkafka\Socket\SwooleSocket;
use RuntimeException;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Throwable;

class SwooleClient extends SyncClient
{
    /** @var bool */
    protected $coRecvRunning = false;

    /** @var Channel[] */
    protected $recvChannels = [];

    /** @var int|false */
    private $recvCoId = false;

    /** @var int */
    private $connectionId = 0;

    public function __construct(string $host, int $port, ?CommonConfig $config = null, string $socketClass = SwooleSocket::class)
    {
        parent::__construct($host, $port, $config, $socketClass);
    }

    public function connect(): void
    {
        // SwooleSocket reuses a mutable native socket. The old reader must exit
        // before parent::connect can replace it, including reconnect in a callback.
        $this->close();
        if ($this->recvCoId !== false && Coroutine::exists($this->recvCoId)) {
            throw new RuntimeException('Previous Kafka receiver has not stopped');
        }
        parent::connect();
    }

    public function close(): bool
    {
        $recvCoId = $this->recvCoId;
        ++$this->connectionId;
        $this->coRecvRunning = false;
        $channels = $this->recvChannels;
        $this->recvChannels = [];
        // Clear registrations before socket/channel close can resume a waiter.
        $this->waitResponseMaps = [];
        try {
            return parent::close();
        } finally {
            foreach ($channels as $channel) {
                $channel->close();
            }
            if ($recvCoId !== false && $recvCoId !== Coroutine::getCid() && Coroutine::exists($recvCoId)) {
                if (Coroutine::getCid() < 0) {
                    throw new RuntimeException('Kafka receiver must be joined inside a coroutine');
                }
                // Native join timeout is seconds. A failed stop never permits reuse.
                Coroutine::join([$recvCoId], 1.0);
                if (Coroutine::exists($recvCoId)) {
                    throw new RuntimeException('Kafka receiver did not stop within one second');
                }
            }
        }
    }

    public function send(AbstractRequest $request, ?RequestHeader $header = null, bool $hasResponse = true): int
    {
        $apiKey = $request->getRequestApiKey();
        if (null === $header) {
            $header = new RequestHeader();
            $header->setRequestApiKey($apiKey);
            $header->setRequestApiVersion($this->getRequestApiVersion($request));
            $header->setClientId($this->getConfig()->getClientId());
            $header->setCorrelationId(++$this->correlationIdIncrValue);
        }
        $data = (new KafkaRequest($request, $header))->pack();
        $correlationId = $header->getCorrelationId();
        if (isset($this->waitResponseMaps[$correlationId])) {
            throw new InvalidArgumentException('Correlation ID is already awaiting a response');
        }
        $connectionId = $this->connectionId;
        if ($hasResponse) {
            $this->recvChannels[$correlationId] = new Channel(1);
            $this->waitResponseMaps[$correlationId] = [
                'apiKey'           => $apiKey,
                'apiVersion'       => $header->getRequestApiVersion(),
                'flexibleVersions' => $request->getFlexibleVersions(),
            ];
        }
        try {
            if ($this->socket->send($data) !== \strlen($data)) {
                throw new SocketException('Incomplete Kafka request write');
            }
            if ($connectionId !== $this->connectionId) {
                throw new SocketException('Kafka connection changed during send');
            }
        } catch (Throwable $exception) {
            if ($connectionId === $this->connectionId) {
                $this->close();
            }
            throw $exception;
        }

        return $correlationId;
    }

    public function recv(?int $correlationId, ?ResponseHeader &$header = null): AbstractResponse
    {
        if (!isset($this->waitResponseMaps[$correlationId], $this->recvChannels[$correlationId])) {
            throw new InvalidArgumentException(sprintf('Invalid correlationId %s', $correlationId));
        }
        $connectionId = $this->connectionId;
        $mapData = $this->waitResponseMaps[$correlationId];
        $channel = $this->recvChannels[$correlationId];
        try {
            if ($this->recvCoId === false || !Coroutine::exists($this->recvCoId)) {
                $this->startRecvCo();
            }
            $data = $channel->pop($this->getConfig()->getRecvTimeout());
            if ($data instanceof Throwable) {
                throw $data;
            }
            if (false === $data || $connectionId !== $this->connectionId) {
                throw new SocketException('Kafka response unavailable or connection closed');
            }

            return $this->decodeResponse($data, $correlationId, $mapData, $header);
        } catch (Throwable $exception) {
            if ($connectionId === $this->connectionId) {
                $this->close();
            }
            throw $exception;
        } finally {
            if ($connectionId === $this->connectionId) {
                unset($this->recvChannels[$correlationId], $this->waitResponseMaps[$correlationId]);
            }
            $channel->close();
        }
    }

    private function startRecvCo(): void
    {
        $this->coRecvRunning = true;
        $connectionId = $this->connectionId;
        $id = Coroutine::create(function () use ($connectionId): void {
            $this->recvCoId = Coroutine::getCid();
            try {
                while ($this->coRecvRunning && $connectionId === $this->connectionId) {
                    $data = $this->readResponseFrame(-1);
                    if ($connectionId !== $this->connectionId) {
                        return;
                    }
                    $correlationId = Int32::unpack($data);
                    $channel = $this->recvChannels[$correlationId] ?? null;
                    if ($channel === null || $channel->isFull() || !$channel->push($data, 0.001)) {
                        throw new SocketException('Unexpected or duplicate Kafka response');
                    }
                }
            } catch (Throwable $exception) {
                if ($connectionId !== $this->connectionId) {
                    return;
                }
                // Wake current waiters with the native error when their channel is
                // empty. Closing first also makes callback throws/reconnect safe.
                foreach ($this->recvChannels as $channel) {
                    if ($channel->isEmpty()) {
                        $channel->push($exception, 0.001);
                    }
                }
                $this->close();
                $callback = $this->getConfig()->getExceptionCallback();
                if ($callback) {
                    $callback($exception);
                }
            } finally {
                if ($this->recvCoId === Coroutine::getCid()) {
                    $this->recvCoId = false;
                }
            }
        });
        if ($id === false) {
            $this->coRecvRunning = false;
            throw new RuntimeException('Unable to start Kafka receiver');
        }
    }
}
