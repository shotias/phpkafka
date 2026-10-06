<?php

declare(strict_types=1);

namespace longlang\phpkafka\Client;

use InvalidArgumentException;
use longlang\phpkafka\Config\CommonConfig;
use longlang\phpkafka\Exception\UnsupportedApiKeyException;
use longlang\phpkafka\Exception\SocketException;
use Throwable;
use longlang\phpkafka\Exception\UnsupportedApiVersionException;
use longlang\phpkafka\Protocol\AbstractRequest;
use longlang\phpkafka\Protocol\AbstractResponse;
use longlang\phpkafka\Protocol\ApiKeys;
use longlang\phpkafka\Protocol\ApiVersions\ApiVersionsRequest;
use longlang\phpkafka\Protocol\ApiVersions\ApiVersionsResponse;
use longlang\phpkafka\Protocol\ApiVersions\ApiVersionsResponseKey;
use longlang\phpkafka\Protocol\ErrorCode;
use longlang\phpkafka\Protocol\KafkaRequest;
use longlang\phpkafka\Protocol\RequestHeader\RequestHeader;
use longlang\phpkafka\Protocol\ResponseHeader\ResponseHeader;
use longlang\phpkafka\Protocol\SaslAuthenticate\SaslAuthenticateRequest;
use longlang\phpkafka\Protocol\SaslAuthenticate\SaslAuthenticateResponse;
use longlang\phpkafka\Protocol\SaslHandshake\SaslHandshakeRequest;
use longlang\phpkafka\Protocol\SaslHandshake\SaslHandshakeResponse;
use longlang\phpkafka\Protocol\Type\Int32;
use longlang\phpkafka\Sasl\SaslInterface;
use longlang\phpkafka\Socket\SocketInterface;
use longlang\phpkafka\Socket\StreamSocket;

class SyncClient implements ClientInterface
{
    /**
     * @var SocketInterface
     */
    protected $socket;

    /**
     * @var ApiVersionsResponseKey[]
     */
    protected $apiKeys;

    /**
     * @var array
     */
    protected $waitResponseMaps = [];

    /**
     * @var int
     */
    protected $correlationIdIncrValue = 0;

    public function __construct(string $host, int $port, ?CommonConfig $config = null, string $socketClass = StreamSocket::class)
    {
        $this->socket = new $socketClass($host, $port, $config);
    }

    public function getHost(): string
    {
        return $this->socket->getHost();
    }

    public function getPort(): int
    {
        return $this->socket->getPort();
    }

    public function getConfig(): CommonConfig
    {
        return $this->socket->getConfig();
    }

    public function getSocket(): SocketInterface
    {
        return $this->socket;
    }

    /**
     * @param ApiVersionsResponseKey[] $apiKeys
     */
    public function setApiKeys(array $apiKeys): ClientInterface
    {
        $result = [];
        foreach ($apiKeys as $item) {
            $result[$item->getApiKey()] = $item;
        }
        $this->apiKeys = $result;

        return $this;
    }

    /**
     * @return ApiVersionsResponseKey[]|null
     */
    public function getApiKeys(): ?array
    {
        return $this->apiKeys;
    }

    public function connect(): void
    {
        $this->close();
        try {
            $this->socket->connect();
            $this->updateApiVersions();
            $this->sendAuthInfo();
        } catch (Throwable $exception) {
            try {
                $this->close();
            } catch (Throwable $cleanupException) {
                // Cleanup must not replace the original transport/protocol failure.
            }
            throw $exception;
        }
    }

    public function close(): bool
    {
        $this->waitResponseMaps = [];

        return $this->socket->close();
    }

    public function getRequestApiVersion(AbstractRequest $request): int
    {
        $apiKey = $request->getRequestApiKey();
        $apiKeyConfig = $this->apiKeys[$apiKey] ?? null;
        if (!$apiKeyConfig) {
            if (ApiKeys::PROTOCOL_API_VERSIONS === $apiKey) {
                return 1;
            }
            throw new UnsupportedApiKeyException(sprintf('Unsupported apikey %s', $apiKey));
        }
        $requestMaxSupportedVersion = $request->getMaxSupportedVersion();
        if ($apiKeyConfig->getMinVersion() > $request->getMaxSupportedVersion()) {
            throw new UnsupportedApiVersionException(sprintf('The version of Api %s must be >= v%s and <= v%s, the latest version currently supported is v%s', $request->getApiKeyText(), $apiKeyConfig->getMinVersion(), $apiKeyConfig->getMaxVersion(), $request->getMaxSupportedVersion()));
        }
        $apiKeyMaxVersion = $apiKeyConfig->getMaxVersion();
        if ($requestMaxSupportedVersion <= $apiKeyMaxVersion) {
            return $requestMaxSupportedVersion;
        } else {
            return $apiKeyMaxVersion;
        }
    }

    /**
     * Send message to kafka server.
     *
     * If successful, return the correlationId
     */
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
        $kafkaRequest = new KafkaRequest($request, $header);
        $data = $kafkaRequest->pack();
        $correlationId = $header->getCorrelationId();
        if (isset($this->waitResponseMaps[$correlationId])) {
            throw new InvalidArgumentException('Correlation ID is already awaiting a response');
        }

        if ($hasResponse) {
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
        } catch (Throwable $exception) {
            try {
                $this->close();
            } catch (Throwable $cleanupException) {
                // Cleanup must not replace the original transport/protocol failure.
            }
            throw $exception;
        }

        return $correlationId;
    }

    public function recv(?int $correlationId, ?ResponseHeader &$header = null): AbstractResponse
    {
        if (!isset($this->waitResponseMaps[$correlationId])) {
            throw new InvalidArgumentException(sprintf('Invalid correlationId %s', $correlationId));
        }
        $mapData = $this->waitResponseMaps[$correlationId];
        try {
            return $this->decodeResponse($this->readResponseFrame(), $correlationId, $mapData, $header);
        } catch (Throwable $exception) {
            // No dispatcher in the synchronous client: an unmatched/partial frame
            // makes the entire connection unsafe to reuse.
            try {
                $this->close();
            } catch (Throwable $cleanupException) {
                // Cleanup must not replace the original transport/protocol failure.
            }
            throw $exception;
        } finally {
            unset($this->waitResponseMaps[$correlationId]);
        }
    }

    protected function readResponseFrame(?float $timeout = null): string
    {
        $data = $this->socket->recv(4, $timeout);
        if (4 !== \strlen($data)) {
            throw new SocketException('Incomplete Kafka response length');
        }
        $length = Int32::unpack($data);
        if ($length < 4 || $length > StreamSocket::READ_MAX_LENGTH) {
            throw new SocketException('Invalid Kafka response length');
        }
        $data = $this->socket->recv($length);
        if (\strlen($data) !== $length) {
            throw new SocketException('Incomplete Kafka response frame');
        }

        return $data;
    }

    protected function decodeResponse(string $data, int $correlationId, array $mapData, ?ResponseHeader &$header): AbstractResponse
    {
        $header = new ResponseHeader();
        $header->unpack($data, $size, ResponseHeader::parseVersion($mapData['apiVersion'], $mapData['flexibleVersions']));
        if ($header->getCorrelationId() !== $correlationId) {
            throw new SocketException('Unexpected Kafka response correlation ID');
        }

        $body = substr($data, $size);
        $result = ApiKeys::createResponse($mapData['apiKey']);
        $result->unpack($body, $bodySize, $mapData['apiVersion']);
        if ($bodySize !== \strlen($body)) {
            throw new SocketException('Incomplete or trailing Kafka response body');
        }

        return $result;
    }

    public function sendRecv(AbstractRequest $request, ?RequestHeader $requestHeader = null, ?ResponseHeader &$responseHeader = null): AbstractResponse
    {
        $correlationId = $this->send($request, $requestHeader);

        return $this->recv($correlationId, $responseHeader);
    }

    protected function updateApiVersions(): void
    {
        $request = new ApiVersionsRequest();
        $correlationId = $this->send($request);
        /** @var ApiVersionsResponse $response */
        $response = $this->recv($correlationId);
        ErrorCode::check($response->getErrorCode());
        $this->setApiKeys($response->getApiKeys());
    }

    protected function sendAuthInfo(): void
    {
        $config = $this->getConfig()->getSasl();
        if (!isset($config['type']) || empty($config['type'])) {
            return;
        }
        $class = new $config['type']($this->getConfig());
        if (!$class instanceof SaslInterface) {
            return;
        }
        $handshakeRequest = new SaslHandshakeRequest();
        $handshakeRequest->setMechanism($class->getName());
        $correlationId = $this->send($handshakeRequest);
        /** @var SaslHandshakeResponse $handshakeResponse */
        $handshakeResponse = $this->recv($correlationId);
        ErrorCode::check($handshakeResponse->getErrorCode());

        $authenticateRequest = new SaslAuthenticateRequest();
        $authenticateRequest->setAuthBytes($class->getAuthBytes());
        $correlationId = $this->send($authenticateRequest);
        /** @var SaslAuthenticateResponse $authenticateResponse */
        $authenticateResponse = $this->recv($correlationId);
        ErrorCode::check($authenticateResponse->getErrorCode());
    }
}
