<?php
declare(strict_types=1);

// Actual Swoole runtime and loopback TCP peer; scripted wire responses are not Kafka acceptance.
if (!extension_loaded('swoole')) {
    fwrite(STDERR, "Swoole extension is required; this check must not be skipped.\n");
    exit(1);
}
use longlang\phpkafka\Client\SwooleClient;
use longlang\phpkafka\Client\SyncClient;
use longlang\phpkafka\Config\CommonConfig;
use longlang\phpkafka\Exception\SocketException;
use longlang\phpkafka\Protocol\ApiVersions\ApiVersionsRequest;
use longlang\phpkafka\Protocol\ApiVersions\ApiVersionsResponse;
use longlang\phpkafka\Protocol\RequestHeader\RequestHeader;
use longlang\phpkafka\Protocol\Type\Int32;
use longlang\phpkafka\Socket\SwooleSocket;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Socket;

spl_autoload_register(static function (string $class): void {
    $prefix = 'longlang\\phpkafka\\';
    if (str_starts_with($class, $prefix)) {
        require dirname(__DIR__) . '/src/' . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';
    }
});
set_error_handler(static function (int $level, string $message): never { throw new ErrorException($message, 0, $level); });
$checks = 0;
function verify(bool $condition, string $label): void {
    global $checks;
    if (!$condition) { throw new RuntimeException($label); }
    ++$checks;
}
function refuses(callable $call, string $label, string $class = Throwable::class): Throwable {
    try { $call(); } catch (Throwable $exception) {
        verify($exception instanceof $class, $label . ': unexpected ' . $exception::class);
        return $exception;
    }
    throw new RuntimeException($label . ': unexpectedly succeeded');
}
function field(object $object, string $name): mixed {
    return (new ReflectionProperty($object, $name))->getValue($object);
}
function clean(SwooleClient $client, string $label): void {
    verify(field($client, 'waitResponseMaps') === [] && field($client, 'recvChannels') === [], $label . ' maps/channels empty');
}
function stopped(SwooleClient $client, string $label): void {
    $id = field($client, 'recvCoId');
    verify($id === false || !Coroutine::exists($id), $label . ' native receiver stopped');
}
function readId(Socket $socket): int {
    $prefix = $socket->recvAll(4, 1);
    if (!is_string($prefix) || strlen($prefix) !== 4) { throw new RuntimeException('Peer missing request length'); }
    $length = Int32::unpack($prefix);
    if ($length < 8 || $length > 5242880) { throw new RuntimeException('Peer invalid request length'); }
    $body = $socket->recvAll($length, 1);
    if (!is_string($body) || strlen($body) !== $length) { throw new RuntimeException('Peer incomplete request'); }
    return Int32::unpack(substr($body, 4, 4));
}
function response(int $id): string { return Int32::pack($id) . (new ApiVersionsResponse())->pack(1); }
function writeFrame(Socket $socket, string $body): void {
    $wire = Int32::pack(strlen($body)) . $body;
    if ($socket->sendAll($wire, 1) !== strlen($wire)) { throw new RuntimeException('Peer incomplete response write'); }
}
function handshake(Socket $socket): void { writeFrame($socket, response(readId($socket))); }
function peer(callable $script, callable $exercise): void {
    $listener = new Socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    verify($listener->bind('127.0.0.1', 0) && $listener->listen(), 'Native loopback listener opens');
    $port = $listener->getsockname()['port'];
    $done = new Channel(1);
    $finish = new Channel(1);
    $id = Coroutine::create(static function () use ($listener, $script, $done, $finish): void {
        $socket = null;
        try {
            $socket = $listener->accept(1);
            if (!$socket instanceof Socket) { throw new RuntimeException('Peer accept failed'); }
            $script($socket, $finish);
            $done->push(true);
        } catch (Throwable $exception) {
            $done->push($exception);
        } finally {
            if ($socket instanceof Socket) { $socket->close(); }
            $listener->close();
        }
    });
    $config = (new CommonConfig())->setConnectTimeout(0.2)->setSendTimeout(0.2)->setRecvTimeout(0.08);
    $client = new SwooleClient('127.0.0.1', $port, $config);
    try {
        $exercise($client, $config);
    } finally {
        $client->close();
        $finish->push(true, 0.01);
        if (Coroutine::exists($id)) { Coroutine::join([$id], 1); }
    }
    $result = $done->pop(0.1);
    if ($result instanceof Throwable) { throw $result; }
    verify($result === true && !Coroutine::exists($id), 'Native peer is joined');
    clean($client, 'Final close');
    stopped($client, 'Final close');
}
final class FailingPack extends ApiVersionsRequest {
    public function pack(int $apiVersion = 0): string { throw new Error('pack control'); }
}
// A fault-injection socket still exercises the real Swoole coroutine/channel engine.
// It is used only for write failures which cannot be scheduled reliably over loopback.
final class FaultSocket extends SwooleSocket {
    public mixed $writeControl = null;
    public bool $open = true;
    public int $connects = 0;
    public ?Throwable $closeFailure = null;
    public Channel $input;
    public function __construct(string $host, int $port, ?CommonConfig $config = null) {
        parent::__construct($host, $port, $config); $this->input = new Channel(16);
    }
    public function connect(): void { ++$this->connects; $this->open = true; $this->input = new Channel(16); }
    public function close(): bool {
        $this->open = false; $this->input->close();
        if ($this->closeFailure !== null) { throw $this->closeFailure; }
        return true;
    }
    public function isConnected(): bool { return $this->open; }
    public function send(string $data, ?float $timeout = null): int {
        if ($this->writeControl instanceof Closure) { return ($this->writeControl)($data); }
        if ($this->writeControl instanceof Throwable) { throw $this->writeControl; }
        return $this->writeControl ?? strlen($data);
    }
    public function recv(int $length, ?float $timeout = null): string {
        $value = $this->input->pop($timeout ?? 0.08);
        if ($value instanceof Throwable) { throw $value; }
        if (!is_string($value)) { throw new SocketException('Fault socket closed or timed out'); }
        return $value;
    }
}
function explicitHeader(int $id): RequestHeader {
    return (new RequestHeader())->setRequestApiKey(18)->setRequestApiVersion(1)->setCorrelationId($id);
}
// Weak references observe constructor ownership without keeping a client alive.
final class ConstructorObservedClient extends SwooleClient {
    public static array $weak = [];
    public function __construct(string $host, int $port, ?CommonConfig $config = null, string $socketClass = SwooleSocket::class) {
        parent::__construct($host, $port, $config, $socketClass);
        self::$weak[] = WeakReference::create($this);
    }
}
Coroutine\run(static function (): void {
    $baseline = Coroutine::stats()['coroutine_num'];
    peer(static function (Socket $socket, Channel $finish): void {
        handshake($socket);
        for ($i = 0; $i < 100; ++$i) { readId($socket); }
        $first = readId($socket); $second = readId($socket);
        writeFrame($socket, response($second)); writeFrame($socket, response($first));
        $finish->pop(1);
    }, static function (SwooleClient $client): void {
        $client->connect();
        clean($client, 'Handshake');
        for ($i = 0; $i < 100; ++$i) { $client->send(new ApiVersionsRequest(), null, false); }
        clean($client, 'One hundred no-response sends');
        refuses(fn () => $client->send(new FailingPack()), 'Packing error', Error::class);
        clean($client, 'Packing error');
        $first = $client->send(new ApiVersionsRequest()); $second = $client->send(new ApiVersionsRequest());
        verify($client->recv($first) instanceof ApiVersionsResponse, 'Multiplexed first response matches');
        verify($client->recv($second) instanceof ApiVersionsResponse, 'Multiplexed second response matches');
        clean($client, 'Multiplexed responses');
    });
    foreach (['timeout', 'eof', 'unknown', 'empty-body', 'trailing-body', 'zero', 'negative', 'oversize', 'duplicate'] as $failure) {
        peer(static function (Socket $socket, Channel $finish) use ($failure): void {
            handshake($socket); $id = readId($socket);
            if ($failure === 'timeout') { Coroutine::sleep(0.15); return; }
            if ($failure === 'eof') { return; }
            if ($failure === 'duplicate') {
                $other = readId($socket);
                writeFrame($socket, response($other)); writeFrame($socket, response($other));
            } elseif (in_array($failure, ['zero', 'negative', 'oversize'], true)) {
                $length = ['zero' => 0, 'negative' => -1, 'oversize' => 5242881][$failure];
                $socket->sendAll(Int32::pack($length), 1);
            } else {
                $body = match ($failure) {
                    'unknown' => response($id + 100),
                    'empty-body' => Int32::pack($id),
                    'trailing-body' => response($id) . "\0",
                };
                writeFrame($socket, $body);
            }
            $finish->pop(1);
        }, static function (SwooleClient $client, CommonConfig $config) use ($failure): void {
            $observed = 0;
            $config->setExceptionCallback(static function (Throwable $exception) use ($client, &$observed): void {
                ++$observed;
                clean($client, 'Failure callback');
                verify(!$client->getSocket()->isConnected(), 'Failure callback sees closed socket');
                refuses(fn () => $client->connect(), 'Receiver callback cannot reconnect before own exit', RuntimeException::class);
            });
            $client->connect();
            $id = $client->send(new ApiVersionsRequest());
            if ($failure === 'duplicate') { $client->send(new ApiVersionsRequest()); }
            $start = microtime(true);
            refuses(fn () => $client->recv($id), 'Failure ' . $failure);
            verify(microtime(true) - $start < 0.8, 'Failure is bounded: ' . $failure);
            $client->close();
            clean($client, $failure);
            stopped($client, $failure);
            if (in_array($failure, ['eof', 'unknown', 'zero', 'negative', 'oversize', 'duplicate'], true)) {
                verify($observed === 1, 'Receiver failure observed once: ' . $failure);
            }
        });
    }
    foreach ([new Error('write control'), 0, 1] as $failure) {
        $client = new SwooleClient('unused', 1, (new CommonConfig())->setRecvTimeout(0.08), FaultSocket::class);
        $client->getSocket()->writeControl = $failure;
        refuses(fn () => $client->send(new ApiVersionsRequest()), 'Swoole failed/short write');
        clean($client, 'Swoole failed/short write');
        verify(!$client->getSocket()->isConnected(), 'Swoole failed/short write closes socket');
    }
    foreach (['send', 'recv', 'receiver'] as $stage) {
        $callbackError = null;
        $config = (new CommonConfig())->setRecvTimeout(0.08);
        $config->setExceptionCallback(static function (Throwable $error) use (&$callbackError): void { $callbackError = $error; });
        $client = new SwooleClient('unused', 1, $config, FaultSocket::class);
        $socket = $client->getSocket();
        $primary = new Error('Swoole primary ' . $stage);
        $socket->closeFailure = new LogicException('Swoole cleanup');
        if ($stage === 'send') {
            $socket->writeControl = $primary;
            $call = fn () => $client->send(new ApiVersionsRequest());
        } else {
            $id = $client->send(new ApiVersionsRequest());
            if ($stage === 'recv') {
                field($client, 'recvChannels')[$id]->push($primary);
            } else {
                $socket->input->push($primary);
            }
            $call = fn () => $client->recv($id);
        }
        verify(refuses($call, 'Swoole nested failure ' . $stage, Error::class) === $primary, 'Swoole original error object ' . $stage);
        clean($client, 'Swoole nested cleanup ' . $stage);
        stopped($client, 'Swoole nested cleanup ' . $stage);
        verify(!$socket->isConnected(), 'Swoole nested cleanup disconnects ' . $stage);
        if ($stage === 'receiver') { verify($callbackError === $primary, 'Receiver callback receives original error despite cleanup failure'); }
        verify(refuses(fn () => $client->close(), 'Explicit Swoole close failure', LogicException::class) === $socket->closeFailure, 'Explicit Swoole cleanup remains visible');
        $socket->closeFailure = null; $client->close();
    }
    // Suspend an old-generation send, close/reconnect and reuse the public ID.
    // Its late failure must not erase the new registration or close the new socket.
    $client = new SwooleClient('unused', 1, (new CommonConfig())->setRecvTimeout(0.08), FaultSocket::class);
    $socket = $client->getSocket(); $release = new Channel(1); $done = new Channel(1);
    $socket->writeControl = static function (string $data) use ($release): int { $release->pop(1); throw new Error('old send failure'); };
    $old = Coroutine::create(static function () use ($client, $done): void {
        try { $client->send(new ApiVersionsRequest(), explicitHeader(77)); $done->push(false); }
        catch (Error $exception) { $done->push($exception->getMessage() === 'old send failure'); }
    });
    $client->close();
    $socket->writeControl = static function (string $data) use ($socket): int {
        $id = Int32::unpack(substr($data, 8, 4)); $body = response($id);
        $socket->input->push(Int32::pack(strlen($body))); $socket->input->push($body);
        return strlen($data);
    };
    $client->connect();
    $client->send(new ApiVersionsRequest(), explicitHeader(77));
    $release->push(true);
    verify($done->pop(1) === true, 'Suspended old send still reports original failure');
    if (Coroutine::exists($old)) { Coroutine::join([$old], 1); }
    verify($client->getSocket()->isConnected(), 'Old failure leaves new socket open');
    verify($client->recv(77) instanceof ApiVersionsResponse, 'Old failure preserves reused-ID response');
    $client->close(); clean($client, 'Generation control'); stopped($client, 'Generation control');

    // Close wakes every concurrent waiter and joins the one native reader.
    $client = new SwooleClient('unused', 1, (new CommonConfig())->setRecvTimeout(0.1), FaultSocket::class);
    $ids = [$client->send(new ApiVersionsRequest()), $client->send(new ApiVersionsRequest())];
    $done = new Channel(2); $waiters = [];
    foreach ($ids as $id) {
        $waiters[] = Coroutine::create(static function () use ($client, $id, $done): void {
            try { $client->recv($id); $done->push(false); } catch (SocketException $exception) { $done->push(true); }
        });
    }
    $client->close();
    verify($done->pop(1) === true && $done->pop(1) === true, 'Explicit close fails all waiting receives');
    foreach ($waiters as $id) { if (Coroutine::exists($id)) { Coroutine::join([$id], 1); } }
    clean($client, 'Concurrent close'); stopped($client, 'Concurrent close');

    // Same native SwooleSocket wrapper reconnects after an actual TCP timeout.
    $listener = new Socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    verify($listener->bind('127.0.0.1', 0) && $listener->listen(), 'Reconnect listener opens');
    $port = $listener->getsockname()['port']; $done = new Channel(1); $finish = new Channel(1);
    $server = Coroutine::create(static function () use ($listener, $done, $finish): void {
        $socket = null;
        try {
            $socket = $listener->accept(1);
            if (!$socket instanceof Socket) { throw new RuntimeException('First reconnect accept failed'); }
            handshake($socket); readId($socket);
            Coroutine::sleep(0.12); // First connection is closed by the receive timeout.
            $socket->close();
            $socket = $listener->accept(1);
            if (!$socket instanceof Socket) { throw new RuntimeException('Second reconnect accept failed'); }
            handshake($socket); writeFrame($socket, response(readId($socket)));
            $finish->pop(1); $done->push(true);
        } catch (Throwable $exception) { $done->push($exception); }
        finally { if ($socket instanceof Socket) { $socket->close(); } $listener->close(); }
    });
    $config = (new CommonConfig())->setConnectTimeout(0.2)->setSendTimeout(0.2)->setRecvTimeout(0.08);
    $client = new SwooleClient('127.0.0.1', $port, $config);
    try {
        $client->connect(); $client->send(new ApiVersionsRequest(), explicitHeader(77));
        refuses(fn () => $client->recv(77), 'Native socket first generation times out');
        clean($client, 'Native timeout'); stopped($client, 'Native timeout');
        $client->connect();
        $client->send(new ApiVersionsRequest(), explicitHeader(77));
        verify($client->recv(77) instanceof ApiVersionsResponse, 'Native reconnect receives reused-ID reply on fresh socket');
    } finally {
        $client->close(); $finish->push(true);
        if (Coroutine::exists($server)) { Coroutine::join([$server], 1); }
    }
    $result = $done->pop(0.1);
    if ($result instanceof Throwable) { throw $result; }
    verify($result === true && !Coroutine::exists($server), 'Native reconnect peer joined');
    clean($client, 'Native reconnect'); stopped($client, 'Native reconnect');

    // An exception callback that deliberately stalls keeps the old reader alive.
    // Native bounded join must reject reuse rather than replacing its socket.
    $release = new Channel(1); $entered = new Channel(1); $done = new Channel(1);
    $config = (new CommonConfig())->setRecvTimeout(0.08);
    $client = new SwooleClient('unused', 1, $config, FaultSocket::class);
    $config->setExceptionCallback(static function (Throwable $exception) use ($release, $entered): void {
        $entered->push(true); $release->pop(4);
    });
    $client->getSocket()->input->push(new Error('receiver Throwable control'));
    $id = $client->send(new ApiVersionsRequest());
    $waiter = Coroutine::create(static function () use ($client, $id, $done): void {
        try { $client->recv($id); $done->push(false); } catch (Throwable $exception) { $done->push(true); }
    });
    verify($entered->pop(1) === true, 'Native Error reaches callback after connection cleanup');
    $start = microtime(true);
    refuses(fn () => $client->connect(), 'Live stalled receiver prevents reconnect', RuntimeException::class);
    verify(microtime(true) - $start < 1.5 && $client->getSocket()->connects === 0, 'Stop timeout is bounded and never opens replacement socket');
    clean($client, 'Stalled receiver');
    $release->push(true);
    $client->close();
    verify($done->pop(1) === true, 'Stalled receiver waiting request fails');
    if (Coroutine::exists($waiter)) { Coroutine::join([$waiter], 1); }
    stopped($client, 'Released stalled receiver');

    foreach (['send', 'recv'] as $operation) {
        // Exercise the actual mutable wrapper and native IO, not FaultSocket.
        // Direct wrapper reconnect while IO is suspended tests its ownership guard.
        $listener = new Socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
        verify($listener->bind('127.0.0.1', 0) && $listener->listen(), 'Native ownership listener opens');
        $port = $listener->getsockname()['port'];
        $ready = new Channel(1); $late = new Channel(1); $finish = new Channel(1); $serverDone = new Channel(1);
        $server = Coroutine::create(static function () use ($listener, $ready, $late, $finish, $serverDone, $operation): void {
            $first = $second = null;
            try {
                $first = $listener->accept(1);
                if (!$first instanceof Socket) { throw new RuntimeException('Old socket accept failed'); }
                if (!$first->setOption(SOL_SOCKET, SO_RCVBUF, 4096)) { throw new RuntimeException('Backpressure receive buffer setup failed'); }
                $ready->push(true);
                $second = $listener->accept(1);
                if (!$second instanceof Socket || $second->sendAll('AB', 1) !== 2) { throw new RuntimeException('New socket setup failed'); }
                if ($operation === 'recv') {
                    if ($late->pop(1) !== true || $first->sendAll('X', 1) !== 1) { throw new RuntimeException('Old receive response control failed'); }
                }
                $finish->pop(1);
                $serverDone->push(true);
            } catch (Throwable $exception) { $serverDone->push($exception); }
            finally {
                if ($first instanceof Socket) { $first->close(); }
                if ($second instanceof Socket) { $second->close(); }
                $listener->close();
            }
        });
        $config = (new CommonConfig())->setConnectTimeout(0.3)->setSendTimeout(0.2)->setRecvTimeout(0.4);
        $wrapper = new SwooleSocket('127.0.0.1', $port, $config);
        $old = null; $sender = null; $done = new Channel(1);
        try {
            $wrapper->connect();
            verify($ready->pop(1) === true, 'Old native connection accepted');
            $old = field($wrapper, 'socket');
            if ($operation === 'send') {
                $size = 16 * 1024 * 1024;
                $written = $wrapper->send(str_repeat('x', $size));
                verify($written > 0 && $written < $size, 'Native partial write fills the blocked connection before stale-send control');
            }
            $sender = Coroutine::create(static function () use ($wrapper, $done, $operation): void {
                try {
                    if ($operation === 'send') { $wrapper->send(str_repeat('x', 16 * 1024 * 1024)); }
                    else { $wrapper->recv(1); }
                    $done->push(false);
                } catch (SocketException $exception) { $done->push($exception); }
            });
            Coroutine::sleep(0.005);
            verify(Coroutine::exists($sender) && $done->isEmpty(), 'Actual native ' . $operation . ' is suspended by peer');
            $wrapper->connect();
            verify(field($wrapper, 'socket') !== $old && $wrapper->recv(1) === 'A', 'Replacement native connection receives its first byte');
            verify(field($wrapper, 'receivedBuffer') === 'B', 'Replacement buffer holds only its own second byte');
            if ($operation === 'recv') { $late->push(true); }
            $result = $done->pop(0.8);
            if (Coroutine::exists($sender)) { Coroutine::join([$sender], 1); }
            verify($wrapper->isConnected() && $wrapper->recv(1) === 'B', 'Old native ' . $operation . ' leaves replacement socket and buffer intact');
            verify($result instanceof SocketException && str_contains($result->getMessage(), 'Socket changed'), 'Old native ' . $operation . ' detects replaced socket');
            verify(!$old->isConnected(), 'Superseded native socket is closed');
        } finally {
            if ($old !== null) { $old->close(); }
            $wrapper->close(); $finish->push(true);
            if ($sender !== null && Coroutine::exists($sender)) { Coroutine::join([$sender], 1); }
            if (Coroutine::exists($server)) { Coroutine::join([$server], 1); }
        }
        $result = $serverDone->pop(0.1);
        if ($result instanceof Throwable) { throw $result; }
        verify($result === true && !Coroutine::exists($server), 'Native ownership peer is joined');
    }
    // Native constructor, multiplexed client and TCP peer; only wire replies are scripted.
    ConstructorObservedClient::$weak = [];
    peer(static function (Socket $socket, Channel $finish): void {
        $versions = (new ApiVersionsResponse())->setApiKeys([
            (new \longlang\phpkafka\Protocol\ApiVersions\ApiVersionsResponseKey())->setApiKey(3)->setMinVersion(0)->setMaxVersion(0),
            (new \longlang\phpkafka\Protocol\ApiVersions\ApiVersionsResponseKey())->setApiKey(10)->setMinVersion(0)->setMaxVersion(0),
        ]);
        writeFrame($socket, Int32::pack(readId($socket)) . $versions->pack(1));
        $metadata = (new \longlang\phpkafka\Protocol\Metadata\MetadataResponse())->setTopics([
            (new \longlang\phpkafka\Protocol\Metadata\MetadataResponseTopic())->setName('constructor-probe'),
        ]);
        writeFrame($socket, Int32::pack(readId($socket)) . $metadata->pack(0));
        for ($attempt = 0; $attempt < 2; ++$attempt) {
            $failure = (new \longlang\phpkafka\Protocol\FindCoordinator\FindCoordinatorResponse())->setErrorCode(15);
            writeFrame($socket, Int32::pack(readId($socket)) . $failure->pack(0));
        }
        $finish->pop(1);
    }, static function (SwooleClient $unused): void {
        $config = new \longlang\phpkafka\Consumer\ConsumerConfig();
        $config->setClient(ConstructorObservedClient::class); $config->setSocket(SwooleSocket::class);
        $config->setTimer(\longlang\phpkafka\Timer\SwooleTimer::class); $config->setUpdateBrokers(false);
        $config->setBroker([1 => 'tcp://' . $unused->getHost() . ':' . $unused->getPort()]);
        $config->setTopic('constructor-probe'); $config->setGroupId('constructor-probe'); $config->setAutoCommit(false);
        $config->setConnectTimeout(0.2); $config->setSendTimeout(0.2); $config->setRecvTimeout(0.2);
        $config->setGroupRetry(1); $config->setGroupRetrySleep(0.0);
        try {
            $error = refuses(fn () => new \longlang\phpkafka\Consumer\Consumer($config), 'Native constructor rejects unavailable coordinator',
                \longlang\phpkafka\Exception\KafkaErrorException::class);
            verify($error->getCode() === 15, 'Original native coordinator failure is preserved');
            unset($error); gc_collect_cycles();
            verify(count(ConstructorObservedClient::$weak) === 1, 'Constructor acquired one stored native client');
            foreach (ConstructorObservedClient::$weak as $weak) {
                $owned = $weak->get();
                if ($owned !== null) {
                    verify(!$owned->getSocket()->isConnected(), 'Failed constructor closes native TCP socket');
                    stopped($owned, 'Failed constructor');
                    clean($owned, 'Failed constructor');
                } else {
                    verify(true, 'Closed constructor-owned client is collectible');
                }
                unset($owned);
            }
        } finally {
            // Makes the old-source rejecting control bounded without hiding failure.
            foreach (ConstructorObservedClient::$weak as $weak) { $weak->get()?->close(); }
        }
    });
    verify(Coroutine::stats()['coroutine_num'] === $baseline, 'No native coroutine remains after lifecycle controls');
});
echo json_encode(['status' => 'PASS', 'checks' => $checks, 'php' => PHP_VERSION, 'swoole' => swoole_version(),
    'scope' => 'Actual Swoole coroutine/channel/socket lifecycle with scripted loopback peer and explicit socket faults; no real Kafka acceptance',
    'sources' => array_map(static fn (string $file): string => hash_file('sha256', dirname(__DIR__) . '/' . $file),
        ['src/Client/SwooleClient.php', 'src/Client/SyncClient.php', 'src/Socket/SwooleSocket.php', 'src/Broker.php'])],
    JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR), PHP_EOL;
