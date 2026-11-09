<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Redis\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Redis;
use Yiisoft\Queue\Redis\QueueProvider;

use function bin2hex;
use function random_bytes;
use function sleep;
use function time;

final class ReservationRecoveryTest extends TestCase
{
    private Redis $redis;
    private string $channel;
    private QueueProvider $provider;

    protected function setUp(): void
    {
        parent::setUp();
        $this->redis = new Redis();
        $this->redis->connect('redis');
        $this->channel = 'reservation-test-' . bin2hex(random_bytes(8));
        $this->provider = new QueueProvider($this->redis, $this->channel, 30);
    }

    protected function tearDown(): void
    {
        foreach (['messages', 'waiting', 'reserved', 'attempts', 'delayed', 'message_id', 'moving_lock'] as $suffix) {
            $this->redis->del($this->channel . '.' . $suffix);
        }
        $this->redis->close();
        parent::tearDown();
    }

    public function testReserveRecordsAnExpirationDeadline(): void
    {
        $id = $this->provider->pushMessage('payload');
        $before = time();

        $message = $this->provider->reserve(1);

        self::assertNotNull($message);
        self::assertSame($id, $message->id);
        $deadline = $this->redis->zScore($this->channel . '.reserved', (string) $id);
        self::assertNotFalse($deadline);
        self::assertGreaterThanOrEqual($before + 30, $deadline);
        self::assertLessThanOrEqual(time() + 30, $deadline);
    }

    public function testUnacknowledgedMessageIsRecoveredAfterTheConsumerDisconnects(): void
    {
        $id = $this->provider->pushMessage('payload');
        $connection = new Redis();
        $connection->connect('redis');
        $consumer = new QueueProvider($connection, $this->channel, 1);
        $first = $consumer->reserve(1);
        self::assertNotNull($first);
        self::assertSame($id, $first->id);
        $connection->close();

        sleep(2);

        $recovered = $this->provider->reserve(1);
        self::assertNotNull($recovered);
        self::assertSame($id, $recovered->id);
        self::assertSame('payload', $recovered->payload);
        self::assertSame('2', $this->redis->hGet($this->channel . '.attempts', (string) $id));
    }

    public function testAnUnexpiredReservationIsNotDeliveredAgain(): void
    {
        $provider = new QueueProvider($this->redis, $this->channel, 30);
        $id = $provider->pushMessage('payload');
        self::assertNotNull($provider->reserve(1));

        self::assertNull($provider->reserve(1));
        self::assertTrue($provider->existInReserved($id));
        self::assertSame('payload', $this->redis->hGet($this->channel . '.messages', (string) $id));
    }

    public function testAnExpiredReservationCanReturnToWaiting(): void
    {
        $firstId = $this->provider->pushMessage('first');
        $secondId = $this->provider->pushMessage('second');
        self::assertNotNull($this->provider->reserve(1));
        self::assertNotNull($this->provider->reserve(1));
        $expired = time() - 1;
        $this->redis->zAdd($this->channel . '.reserved', $expired, (string) $firstId, $expired, (string) $secondId);

        $recovered = $this->provider->reserve(1);

        self::assertNotNull($recovered);
        $waitingId = $recovered->id === $firstId ? $secondId : $firstId;
        self::assertTrue($this->provider->existInWaiting($waitingId));
        self::assertFalse($this->provider->existInReserved($waitingId));
        self::assertTrue($this->provider->existInReserved($recovered->id));
    }

    public function testAcknowledgementRemovesTheReservation(): void
    {
        $id = $this->provider->pushMessage('payload');
        self::assertNotNull($this->provider->reserve(1));

        $this->provider->delete((string) $id);

        self::assertFalse($this->provider->existInReserved($id));
        self::assertFalse($this->provider->existInWaiting($id));
        self::assertNull($this->provider->reserve(1));
    }
}
