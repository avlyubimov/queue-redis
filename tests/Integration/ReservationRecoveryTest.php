<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Redis\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Redis;
use RedisException;
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

    public function testAConsumerThatDoesNotAcquireTheMoverLockCannotReleaseIt(): void
    {
        $lock = $this->channel . '.moving_lock';
        $this->redis->set($lock, 'another-owner', ['EX' => 30]);

        self::assertNull($this->provider->reserve(1));

        self::assertSame('another-owner', $this->redis->get($lock));
    }

    public function testAnExpiredLockOwnerCannotReleaseItsSuccessorsLock(): void
    {
        $lock = $this->channel . '.moving_lock';
        $redis = $this->createPartialMock(Redis::class, ['zrevrangebyscore']);
        $redis->connect('redis');
        $redis->method('zrevrangebyscore')->willReturnCallback(
            function (string $key, string $max, string $min) use ($lock): array {
                self::assertNotFalse($this->redis->get($lock));
                // Simulate the first lock expiring and a second mover acquiring it.
                $this->redis->set($lock, 'successor', ['EX' => 30]);
                throw new RedisException('Interrupted expiry recovery.');
            },
        );
        $provider = new QueueProvider($redis, $this->channel);

        try {
            $provider->reserve(1);
            self::fail('Expiry recovery should have been interrupted.');
        } catch (RedisException $exception) {
            self::assertSame('Interrupted expiry recovery.', $exception->getMessage());
        } finally {
            $redis->close();
        }

        self::assertSame('successor', $this->redis->get($lock));
    }

    public function testTheMoverReleasesItsOwnLockAfterAnException(): void
    {
        $redis = $this->createPartialMock(Redis::class, ['zrevrangebyscore']);
        $redis->connect('redis');
        $redis->method('zrevrangebyscore')->willThrowException(new RedisException('Interrupted expiry recovery.'));

        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('Expiry recovery should have been interrupted.');
        } catch (RedisException $exception) {
            self::assertSame('Interrupted expiry recovery.', $exception->getMessage());
        } finally {
            $redis->close();
        }

        self::assertFalse($this->redis->get($this->channel . '.moving_lock'));
    }

    /**
     * @dataProvider expiredSetProvider
     */
    public function testAStaleMoverCannotRequeueAFreshReservation(string $suffix): void
    {
        $id = $this->provider->pushMessage('payload');
        self::assertNotNull($this->provider->reserve(1));
        $this->redis->zRem($this->channel . '.reserved', (string) $id);
        $this->redis->zAdd($this->channel . '.' . $suffix, time() - 1, (string) $id);

        $redis = $this->createPartialMock(Redis::class, ['zrevrangebyscore']);
        $redis->connect('redis');
        $interleaved = false;
        $redis->method('zrevrangebyscore')->willReturnCallback(
            function (string $key, string $max, string $min) use ($suffix, $id, &$interleaved): array {
                $expired = $this->redis->zRevRangeByScore($key, $max, $min);
                self::assertIsArray($expired);
                if ($key === $this->channel . '.' . $suffix && !$interleaved) {
                    $interleaved = true;
                    self::assertSame([(string) $id], $expired);
                    // Pause this mover after selection. Its lock expires, then another
                    // consumer recovers the message and takes a fresh reservation.
                    $this->redis->del($this->channel . '.moving_lock');
                    $recovered = $this->provider->reserve(1);
                    self::assertNotNull($recovered);
                    self::assertSame($id, $recovered->id);
                }
                return $expired;
            },
        );

        try {
            self::assertNull((new QueueProvider($redis, $this->channel))->reserve(1));
        } finally {
            $redis->close();
        }

        self::assertTrue($interleaved);
        self::assertTrue($this->provider->existInReserved($id));
        self::assertSame('2', $this->redis->hGet($this->channel . '.attempts', (string) $id));
        self::assertSame([], $this->redis->lRange($this->channel . '.waiting', 0, -1));
    }

    /** @return array<string, array{string}> */
    public static function expiredSetProvider(): array
    {
        return ['reserved' => ['reserved'], 'delayed' => ['delayed']];
    }
}
