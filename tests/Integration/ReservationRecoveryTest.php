<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Redis\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Redis;
use RedisException;
use RuntimeException;
use Yiisoft\Queue\Message\DelayEnvelope;
use Yiisoft\Queue\Redis\QueueProvider;

use function bin2hex;
use function random_bytes;
use function sleep;
use function constant;
use function defined;

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
        $before = $this->serverTime();

        $message = $this->provider->reserve(1);

        self::assertNotNull($message);
        self::assertSame($id, $message->id);
        $deadline = $this->redis->zScore($this->channel . '.reserved', (string) $id);
        self::assertNotFalse($deadline);
        self::assertGreaterThanOrEqual($before + 30, $deadline);
        self::assertLessThanOrEqual($this->serverTime() + 30, $deadline);
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
        $expired = $this->serverTime() - 1;
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
        $this->redis->zAdd($this->channel . '.' . $suffix, $this->serverTime() - 1, (string) $id);

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

    /** @dataProvider encodingProvider */
    public function testRecoveryPreservesClientValueEncoding(int $serializer, int $compression): void
    {
        $this->redis->setOption(Redis::OPT_PREFIX, 'prefixed:');
        $this->redis->setOption(Redis::OPT_SERIALIZER, $serializer);
        $this->redis->setOption(Redis::OPT_COMPRESSION, $compression);

        foreach ([false, true] as $delayed) {
            $meta = $delayed ? [DelayEnvelope::META_DELAY_SECONDS => 30] : [];
            $id = $this->provider->pushMessage('payload', $meta);
            if ($delayed) {
                $this->redis->zAdd($this->channel . '.delayed', 0, (string) $id);
            }

            $first = $this->provider->reserve(1);
            self::assertNotNull($first);
            self::assertSame($id, $first->id);
            self::assertSame('payload', $first->payload);
            self::assertFalse($this->redis->get($this->channel . '.moving_lock'));
            self::assertTrue($this->provider->existInReserved($id));

            $this->redis->zAdd($this->channel . '.reserved', 0, (string) $id);
            $recovered = $this->provider->reserve(1);
            self::assertNotNull($recovered);
            self::assertSame($id, $recovered->id);
            self::assertSame('payload', $recovered->payload);
            self::assertFalse($this->redis->get($this->channel . '.moving_lock'));
            self::assertSame(2, (int) $this->redis->hGet($this->channel . '.attempts', (string) $id));
            $this->provider->delete((string) $id);
            self::assertFalse($this->provider->existInReserved($id));
        }
    }

    /** @return iterable<string, array{int, int}> */
    public static function encodingProvider(): iterable
    {
        foreach (['SERIALIZER_NONE', 'SERIALIZER_PHP', 'SERIALIZER_JSON', 'SERIALIZER_IGBINARY', 'SERIALIZER_MSGPACK'] as $serializerName) {
            if (!defined(Redis::class . '::' . $serializerName)) {
                continue;
            }
            $serializer = constant(Redis::class . '::' . $serializerName);
            foreach (['COMPRESSION_NONE', 'COMPRESSION_LZF', 'COMPRESSION_LZ4', 'COMPRESSION_ZSTD'] as $compressionName) {
                if (!defined(Redis::class . '::' . $compressionName)) {
                    continue;
                }
                $compression = constant(Redis::class . '::' . $compressionName);
                if ($serializer === Redis::SERIALIZER_JSON && $compression !== Redis::COMPRESSION_NONE) {
                    // PhpRedis 6.3.0 itself does not round-trip JSON scalar IDs with compression.
                    continue;
                }
                yield "$serializerName/$compressionName" => [$serializer, $compression];
            }
        }
    }

    public function testMessagesWithIntegerEncodedIdsCanStillBeRecovered(): void
    {
        $this->redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_PHP);
        $id = $this->provider->pushMessage('payload');
        $this->redis->del($this->channel . '.waiting');
        $this->redis->zAdd($this->channel . '.delayed', 0, $id);

        $message = $this->provider->reserve(1);

        self::assertNotNull($message);
        self::assertSame($id, $message->id);
        self::assertSame('payload', $message->payload);
        self::assertTrue($this->provider->existInReserved($id));
        $this->provider->delete((string) $id);
        self::assertFalse($this->provider->existInReserved($id));
    }

    public function testAOneSecondReservationIsNotRoundedDown(): void
    {
        $provider = new QueueProvider($this->redis, $this->channel, 1);
        $id = $provider->pushMessage('payload');
        $before = $this->serverTime();
        self::assertNotNull($provider->reserve(1));
        $deadline = $this->redis->zScore($this->channel . '.reserved', (string) $id);
        self::assertNotFalse($deadline);
        self::assertGreaterThanOrEqual($before + 1, $deadline);
        self::assertLessThanOrEqual($this->serverTime() + 1, $deadline);
    }

    public function testDeadlinesUseRedisTimeRatherThanTheWorkersClock(): void
    {
        $redis = $this->createPartialMock(Redis::class, ['time']);
        $redis->connect('redis');
        $redis->method('time')->willReturn(['1234', '567890']);
        $provider = new QueueProvider($redis, $this->channel, 1);
        try {
            $id = $provider->pushMessage('delayed', [DelayEnvelope::META_DELAY_SECONDS => 1]);
            self::assertSame(1235.56789, $this->redis->zScore($this->channel . '.delayed', (string) $id));
            $this->redis->zAdd($this->channel . '.delayed', 1234.56789, (string) $id);
            self::assertNotNull($provider->reserve(1));
            self::assertSame(1235.56789, $this->redis->zScore($this->channel . '.reserved', (string) $id));
            self::assertNull($provider->reserve(1));
        } finally {
            $redis->close();
        }
    }

    /** @dataProvider registrationFailureProvider */
    public function testRegistrationFailureDoesNotDeliverOrLoseTheMessage(bool $throw, bool $registered): void
    {
        $id = $this->provider->pushMessage('payload');
        $redis = $this->createPartialMock(Redis::class, ['zadd']);
        $redis->connect('redis');
        $redis->method('zadd')->willReturnCallback(
            function (string $key, float $deadline, string $member) use ($throw, $registered): bool {
                if ($registered) {
                    $this->redis->zAdd($key, $deadline, $member);
                }
                if ($throw) {
                    throw new RedisException('Registration failed.');
                }
                return false;
            },
        );
        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('A failed registration must not return a message.');
        } catch (RedisException|RuntimeException $exception) {
            self::assertSame($throw ? 'Registration failed.' : 'Unable to register the message reservation.', $exception->getMessage());
        } finally {
            $redis->close();
        }
        self::assertSame($registered, $this->provider->existInReserved($id));
        self::assertSame($registered ? [] : [(string) $id], $this->redis->lRange($this->channel . '.waiting', 0, -1));
        self::assertFalse($this->redis->hGet($this->channel . '.attempts', (string) $id));
    }

    /** @return array<string, array{bool, bool}> */
    public static function registrationFailureProvider(): array
    {
        return [
            'false' => [false, false],
            'exception' => [true, false],
            'false after write' => [false, true],
            'exception after write' => [true, true],
        ];
    }

    public function testAZeroZaddReplyIsASuccessfulReservation(): void
    {
        $id = $this->provider->pushMessage('payload');
        // Updating an existing member returns zero, not a Redis error.
        $this->redis->zAdd($this->channel . '.reserved', $this->serverTime() + 30, (string) $id);

        self::assertNotNull($this->provider->reserve(1));
        self::assertTrue($this->provider->existInReserved($id));
    }

    public function testFailedRequeuePreservesTheExpiredEntry(): void
    {
        $id = $this->provider->pushMessage('payload');
        self::assertNotNull($this->provider->reserve(1));
        $this->redis->zAdd($this->channel . '.reserved', 0, (string) $id);
        $this->redis->set($this->channel . '.waiting', 'wrong-type');

        try {
            $this->provider->reserve(1);
            self::fail('A failed Lua command must be reported.');
        } catch (RedisException|RuntimeException) {
            self::assertSame(0.0, $this->redis->zScore($this->channel . '.reserved', (string) $id));
            self::assertSame('payload', $this->redis->hGet($this->channel . '.messages', (string) $id));
            self::assertFalse($this->redis->get($this->channel . '.moving_lock'));
        }
    }

    public function testAFailedLuaReplyIsNotTreatedAsASuccessfulRecovery(): void
    {
        $id = $this->provider->pushMessage('payload');
        self::assertNotNull($this->provider->reserve(1));
        $this->redis->zAdd($this->channel . '.reserved', 0, (string) $id);
        $redis = $this->createPartialMock(Redis::class, ['eval', 'brpop']);
        $redis->connect('redis');
        $redis->method('eval')->willReturn(false);
        $redis->expects(self::never())->method('brpop');
        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('A failed Lua reply must be reported.');
        } catch (RuntimeException $exception) {
            self::assertSame('Unable to execute the queue Lua script.', $exception->getMessage());
        } finally {
            $redis->close();
        }
        self::assertSame(0.0, $this->redis->zScore($this->channel . '.reserved', (string) $id));
    }

    public function testAFailedLockCleanupIsReportedBeforePoppingAMessage(): void
    {
        $id = $this->provider->pushMessage('payload');
        $redis = $this->createPartialMock(Redis::class, ['eval', 'brpop']);
        $redis->connect('redis');
        $redis->method('eval')->willReturn(false);
        $redis->expects(self::never())->method('brpop');
        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('A failed lock cleanup must be reported.');
        } catch (RuntimeException $exception) {
            self::assertSame('Unable to execute the queue Lua script.', $exception->getMessage());
        } finally {
            $redis->close();
        }
        self::assertSame([(string) $id], $this->redis->lRange($this->channel . '.waiting', 0, -1));
    }

    public function testCleanupFailureDoesNotReplaceTheRecoveryException(): void
    {
        $redis = $this->createPartialMock(Redis::class, ['eval', 'zrevrangebyscore']);
        $redis->connect('redis');
        $redis->method('eval')->willReturn(false);
        $redis->method('zrevrangebyscore')->willThrowException(new RedisException('Original recovery failure.'));
        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('The original recovery error must be reported.');
        } catch (RedisException $exception) {
            self::assertSame('Original recovery failure.', $exception->getMessage());
        } finally {
            $redis->close();
        }
    }

    public function testServerTimeFailureAfterPopRestoresTheMessage(): void
    {
        $id = $this->provider->pushMessage('payload');
        $this->redis->set($this->channel . '.moving_lock', 'another-owner', ['EX' => 30]);
        $redis = $this->createPartialMock(Redis::class, ['time']);
        $redis->connect('redis');
        $redis->method('time')->willThrowException(new RedisException('Server TIME failed.'));
        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('A failed TIME command must be reported.');
        } catch (RedisException $exception) {
            self::assertSame('Server TIME failed.', $exception->getMessage());
        } finally {
            $redis->close();
        }
        self::assertSame([(string) $id], $this->redis->lRange($this->channel . '.waiting', 0, -1));
        self::assertFalse($this->provider->existInReserved($id));
    }

    public function testFailedRestorationKeepsTheRegistrationErrorAsItsCause(): void
    {
        $this->provider->pushMessage('payload');
        $redis = $this->createPartialMock(Redis::class, ['zadd', 'eval']);
        $redis->connect('redis');
        $redis->method('zadd')->willReturn(false);
        $redis->method('eval')->willReturnCallback(
            fn(string $script, array $arguments, int $keyCount): int|false => $keyCount === 2
                ? false
                : $this->redis->eval($script, $arguments, $keyCount),
        );
        try {
            (new QueueProvider($redis, $this->channel))->reserve(1);
            self::fail('A failed restoration must be reported.');
        } catch (RuntimeException $exception) {
            self::assertSame('Unable to restore the message after reservation failure: Unable to execute the queue Lua script.', $exception->getMessage());
            self::assertSame('Unable to register the message reservation.', $exception->getPrevious()?->getMessage());
        } finally {
            $redis->close();
        }
    }

    private function serverTime(): float
    {
        $time = $this->redis->time();
        self::assertIsArray($time);
        return (float) $time[0] + (float) $time[1] / 1_000_000;
    }
}
