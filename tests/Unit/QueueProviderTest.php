<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Redis\Tests\Unit;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Yiisoft\Queue\Redis\Exception\NotConnectedRedisException;
use Yiisoft\Queue\Redis\QueueProvider;
use Yiisoft\Queue\Redis\QueueProviderInterface;
use PHPUnit\Framework\MockObject\Exception;
use Redis;
use RuntimeException;

class QueueProviderTest extends TestCase
{
    /**
     * @dataProvider invalidReservationTimeoutProvider
     */
    public function testReservationTimeoutMustBePositive(int $timeout): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Reservation timeout must be a positive integer.');

        new QueueProvider($this->createMock(Redis::class), 'test', $timeout);
    }

    /** @return array<string, array{int}> */
    public static function invalidReservationTimeoutProvider(): array
    {
        return ['zero' => [0], 'negative' => [-1]];
    }

    public function testChangingTheChannelPreservesTheReservationTimeout(): void
    {
        $redis = $this->createMock(Redis::class);
        $redis->method('isConnected')->willReturn(true);
        $redis->method('set')->willReturn(false);
        $redis->method('brpop')->with('other.waiting', 1)->willReturn(['other.waiting', '1']);
        $redis->method('hget')->with('other.messages', '1')->willReturn('payload');
        $before = time();
        $redis->expects(self::once())->method('zadd')->willReturnCallback(
            static function (string $key, float|int $deadline, string $id) use ($before): int {
                self::assertSame('other.reserved', $key);
                self::assertSame('1', $id);
                self::assertGreaterThanOrEqual($before + 42, $deadline);
                self::assertLessThanOrEqual(time() + 42, $deadline);
                return 1;
            },
        );
        $provider = new QueueProvider($redis, 'test', 42);

        self::assertNotNull($provider->withChannelName('other')->reserve(1));
    }

    public function test__construct()
    {
        $redis = $this->createMock(Redis::class);
        $redis->method('isConnected')->willReturn(true);
        $provider = new QueueProvider($redis, 'test');
        $this->assertInstanceOf(QueueProviderInterface::class, $provider);
        return $provider;
    }

    /**
     * @depends test__construct
     * @throws Exception
     */
    public function testImmutable(QueueProvider $provider): void
    {
        self::assertNotSame($provider, $provider->withChannelName('new'));
    }

    public function testNotConnected(): void
    {
        $redis = new Redis();
        try {
            $provider = new QueueProvider($redis, 'test');
            $provider->getId();
        } catch (NotConnectedRedisException $e) {
            $this->assertEquals('Not connected to Redis.', $e->getName());
            $this->assertNotNull($e->getSolution());
        }
        $this->expectException(NotConnectedRedisException::class);
        $provider->reserve();
    }

    public function testRedisException(): void
    {
        $mock = $this->createMock(Redis::class);
        $mock->method('brPop')->willReturn([1 => 1], [1 => '1']);
        $mock->method('isConnected')->willReturn(true);
        $mock->method('hget')->willReturn(null);
        $mock->method('incr')->willReturn(false);
        $mock->method('zrevrangebyscore')->willReturn(['1', '2']);
        $mock->method('zremrangebyscore')->willReturn(0);
        $mock->method('set')->willReturn(true);
        $mock->expects($this->exactly(8))->method('rpush')->willReturn(1);
        $provider = new QueueProvider($mock);
        $this->assertNull($provider->reserve());
        $this->assertNull($provider->reserve());

        $this->expectException(RuntimeException::class);
        $provider->getId();
    }

    /**
     * @throws Exception
     */
    public function testGetChannelName(): void
    {
        // Тестируем значение по умолчанию
        $redis = $this->createMock(Redis::class);
        $redis->method('isConnected')->willReturn(true);
        $provider = new QueueProvider($redis);
        $this->assertEquals('yii-queue', $provider->getChannelName());

        // Тестируем пользовательское имя канала
        $customChannelName = 'custom-channel';
        $provider = new QueueProvider($redis, $customChannelName);
        $this->assertEquals($customChannelName, $provider->getChannelName());
    }
}
