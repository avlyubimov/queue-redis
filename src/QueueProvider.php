<?php

declare(strict_types=1);

namespace Yiisoft\Queue\Redis;

use InvalidArgumentException;
use RedisException;
use Yiisoft\Queue\Message\DelayEnvelope;
use Yiisoft\Queue\Redis\Exception\NotConnectedRedisException;
use Redis;
use RuntimeException;

use function bin2hex;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function random_bytes;

class QueueProvider implements QueueProviderInterface
{
    /**
     * @var string
     *
     * @psalm-suppress MissingClassConstType PHP 8.1 support prevents native typed class constants.
     */
    private const DEFAULT_CHANNEL_NAME = 'yii-queue';

    /**
     * @param int $reservationTimeout Seconds before an unacknowledged message can be reserved again.
     *
     * @throws InvalidArgumentException
     * @throws RedisException
     */
    public function __construct(
        private Redis $redis, //redis connection,
        private string $channelName = self::DEFAULT_CHANNEL_NAME,
        private int $reservationTimeout = 300,
    ) {
        if ($reservationTimeout < 1) {
            throw new InvalidArgumentException('Reservation timeout must be a positive integer.');
        }
    }

    /**
     * @param array<string, array|bool|float|int|string|null> $meta
     *
     * @throws RedisException
     */
    public function pushMessage(string $message, array $meta = []): int
    {
        $this->checkConnection();
        $id = $this->getId();
        $this->redis->hset("$this->channelName.messages", (string) $id, $message);

        /** @var array|bool|float|int|string|null $delay */
        $delay = $meta[DelayEnvelope::META_DELAY_SECONDS] ?? 0;
        $delay = is_int($delay) || is_float($delay) ? $delay : 0;
        if ($delay > 0) {
            $this->redis->zadd("$this->channelName.delayed", time() + $delay, $id);
        } else {
            $this->redis->lpush("$this->channelName.waiting", $id);
        }
        return $id;
    }

    /**
     * @throws RedisException
     */
    public function existInWaiting(int $id): bool
    {
        $this->checkConnection();
        $exist = $this->redis->hexists("$this->channelName.messages", (string) $id);
        return is_bool($exist) ? $exist : false;
    }

    /**
     * @throws RedisException
     */
    public function existInReserved(int $id): bool
    {
        $this->checkConnection();
        return $this->redis->zscore("$this->channelName.reserved", (string) $id) !== false;
    }

    /**
     * @throws RedisException
     */
    public function reserve(int $timeout = 0): ?Reserve
    {
        $this->checkConnection();
        // Moves delayed and reserved jobs into waiting list with lock for one second
        $lockKey = "$this->channelName.moving_lock";
        $lockToken = bin2hex(random_bytes(16));
        if ($this->redis->set($lockKey, $lockToken, ['NX', 'EX', 1])) {
            try {
                $this->moveExpired("$this->channelName.delayed");
                $this->moveExpired("$this->channelName.reserved");
            } finally {
                $this->redis->eval(
                    <<<'LUA'
                    if redis.call('GET', KEYS[1]) == ARGV[1] then
                        return redis.call('DEL', KEYS[1])
                    end
                    return 0
                    LUA,
                    [$lockKey, $lockToken],
                    1,
                );
            }
        }

        $result = $this->redis->brpop("$this->channelName.waiting", $timeout);
        if (null === $result || !isset($result[1])) {
            return null;
        }
        $id = $result[1];
        if (!is_string($id)) {
            return null;
        }

        $payload = $this->redis->hget("$this->channelName.messages", $id);
        if (!is_string($payload)) {
            return null;
        }
        $this->redis->zadd("$this->channelName.reserved", time() + $this->reservationTimeout, $id);
        $this->redis->hincrby("$this->channelName.attempts", $id, 1);
        return new Reserve((int) $id, $payload);
    }

    /**
     * @throws RedisException
     */
    public function delete(string $id): void
    {
        $this->checkConnection();
        $this->redis->zrem("$this->channelName.reserved", $id);
        $this->redis->zrem("$this->channelName.delayed", $id);
        $this->redis->hdel("$this->channelName.messages", $id);
        $this->redis->hdel("$this->channelName.attempts", $id);
    }

    /**
     * @throws RedisException
     */
    public function getId(): int
    {
        $this->checkConnection();
        $id = $this->redis->incr("$this->channelName.message_id");
        if (is_int($id)) {
            return $id;
        }

        throw new RuntimeException('Unable to get message id.');
    }

    /**
     * @throws RedisException
     */
    public function getLastId(): int
    {
        $this->checkConnection();
        $id = $this->redis->get("$this->channelName.message_id");

        return is_string($id) ? (int) $id : 0;
    }

    public function withChannelName(string $channelName): QueueProviderInterface
    {
        if ($this->channelName === $channelName) {
            return $this;
        }

        return new self($this->redis, $channelName, $this->reservationTimeout);
    }

    public function getChannelName(): string
    {
        return $this->channelName;
    }

    /**
     * @throws RedisException
     */
    private function moveExpired(string $from): void
    {
        $now = time();
        $expired = $this->redis->zrevrangebyscore($from, (string) $now, '-inf');
        if (is_array($expired)) {
            /** @var string $id */
            foreach ($expired as $id) {
                // A concurrent mover may have already recovered and reserved this ID.
                $this->redis->eval(
                    <<<'LUA'
                    local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
                    if score and tonumber(score) <= tonumber(ARGV[2]) then
                        if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
                            return redis.call('RPUSH', KEYS[2], ARGV[1])
                        end
                    end
                    return 0
                    LUA,
                    [$from, "$this->channelName.waiting", $id, $now],
                    2,
                );
            }
        }
    }

    /**
     * @throws RedisException
     */
    private function checkConnection(): void
    {
        if (!$this->redis->isConnected()) {
            throw new NotConnectedRedisException('Redis is not connected.');
        }
    }
}
