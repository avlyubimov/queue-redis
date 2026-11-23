<p align="center">
    <a href="https://github.com/yiisoft" target="_blank">
        <img src="https://yiisoft.github.io/docs/images/yii_logo.svg" height="100px">
    </a>
    <h1 align="center">Yii Queue Redis</h1>
    <br>
</p>

[![Latest Stable Version](https://poser.pugx.org/yiisoft/_____/v/stable.png)](https://packagist.org/packages/yiisoft/_____)
[![Total Downloads](https://poser.pugx.org/yiisoft/_____/downloads.png)](https://packagist.org/packages/yiisoft/_____)
[![Build status](https://github.com/yiisoft/_____/workflows/build/badge.svg)](https://github.com/yiisoft/_____/actions?query=workflow%3Abuild)
[![Code Coverage](https://codecov.io/gh/yiisoft/_____/branch/master/graph/badge.svg)](https://codecov.io/gh/yiisoft/_____)
[![Mutation testing badge](https://img.shields.io/endpoint?style=flat&url=https%3A%2F%2Fbadge-api.stryker-mutator.io%2Fgithub.com%2Fyiisoft%2F_____%2Fmaster)](https://dashboard.stryker-mutator.io/reports/github.com/yiisoft/_____/master)
[![static analysis](https://github.com/yiisoft/_____/workflows/static%20analysis/badge.svg)](https://github.com/yiisoft/_____/actions?query=workflow%3A%22static+analysis%22)
[![type-coverage](https://shepherd.dev/github/yiisoft/_____/coverage.svg)](https://shepherd.dev/github/yiisoft/_____)
[![psalm-level](https://shepherd.dev/github/yiisoft/_____/level.svg)](https://shepherd.dev/github/yiisoft/_____)

The package ...

## Requirements

- PHP 8.1 or higher.

## Installation

The package could be installed with composer:

```shell
composer require yiisoft/queue-redis
```

## General usage

### Reservation timeout

Reserved messages that are not acknowledged with `delete()` become eligible for
redelivery after 300 seconds by default. A subsequent `reserve()` call moves
expired reservations back to the waiting list. This allows another consumer to
process a message when its previous consumer exits before acknowledging it.

Configure the timeout in seconds through `QueueProvider`:

```php
use Yiisoft\Queue\Redis\QueueProvider;

// $redis is a connected ext-redis client.
$provider = new QueueProvider($redis, 'emails', reservationTimeout: 600);
```

The reservation timeout must be positive and should exceed the longest expected
message processing time. It is independent of the timeout passed to `reserve()`,
which controls how long to wait for an available message. Handlers should be
idempotent because a message may be delivered more than once.

Deadlines and expiry checks use Redis `TIME`, including its microsecond component,
so workers do not need synchronized local clocks and a one-second reservation is
not rounded down to the start of the second. `TIME` is read outside Lua to retain
compatibility with Redis 3.0.

All producers and consumers of a channel must use the same PhpRedis prefix,
serializer, and compression settings. These settings must round-trip string
message IDs and payloads correctly in PhpRedis. Recovery and lock cleanup pack
Lua arguments with the client's configured encoding, including compression.

If reservation registration fails, `reserve()` throws instead of returning the
message. It restores the popped ID to the waiting list when no reservation
deadline exists; an already registered deadline is left for normal recovery.
Restoration is best effort if the connection is unavailable. Popping and
registering a reservation remain separate operations, so this does not eliminate
the crash window between them. Lua failures are also reported as exceptions.
If `RPUSH` fails during recovery, the Lua script attempts to restore the original
sorted-set entry before reporting the error. If recovery and lock cleanup both fail, the recovery error is preserved
and the lock expires after one second.

### Redis command permissions

Queue workers need `EVAL` and permission for every command called inside the Lua
scripts, as well as the normal queue commands. For an ACL-managed Redis server,
grant access only to the channel's keys (for example, `~emails.*`, including any
configured PhpRedis key prefix) and the following commands:

| Operation | Commands |
| --- | --- |
| Push messages | `INCR`, `HSET`, `LPUSH`, `ZADD`, `TIME` |
| Reserve messages and count attempts | `BRPOP`, `HGET`, `ZADD`, `HINCRBY`, `TIME` |
| Coordinate recovery and release locks | `SET` (with `NX` and `EX`), `EVAL`; Lua uses `GET`, `DEL`, `ZSCORE`, `ZREM`, `RPUSH`, `LPUSH`, `ZADD` |
| Select expired entries | `ZREVRANGEBYSCORE` |
| Check message status | `HEXISTS`, `ZSCORE` |
| Acknowledge messages | `ZREM`, `HDEL` |
| Read the last generated ID | `GET` |

Connection setup may additionally require `AUTH`, `SELECT`, or `PING`, depending
on the application's Redis connection configuration. Recovery does not require
`KEYS`, `SCAN`, `SCRIPT LOAD`, or administrative commands.

## Testing

### Unit testing

The package is tested with [PHPUnit](https://phpunit.de/). To run tests:

```shell
make test
```

### Mutation testing

The package tests are checked with [Infection](https://infection.github.io/) mutation framework. To run it:

```shell
./vendor/bin/infection
```

### Static analysis

The code is statically analyzed with [Psalm](https://psalm.dev/). To run static analysis:

```shell
make static-analyze
```

### Code style

Use [Rector](https://github.com/rectorphp/rector) to make codebase follow some specific rules or 
use either newest or any specific version of PHP: 

```shell
make rector
```

### Dependencies

Use [Composer Dependency Analyser](https://github.com/shipmonk-rnd/composer-dependency-analyser) to detect unknown,
shadow, and unused [Composer](https://getcomposer.org) dependencies:

```shell
./vendor/bin/composer-dependency-analyser
```

## License

The Yii Queue Redis is free software. It is released under the terms of the BSD License.
Please see [`LICENSE`](./LICENSE.md) for more information.

Maintained by [Yii Software](https://www.yiiframework.com/).

## Support the project

[![Open Collective](https://img.shields.io/badge/Open%20Collective-sponsor-7eadf1?logo=open%20collective&logoColor=7eadf1&labelColor=555555)](https://opencollective.com/yiisoft)

## Follow updates

[![Official website](https://img.shields.io/badge/Powered_by-Yii_Framework-green.svg?style=flat)](https://www.yiiframework.com/)
[![Twitter](https://img.shields.io/badge/twitter-follow-1DA1F2?logo=twitter&logoColor=1DA1F2&labelColor=555555?style=flat)](https://twitter.com/yiiframework)
[![Telegram](https://img.shields.io/badge/telegram-join-1DA1F2?style=flat&logo=telegram)](https://t.me/yii3en)
[![Facebook](https://img.shields.io/badge/facebook-join-1DA1F2?style=flat&logo=facebook&logoColor=ffffff)](https://www.facebook.com/groups/yiitalk)
[![Slack](https://img.shields.io/badge/slack-join-1DA1F2?style=flat&logo=slack)](https://yiiframework.com/go/slack)
