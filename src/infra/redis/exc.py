from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    BusyLoadingError,
)

_REDIS_RETRYABLE = (
    RedisConnectionError,
    RedisTimeoutError,
    BusyLoadingError,
)


class RetryableRedisError(RuntimeError):
    """A transient Redis failure that should make the message/request retry."""
