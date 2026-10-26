"""Small retry-aware wrappers around the Redis commands used by this service."""

import asyncio
from logging import getLogger
from typing import Any, Mapping

from redis.asyncio import Redis

from src.infra.redis.exc import RetryableRedisError, _REDIS_RETRYABLE


logger = getLogger(__name__)


async def _call(operation: str, context: Mapping[str, Any], command):
    try:
        return await command()
    except asyncio.CancelledError:
        raise
    except _REDIS_RETRYABLE as error:
        logger.exception("Redis %s failed", operation, extra=dict(context))
        raise RetryableRedisError(f"Redis {operation} failed: {error!r}") from error


async def get_safe(redis: Redis, key: str) -> str | None:
    return await _call("GET", {"key": key}, lambda: redis.get(key))


async def set_safe(
    redis: Redis,
    key: str,
    value: str,
    *,
    ex: int,
    nx: bool = False,
) -> bool:
    result = await _call(
        "SET",
        {"key": key},
        lambda: redis.set(key, value, ex=ex, nx=nx),
    )
    return bool(result)


async def delete_safe(redis: Redis, key: str) -> int:
    return await _call("DEL", {"key": key}, lambda: redis.delete(key))


async def ttl_safe(redis: Redis, key: str) -> int:
    return await _call("TTL", {"key": key}, lambda: redis.ttl(key))
