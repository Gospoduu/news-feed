# infra/redis/safe.py

import asyncio
from logging import getLogger
from typing import Any, Mapping
from redis.asyncio import Redis
from Back.core.exc import RetryableError
from .exc import _REDIS_RETRYABLE

logger = getLogger(__name__)


async def xadd_safe(
    redis: Redis,
    key: str,
    fields: Mapping[str, Any],
    maxlen: int | None = None,
):
    try:
        return await redis.xadd(key, fields, maxlen=maxlen)
    except asyncio.CancelledError:
        logger.warning("xadd_safe cancelled. key: %s", key)
        raise
    except _REDIS_RETRYABLE as e:
        logger.exception("Redis xadd failed",extra={"key": key})
        raise RetryableError(f"Redis xadd failed: {e!r}") from e



async def xread_safe(
    redis: Redis,
    streams: Mapping[str, str],
    block: int | None = None,
    count: int | None = None
):
    try:
        return await redis.xread(streams=streams, block=block, count=count)
    except asyncio.CancelledError:
        logger.warning("xread_safe cancelled. streams: %s", streams)
        raise
    except _REDIS_RETRYABLE as e:
        logger.exception("Redis xread failed", extra={"streams": streams, "error": str(e)})
        raise RetryableError(f"Redis xread failed: {e!r}") from e


async def xrange_safe(
    redis: Redis,
    key: str,
    min: str,
    max: str = "+",
    count: int | None = None
):
    try:
        return await redis.xrange(key, min=min, max=max, count=count)
    except asyncio.CancelledError:
        logger.warning("xrange_safe cancelled. key: %s", key)
        raise
    except _REDIS_RETRYABLE as e:
        logger.exception("Redis xrange failed", extra={"key": key})
        raise RetryableError(f"Redis xrange failed: {e!r}") from e
    except Exception:
        logger.exception("Redis xrange failed", extra={"key": key})
        raise