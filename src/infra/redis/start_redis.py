import redis.asyncio as redis
import os

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set in .env")

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=40,
)
