# Back/infra/redis/start_redis

import redis.asyncio as redis
from dotenv import load_dotenv
import os
from pathlib import Path

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set in .env")

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=40,
)
