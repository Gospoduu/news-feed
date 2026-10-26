import logging
import os
from typing import Any

from src.infra.redis.start_redis import redis_client
from src.interfaces.streams.broker import broker
from src.services.insights import InsightsService


logger = logging.getLogger(__name__)
ANALYSIS_REQUEST_TOPIC = os.getenv("ANALYSIS_REQUEST_TOPIC")
ANALYSIS_RESPONSE_TOPIC = os.getenv("ANALYSIS_RESPONSE_TOPIC")

if not ANALYSIS_REQUEST_TOPIC:
    raise RuntimeError("ANALYSIS_REQUEST_TOPIC is not configured")
if not ANALYSIS_RESPONSE_TOPIC:
    raise RuntimeError("ANALYSIS_RESPONSE_TOPIC is not configured")


response_service = InsightsService(redis_client)


async def publish_analysis_request(event: dict[str, Any], *, key: bytes) -> None:
    await broker.publish(
        event,
        topic=ANALYSIS_REQUEST_TOPIC,
        key=key,
        no_confirm=False,
    )


@broker.subscriber(ANALYSIS_RESPONSE_TOPIC, group_id="news-feed", max_workers=5)
async def handle_analysis_response(event: dict[str, Any]) -> None:
    saved = await response_service.save_response(event)
    if not saved:
        logger.info(
            "Discarded stale insight response: event_id=%s company_id=%s",
            event.get("event_id"),
            event.get("company_id"),
        )
