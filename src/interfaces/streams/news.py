import logging
import os
from typing import Any

from src.interfaces.streams.broker import broker
from src.services.news_ingestion import NewsIngestionService


logger = logging.getLogger(__name__)
NEWS_TOPIC = os.getenv("NEWS_TOPIC")
if not NEWS_TOPIC:
    raise RuntimeError("NEWS_TOPIC is not configured")

service = NewsIngestionService()


@broker.subscriber(NEWS_TOPIC, group_id="news-feed", max_workers=5)
async def consume_parsed_news(event: dict[str, Any]) -> None:
    """Persist a batch sent by news-parser; Kafka retries failures."""
    inserted = await service.ingest(event)
    logger.info("Consumed news event_id=%s, inserted=%d", event["event_id"], inserted)
