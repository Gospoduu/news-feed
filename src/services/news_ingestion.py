"""Validation and persistence for events produced by news-parser."""

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from src.infra.db_engine import get_connection
from src.infra.repositories.news import NewsRepository


class InvalidNewsEvent(ValueError):
    """Raised when a Kafka payload does not implement ``news.parsed.v1``."""


def _required_text(data: Mapping[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise InvalidNewsEvent(f"{field} must be a non-empty string")
    return value.strip()


def _optional_text(data: Mapping[str, Any], field: str) -> str | None:
    value = data.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidNewsEvent(f"{field} must be a string or null")
    return value.strip() or None


def _text_list(data: Mapping[str, Any], field: str) -> list[str]:
    value = data.get(field, [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise InvalidNewsEvent(f"{field} must be a list of strings")
    return value


def _optional_datetime(data: Mapping[str, Any], field: str) -> datetime | None:
    value = data.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidNewsEvent(f"{field} must be an ISO-8601 string or null")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise InvalidNewsEvent(f"{field} must be an ISO-8601 datetime") from error


def parse_news_event(event: Mapping[str, Any]) -> tuple[UUID, list[dict[str, Any]]]:
    """Parse a ``news.parsed.v1`` payload into records for the news table."""
    if event.get("event_type") != "news.parsed.v1" or event.get("schema_version") != 1:
        raise InvalidNewsEvent("unsupported news event type or schema version")
    try:
        event_id = UUID(_required_text(event, "event_id"))
    except ValueError as error:
        raise InvalidNewsEvent("event_id must be a UUID") from error

    source = event.get("source")
    if not isinstance(source, Mapping):
        raise InvalidNewsEvent("source must be an object")
    source_rss_url = _required_text(source, "rss_url")
    source_name = _required_text(source, "name")
    source_site_url = _optional_text(source, "site_url")

    payload_news = event.get("news")
    if not isinstance(payload_news, list) or not payload_news:
        raise InvalidNewsEvent("news must be a non-empty list")

    records: list[dict[str, Any]] = []
    for item in payload_news:
        if not isinstance(item, Mapping):
            raise InvalidNewsEvent("every news item must be an object")
        records.append(
            {
                "source_rss_url": source_rss_url,
                "source_name": source_name,
                "source_site_url": source_site_url,
                "external_id": _required_text(item, "external_id"),
                "title": _required_text(item, "title"),
                "link": _required_text(item, "link"),
                "content_text": _optional_text(item, "content_text"),
                "authors": _text_list(item, "authors"),
                "published_at": _optional_datetime(item, "published_at"),
                "updated_at": _optional_datetime(item, "updated_at"),
                "summary": _optional_text(item, "summary"),
                "section": _optional_text(item, "section"),
                "tags": _text_list(item, "tags"),
                "language": _optional_text(item, "language"),
                "image_url": _optional_text(item, "image_url"),
                "image_caption": _optional_text(item, "image_caption"),
                "images": _text_list(item, "images"),
            }
        )
    return event_id, records


class NewsIngestionService:
    def __init__(self, connection_factory=get_connection) -> None:
        self._connection_factory = connection_factory
        self._repository = NewsRepository()

    async def ingest(self, event: Mapping[str, Any]) -> int:
        event_id, articles = parse_news_event(event)
        async with self._connection_factory() as connection:
            return await self._repository.ingest_batch(connection, event_id, articles)

    async def get_news(
            self,
            *,
            limit: int = 50,
            offset: int = 0,
    ) -> list[dict[str, Any]]:
        async with self._connection_factory() as connection:
            return await self._repository.get_feed(
                connection,
                limit=limit,
                offset=offset,
            )
