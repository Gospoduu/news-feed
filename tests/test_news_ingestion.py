import os
from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://news:news@localhost/news")

from src.infra.repositories.news import NewsRepository
from src.services.news_ingestion import (
    InvalidNewsEvent,
    NewsIngestionService,
    parse_news_event,
)


def event() -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": "news.parsed.v1",
        "schema_version": 1,
        "source": {
            "name": "Example",
            "rss_url": "https://example.com/rss",
            "site_url": "https://example.com",
        },
        "news": [
            {
                "external_id": "article-1",
                "title": "Title",
                "link": "https://example.com/article-1",
                "content_text": None,
                "authors": ["Author"],
                "published_at": "2026-10-25T10:00:00+00:00",
                "updated_at": None,
                "summary": None,
                "section": None,
                "tags": ["tag"],
                "language": "ru",
                "image_url": None,
                "image_caption": None,
                "images": [],
            }
        ],
    }


class NewsIngestionTests(IsolatedAsyncioTestCase):
    def test_contract_is_normalized_for_news_table(self):
        payload = event()
        event_id, records = parse_news_event(payload)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["source_rss_url"], "https://example.com/rss")
        self.assertEqual(records[0]["external_id"], "article-1")
        self.assertEqual(str(event_id), payload["event_id"])

    def test_rejects_unknown_event_version(self):
        payload = event()
        payload["schema_version"] = 2
        with self.assertRaises(InvalidNewsEvent):
            parse_news_event(payload)

    async def test_service_persists_normalized_batch(self):
        connection = object()

        @asynccontextmanager
        async def connection_factory():
            yield connection

        service = NewsIngestionService(connection_factory)
        service._repository = AsyncMock()
        service._repository.ingest_batch.return_value = 1

        payload = event()
        inserted = await service.ingest(payload)

        self.assertEqual(inserted, 1)
        _, records = parse_news_event(payload)
        event_id = payload["event_id"]
        self.assertEqual(
            service._repository.ingest_batch.await_args.args[0], connection
        )
        self.assertEqual(
            str(service._repository.ingest_batch.await_args.args[1]), event_id
        )
        self.assertEqual(service._repository.ingest_batch.await_args.args[2], records)

    async def test_duplicate_event_does_not_insert_articles(self):
        connection = AsyncMock()
        duplicate = MagicMock()
        duplicate.scalar_one_or_none.return_value = None
        connection.execute.return_value = duplicate

        inserted = await NewsRepository().ingest_batch(
            connection, uuid4(), [{"external_id": "article-1"}]
        )

        self.assertEqual(inserted, 0)
        self.assertEqual(connection.execute.await_count, 1)
