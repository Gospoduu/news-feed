from collections.abc import Sequence
from typing import Any
from uuid import UUID
from datetime import timedelta, datetime, timezone

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import consumed_news_events, news
from src.infra.repositories.base import BaseRepository


class NewsRepository(BaseRepository):
    """Persistence boundary for news received from the parser."""

    model = news

    async def ingest_batch(
        self,
        connection: AsyncConnection,
        event_id: UUID,
        articles: Sequence[dict[str, Any]],
    ) -> int:
        """Save an event once and return the number of newly saved articles."""
        accepted = await connection.execute(
            insert(consumed_news_events)
            .values(event_id=event_id)
            .on_conflict_do_nothing(index_elements=[consumed_news_events.c.event_id])
            .returning(consumed_news_events.c.event_id)
        )
        if accepted.scalar_one_or_none() is None:
            return 0

        inserted = await connection.execute(
            insert(news)
            .values(list(articles))
            .on_conflict_do_nothing(constraint="uq_news_source_external_id")
            .returning(news.c.id)
        )
        return len(inserted.scalars().all())

    async def get_feed(
            self,
            connection: AsyncConnection,
            *,
            limit: int = 50,
            offset: int = 0,
    ) -> list[dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)

        result = await connection.execute(
            sa.select(self.model)
            .where(self.model.c.published_at >= cutoff)
            .order_by(self.model.c.published_at.desc(), self.model.c.id.desc())
            .limit(limit)
            .offset(offset)
        )
        return [dict(row._mapping) for row in result]