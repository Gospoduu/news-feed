"""Request and receive company insights through Kafka, with Redis caching."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from redis.asyncio import Redis

from src.infra.redis.insights import InsightCache


PublishInsight = Callable[..., Awaitable[None]]
REQUEST_EVENT_TYPE = "insight.requested"
RESPONSE_EVENT_TYPE = "insight.completed"


class InvalidInsightEvent(ValueError):
    pass


@dataclass(frozen=True)
class InsightLookup:
    processing: bool
    response: dict[str, Any] | None = None


class InsightsService:
    def __init__(self, redis: Redis, publisher: PublishInsight | None = None) -> None:
        self._cache = InsightCache(redis)
        self._publisher = publisher

    async def get_company_insight(self, company_id: str) -> InsightLookup:
        company_id = company_id.strip()
        if not company_id:
            raise ValueError("company_id must not be empty")

        state = await self._cache.get(company_id)
        if state is not None:
            if state.get("status") == "ready" and isinstance(state.get("response"), dict):
                return InsightLookup(processing=False, response=state["response"])
            return InsightLookup(processing=True)

        if self._publisher is None:
            raise RuntimeError("Insight publisher is not configured")

        event_id = str(uuid4())
        claimed = await self._cache.claim(company_id, event_id)
        if not claimed:
            state = await self._cache.get(company_id)
            if state and state.get("status") == "ready":
                return InsightLookup(processing=False, response=state.get("response"))
            return InsightLookup(processing=True)

        event = {
            "event_id": event_id,
            "event_type": REQUEST_EVENT_TYPE,
            "schema_version": 1,
            "company_id": company_id,
        }
        try:
            await self._publisher(event, key=company_id.encode())
        except Exception:
            await self._cache.discard_pending(company_id, event_id)
            raise
        return InsightLookup(processing=True)

    async def save_response(self, event: Mapping[str, Any]) -> bool:
        """Cache a valid response from the analytics service for its requester."""
        if event.get("event_type") != RESPONSE_EVENT_TYPE or event.get("schema_version") != 1:
            raise InvalidInsightEvent("unsupported insight response event")
        company_id = event.get("company_id")
        if not isinstance(company_id, str) or not company_id.strip():
            raise InvalidInsightEvent("company_id must be a non-empty string")
        # The insight agent assigns its own event_id to the completed insight
        # and echoes our request ID in request_event_id.
        request_event_id = event.get("request_event_id", event.get("event_id"))
        try:
            event_id = str(UUID(str(request_event_id)))
        except (KeyError, ValueError) as error:
            raise InvalidInsightEvent("event_id must be a UUID") from error
        if "insight" not in event:
            raise InvalidInsightEvent("insight response must contain insight")

        return await self._cache.store_response(
            company_id.strip(),
            event_id,
            dict(event),
        )
