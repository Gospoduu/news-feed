"""Redis state for the one-insight-request-per-company-per-day rule."""

import json
from typing import Any

from redis.asyncio import Redis

from src.infra.redis.safe import delete_safe, get_safe, set_safe, ttl_safe


INSIGHT_TTL_SECONDS = 24 * 60 * 60


class InsightCache:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @staticmethod
    def key(company_id: str) -> str:
        return f"insight:company:{company_id}"

    async def get(self, company_id: str) -> dict[str, Any] | None:
        raw = await get_safe(self._redis, self.key(company_id))
        if raw is None:
            return None
        try:
            state = json.loads(raw)
        except json.JSONDecodeError:
            await delete_safe(self._redis, self.key(company_id))
            return None
        return state if isinstance(state, dict) else None

    async def claim(self, company_id: str, event_id: str) -> bool:
        state = json.dumps({"status": "pending", "event_id": event_id})
        return await set_safe(
            self._redis,
            self.key(company_id),
            state,
            ex=INSIGHT_TTL_SECONDS,
            nx=True,
        )

    async def store_response(
        self,
        company_id: str,
        event_id: str,
        response: dict[str, Any],
    ) -> bool:
        """Store a response only for the still-active request of this company."""
        state = await self.get(company_id)
        if state is None or state.get("event_id") != event_id:
            return False

        remaining_ttl = await ttl_safe(self._redis, self.key(company_id))
        if remaining_ttl <= 0:
            return False

        return await set_safe(
            self._redis,
            self.key(company_id),
            json.dumps(
                {
                    "status": "ready",
                    "event_id": event_id,
                    "response": response,
                }
            ),
            ex=remaining_ttl,
        )

    async def discard_pending(self, company_id: str, event_id: str) -> None:
        """Release a failed-to-publish request so the caller can try again."""
        state = await self.get(company_id)
        if state and state.get("status") == "pending" and state.get("event_id") == event_id:
            await delete_safe(self._redis, self.key(company_id))
