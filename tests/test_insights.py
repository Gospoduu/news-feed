from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from src.services.insights import InsightsService


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, *, ex: int, nx: bool):
        if nx and key in self.values:
            return None
        self.values[key] = value
        self.ttls[key] = ex
        return True

    async def delete(self, key: str):
        self.values.pop(key, None)
        self.ttls.pop(key, None)
        return 1

    async def ttl(self, key: str):
        return self.ttls.get(key, -2)


class InsightsServiceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.redis = FakeRedis()
        self.publisher = AsyncMock()
        self.service = InsightsService(self.redis, self.publisher)

    async def test_first_request_publishes_once_and_returns_processing(self):
        result = await self.service.get_company_insight("U_67")

        self.assertTrue(result.processing)
        self.publisher.assert_awaited_once()
        event = self.publisher.await_args.args[0]
        self.assertEqual(
            {key: event[key] for key in ("event_type", "schema_version", "company_id")},
            {
                "event_type": "insight.requested",
                "schema_version": 1,
                "company_id": "U_67",
            },
        )

        second = await self.service.get_company_insight("U_67")
        self.assertTrue(second.processing)
        self.publisher.assert_awaited_once()

    async def test_response_is_cached_and_returned_without_new_request(self):
        await self.service.get_company_insight("U_67")
        request = self.publisher.await_args.args[0]
        response = {
            "event_id": request["event_id"],
            "event_type": "insight.completed",
            "schema_version": 1,
            "company_id": "U_67",
            "insight": {"score": 42},
        }

        self.assertTrue(await self.service.save_response(response))
        cached = await self.service.get_company_insight("U_67")

        self.assertFalse(cached.processing)
        self.assertEqual(cached.response, response)
        self.publisher.assert_awaited_once()

    async def test_stale_response_is_not_saved(self):
        await self.service.get_company_insight("U_67")
        response = {
            "event_id": "00000000-0000-0000-0000-000000000001",
            "event_type": "insight.completed",
            "schema_version": 1,
            "company_id": "U_67",
            "insight": {"score": 42},
        }

        self.assertFalse(await self.service.save_response(response))
