from collections.abc import Callable
from typing import Any

from src.infra.repositories.financial_uid_links import FinancialUidLinksRepository


class Counterparties:
    """Queries over companies linked to a physical person."""

    def __init__(
        self,
        connection_factory: Callable[..., Any],
    ) -> None:
        self._connection_factory = connection_factory
        self._links_repository = FinancialUidLinksRepository()

    async def get_companies_by_person_id(
        self,
        person_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Return all known companies linked to ``SYNTHETIC_F`` (FID)."""
        async with self._connection_factory() as connection:
            return await self._links_repository.list_companies_by_fid(
                connection,
                person_id,
                limit=limit,
                offset=offset,
            )
