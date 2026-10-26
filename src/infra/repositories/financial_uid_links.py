from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import company_profiles, financial_uid_links
from src.infra.repositories.base import BaseRepository, Record


class FinancialUidLinksRepository(BaseRepository):
    model = financial_uid_links

    async def list_by_uid(
        self, connection: AsyncConnection, uid: str
    ) -> list[Record]:
        result = await connection.execute(
            select(self.model)
            .where(self.model.c.uid == uid)
            .order_by(self.model.c.id)
        )
        return [self._record(row) for row in result]

    async def list_by_fid(
        self, connection: AsyncConnection, fid: str
    ) -> list[Record]:
        result = await connection.execute(
            select(self.model)
            .where(self.model.c.fid == fid)
            .order_by(self.model.c.id)
        )
        return [self._record(row) for row in result]

    async def list_companies_by_fid(
        self,
        connection: AsyncConnection,
        fid: str,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Record]:
        """Return companies linked to a person together with their profiles.

        A company without a row in ``company_profiles`` is retained in the
        response with profile fields set to ``None``.
        """
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must not be negative")

        result = await connection.execute(
            select(
                financial_uid_links.c.fid,
                financial_uid_links.c.uid.label("company_uid"),
                financial_uid_links.c.city_name,
                financial_uid_links.c.rc_name,
                company_profiles,
            )
            .select_from(
                financial_uid_links.outerjoin(
                    company_profiles,
                    company_profiles.c.uid == financial_uid_links.c.uid,
                )
            )
            .where(financial_uid_links.c.fid == fid)
            .order_by(financial_uid_links.c.uid, company_profiles.c.id)
            .limit(limit)
            .offset(offset)
        )
        return [self._record(row) for row in result]
