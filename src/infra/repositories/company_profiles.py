from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import company_profiles
from src.infra.repositories.base import BaseRepository, Record


class CompanyProfilesRepository(BaseRepository):
    model = company_profiles

    async def list_by_uid(
        self, connection: AsyncConnection, uid: str
    ) -> list[Record]:
        result = await connection.execute(
            select(self.model)
            .where(self.model.c.uid == uid)
            .order_by(self.model.c.id)
        )
        return [self._record(row) for row in result]
