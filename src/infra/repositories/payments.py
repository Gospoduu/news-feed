from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import payments
from src.infra.repositories.base import BaseRepository, Record


class PaymentsRepository(BaseRepository):
    model = payments

    async def list_by_beneficiary(
        self, connection: AsyncConnection, uid: str
    ) -> list[Record]:
        result = await connection.execute(
            select(self.model)
            .where(self.model.c.beneficiary_uid == uid)
            .order_by(self.model.c.id)
        )
        return [self._record(row) for row in result]

    async def list_by_payer(
        self, connection: AsyncConnection, uid: str
    ) -> list[Record]:
        result = await connection.execute(
            select(self.model)
            .where(self.model.c.payer_uid == uid)
            .order_by(self.model.c.id)
        )
        return [self._record(row) for row in result]
