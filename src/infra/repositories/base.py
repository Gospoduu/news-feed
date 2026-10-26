"""Reusable CRUD repository for SQLAlchemy Core tables."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import Column, Table, delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection


Record = dict[str, Any]


class BaseRepository:
    """Common CRUD operations for a table with a single primary key."""

    model: Table
    primary_key = "id"

    @property
    def pk(self) -> Column[Any]:
        return self.model.c[self.primary_key]

    @staticmethod
    def _record(row: Any) -> Record:
        return dict(row._mapping)

    async def create(self, connection: AsyncConnection, **values: Any) -> Record:
        """Insert one record and return its database representation."""
        result = await connection.execute(
            insert(self.model).values(**values).returning(self.model)
        )
        return self._record(result.one())

    async def get_one(
        self, connection: AsyncConnection, key: Any
    ) -> Record | None:
        """Return one record by the repository primary key."""
        result = await connection.execute(
            select(self.model).where(self.pk == key)
        )
        row = result.one_or_none()
        return self._record(row) if row is not None else None

    async def get_one_by_id(
        self, connection: AsyncConnection, key: Any
    ) -> Record | None:
        """Compatibility alias for repositories using an ``id`` primary key."""
        return await self.get_one(connection, key)

    async def list(
        self,
        connection: AsyncConnection,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Record]:
        """Return a deterministic page ordered by the primary key."""
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must not be negative")
        result = await connection.execute(
            select(self.model)
            .order_by(self.pk)
            .limit(limit)
            .offset(offset)
        )
        return [self._record(row) for row in result]

    async def update(
        self, connection: AsyncConnection, key: Any, **values: Any
    ) -> Record | None:
        """Update one record and return it, or return ``None`` when absent."""
        if not values:
            raise ValueError("at least one field is required for an update")
        result = await connection.execute(
            update(self.model)
            .where(self.pk == key)
            .values(**values)
            .returning(self.model)
        )
        row = result.one_or_none()
        return self._record(row) if row is not None else None

    async def delete(self, connection: AsyncConnection, key: Any) -> bool:
        """Delete one record and report whether it existed."""
        result = await connection.execute(
            delete(self.model).where(self.pk == key)
        )
        return result.rowcount == 1

    async def delete_by_id(self, connection: AsyncConnection, key: Any) -> bool:
        """Compatibility alias for repositories using an ``id`` primary key."""
        return await self.delete(connection, key)

    async def create_many(
        self, connection: AsyncConnection, records: Sequence[Mapping[str, Any]]
    ) -> list[Record]:
        """Insert a small explicit batch and return the created records."""
        if not records:
            return []
        result = await connection.execute(
            insert(self.model).values([dict(record) for record in records])
            .returning(self.model)
        )
        return [self._record(row) for row in result]
