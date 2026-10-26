"""Idempotent import of the CSV datasets bundled with the project."""

import csv
from collections.abc import Iterable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from src.infra.models.public import (
    company_profiles,
    counterparties,
    financial_uid_links,
    payments,
)


DATA_DIRECTORY = Path(__file__).resolve().parents[2] / "data"
PROFILE_FILE = DATA_DIRECTORY / "df_Uid_data_hack.csv"
LINK_FILE = DATA_DIRECTORY / "df_Fid_Uid_link_hack.csv"
PAYMENT_FILE = DATA_DIRECTORY / "df_Uid_payment_hack.csv"
BATCH_SIZE = 5_000
# asyncpg limits one statement to 32,767 bind parameters; profiles have 25
# columns, so batches must stay below 1,310 rows.
PROFILE_BATCH_SIZE = 1_000


def _csv_rows(path: Path) -> Iterator[tuple[int, dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as source:
        yield from enumerate(csv.DictReader(source, delimiter=";"), start=2)


def _text(row: dict[str, str], column: str, *, required: bool = False) -> str | None:
    value = row[column].strip()
    if value:
        return value
    if required:
        raise ValueError(f"{column} is empty")
    return None


def _flag(row: dict[str, str], column: str) -> bool:
    value = _text(row, column, required=True)
    if value == "1.0":
        return True
    if value == "0.0":
        return False
    raise ValueError(f"{column} must contain 0.0 or 1.0, got {value!r}")


def _batches(
    items: Iterable[dict[str, Any]], batch_size: int = BATCH_SIZE
) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for item in items:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


async def _upsert(
    connection: AsyncConnection,
    table: Any,
    rows: Iterable[dict[str, Any]],
    conflict_columns: list[str],
    *,
    update_existing: bool = True,
    batch_size: int = BATCH_SIZE,
) -> None:
    """Write CSV rows in bounded batches and make reruns safe."""
    for batch in _batches(rows, batch_size):
        statement = insert(table).values(batch)
        if not update_existing:
            await connection.execute(
                statement.on_conflict_do_nothing(index_elements=conflict_columns)
            )
            continue
        update_values = {
            column.name: statement.excluded[column.name]
            for column in table.c
            if column.name not in {"id", *conflict_columns}
        }
        changed = or_(
            *(
                table.c[column].is_distinct_from(statement.excluded[column])
                for column in update_values
            )
        )
        await connection.execute(
            statement.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_values,
                where=changed,
            )
        )


def _counterparty_rows() -> Iterator[dict[str, str]]:
    for _, row in _csv_rows(PROFILE_FILE):
        yield {"uid": _text(row, "SYNTHETIC_U", required=True)}
    for _, row in _csv_rows(LINK_FILE):
        yield {"uid": _text(row, "SYNTHETIC_U", required=True)}
    for _, row in _csv_rows(PAYMENT_FILE):
        yield {"uid": _text(row, "BENEFICIARY", required=True)}
        yield {"uid": _text(row, "PAYER", required=True)}


def _profile_rows() -> Iterator[dict[str, Any]]:
    for source_row, row in _csv_rows(PROFILE_FILE):
        yield {
            "source_row": source_row,
            "uid": _text(row, "SYNTHETIC_U", required=True),
            "org_type": _text(row, "ORG_TYPE", required=True),
            "okfs_name": _text(row, "OKFS_NAME", required=True),
            "company_age_years_gradation": _text(
                row, "COMPANY_AGE_YEARS_GRADATION", required=True
            ),
            "eco_mark_label": _text(row, "ECO_MARK_LABEL"),
            "main_okved_code": _text(row, "MAINOKVED_CODE", required=True),
            "main_okved_name": _text(row, "MAINOKVED_NAME", required=True),
            "negative_mark": _flag(row, "NEGATIVEMARK"),
            "amount_workers_fns_gradation": _text(
                row, "AMOUNT_WORKERS_FNS_GRADATION", required=True
            ),
            "amount_workers_spark_gradation": _text(
                row, "AMOUNT_WORKERS_SPARK_GRADATION"
            ),
            "client_type": _text(row, "CL_TYPE", required=True),
            "product_rko_mark": _flag(row, "CL_PRODUCT_RKO_MARK"),
            "product_gar_mark": _flag(row, "CL_PRODUCT_GAR_MARK"),
            "product_loa_mark": _flag(row, "CL_PRODUCT_LOA_MARK"),
            "product_lea_mark": _flag(row, "CL_PRODUCT_LEA_MARK"),
            "product_dep_mark": _flag(row, "CL_PRODUCT_DEP_MARK"),
            "product_zpp_mark": _flag(row, "CL_PRODUCT_ZPP_MARK"),
            "fts_flag": _flag(row, "FTS_FLAG"),
            "fts_country_name": _text(row, "FTS_COUNTRY_NAME"),
            "factoring_mark": _flag(row, "FACTORING_MARK"),
            "revenue_company": Decimal(
                _text(row, "REVENUE_COMPANY", required=True)
            ),
            "group_id": _text(row, "GROUP_ID", required=True),
            "city": _text(row, "CITY", required=True),
            "region": _text(row, "REGION", required=True),
        }


def _link_rows() -> Iterator[dict[str, Any]]:
    for source_row, row in _csv_rows(LINK_FILE):
        yield {
            "source_row": source_row,
            "fid": _text(row, "SYNTHETIC_F", required=True),
            "uid": _text(row, "SYNTHETIC_U", required=True),
            "city_name": _text(row, "CITY_NAME"),
            "rc_name": _text(row, "RC_NAME"),
        }


def _payment_rows() -> Iterator[dict[str, Any]]:
    for source_row, row in _csv_rows(PAYMENT_FILE):
        yield {
            "source_row": source_row,
            "beneficiary_uid": _text(row, "BENEFICIARY", required=True),
            "payer_uid": _text(row, "PAYER", required=True),
            "payment_amount": Decimal(_text(row, "PAY_AMT", required=True)),
            "payment_count": int(_text(row, "PAY_CNT", required=True)),
        }


async def seed_database(connection: AsyncConnection) -> None:
    """Import the bundled datasets, updating changed source rows in place."""
    await _upsert(
        connection,
        counterparties,
        _counterparty_rows(),
        ["uid"],
        update_existing=False,
    )
    # Keep the multi-value INSERT below asyncpg's 32,767 bind-parameter limit.
    await _upsert(
        connection,
        company_profiles,
        _profile_rows(),
        ["source_row"],
        batch_size=PROFILE_BATCH_SIZE,
    )
    await _upsert(connection, financial_uid_links, _link_rows(), ["source_row"])
    await _upsert(connection, payments, _payment_rows(), ["source_row"])
