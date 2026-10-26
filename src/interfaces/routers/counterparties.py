from fastapi import APIRouter, Query

from src.infra.db_engine import get_connection
from src.services.counterparties import Counterparties


router = APIRouter(prefix="/persons", tags=["Persons"])
service = Counterparties(get_connection)


@router.get("/{person_id}/companies")
async def get_person_companies(
    person_id: str,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """Return company profiles linked to a physical person's FID."""
    return await service.get_companies_by_person_id(
        person_id,
        limit=limit,
        offset=offset,
    )
