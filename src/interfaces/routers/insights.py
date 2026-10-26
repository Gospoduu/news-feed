from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from src.infra.redis.start_redis import redis_client
from src.interfaces.streams.analysis import publish_analysis_request
from src.services.insights import InsightsService


router = APIRouter(prefix="/companies", tags=["Insights"])
service = InsightsService(redis_client, publish_analysis_request)


@router.get("/{company_id}/insight")
async def get_company_insight(company_id: str):
    try:
        result = await service.get_company_insight(company_id)
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(error)) from error

    if result.processing:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": "processing", "company_id": company_id},
        )
    return result.response
