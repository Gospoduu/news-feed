# interfaces/routers/news.py
from fastapi import APIRouter, Query

from src.services.news_ingestion import NewsIngestionService

router = APIRouter(prefix="/news", tags=["news"])
service = NewsIngestionService()


@router.get("/feed")
async def get_news_feed(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return await service.get_news(limit=limit, offset=offset)