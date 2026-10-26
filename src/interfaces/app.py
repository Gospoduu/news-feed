"""HTTP API and Kafka consumers for the news-feed service."""

from contextlib import asynccontextmanager
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.infra.db_engine import engine, init_db
from src.infra.redis.start_redis import redis_client
from src.interfaces.routers.counterparties import router as counterparties_router
from src.interfaces.routers.insights import router as insights_router
from src.interfaces.routers.news import router as news_router
from src.interfaces.streams import analysis as _analysis  # Registers subscriber.
from src.interfaces.streams import news as _news  # Registers subscriber.
from src.interfaces.streams.broker import broker


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    await broker.start()
    try:
        yield
    finally:
        await broker.stop()
        await redis_client.aclose()
        await engine.dispose()


app = FastAPI(lifespan=lifespan)
cors_origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8081,http://127.0.0.1:8081",
    ).split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(counterparties_router)
app.include_router(insights_router)
app.include_router(news_router)
