from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import config
from api.middleware import RequestContextMiddleware
from api.routers import features, health, metadata, admin
from online_store.config import config as redis_config
from registry.database import create_tables
from registry.seed_data import seed_features
from registry.database import AsyncSessionLocal

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        logging.getLevelName(config.log_level)
    ),
)
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Feature Store API starting", environment=config.environment)

    # Redis connection pool
    pool = aioredis.ConnectionPool.from_url(
        f"redis://{redis_config.redis_host}:{redis_config.redis_port}/{redis_config.redis_db}",
        max_connections=redis_config.redis_max_connections,
        decode_responses=True,
    )
    app.state.redis = aioredis.Redis(connection_pool=pool)

    # Database tables + seed
    await create_tables()
    async with AsyncSessionLocal() as db:
        await seed_features(db)

    logger.info("Feature Store API ready")
    yield

    await app.state.redis.aclose()
    logger.info("Feature Store API shutdown")


app = FastAPI(
    title="Feature Store API",
    description="Enterprise real-time feature store for AI/ML systems",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(RequestContextMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(features.router)
app.include_router(metadata.router)
app.include_router(admin.router)
