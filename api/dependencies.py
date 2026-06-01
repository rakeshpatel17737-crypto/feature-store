from __future__ import annotations

import redis.asyncio as aioredis
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from registry.database import AsyncSessionLocal
from online_store.config import config as redis_config


def get_redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
