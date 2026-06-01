from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .config import config
from .models import Base

# Async engine for FastAPI
async_engine = create_async_engine(
    config.database_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)
AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False)

# Sync engine for Airflow / scripts
sync_engine = create_engine(config.sync_database_url, pool_pre_ping=True)
SyncSessionLocal = sessionmaker(bind=sync_engine, autocommit=False, autoflush=False)


async def create_tables() -> None:
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_async_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


def get_sync_session() -> Session:
    with SyncSessionLocal() as session:
        yield session
