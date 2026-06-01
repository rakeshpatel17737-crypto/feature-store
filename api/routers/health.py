from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from api.dependencies import get_redis, get_db
from api.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(redis=Depends(get_redis), db=Depends(get_db)):
    redis_status = "ok"
    db_status = "ok"

    try:
        await redis.ping()
    except Exception:
        redis_status = "error"

    try:
        await db.execute(__import__("sqlalchemy").text("SELECT 1"))
    except Exception:
        db_status = "error"

    overall = "healthy" if redis_status == "ok" and db_status == "ok" else "degraded"
    return HealthResponse(
        status=overall,
        redis=redis_status,
        db=db_status,
        timestamp=datetime.now(tz=timezone.utc),
    )


@router.get("/ready")
async def ready(redis=Depends(get_redis)):
    try:
        await redis.ping()
        return {"ready": True}
    except Exception:
        return JSONResponse(status_code=503, content={"ready": False})
