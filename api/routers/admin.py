from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_redis, get_db
from api.schemas import RefreshRequest, RefreshResponse, RCARequest, RCAResponse
from online_store.key_schema import feature_key, FEATURE_FIELDS

router = APIRouter(tags=["admin"])


async def _do_refresh(user_id: str, redis) -> None:
    """Background task: extend TTL for a user's feature key."""
    key = feature_key(user_id)
    await redis.expire(key, 300)


@router.post("/refresh-feature", response_model=RefreshResponse, status_code=202)
async def refresh_feature(
    body: RefreshRequest,
    background_tasks: BackgroundTasks,
    redis=Depends(get_redis),
):
    queued = body.feature_names or FEATURE_FIELDS[:-1]
    background_tasks.add_task(_do_refresh, body.user_id, redis)
    return RefreshResponse(
        status="refresh_queued",
        user_id=body.user_id,
        queued_features=queued,
    )


@router.post("/diagnostics/rca", response_model=RCAResponse)
async def run_rca(
    body: RCARequest,
    db: AsyncSession = Depends(get_db),
):
    from diagnostics.rca_engine import rca_engine
    from diagnostics.schemas import DriftReport

    drift_report = DriftReport(
        feature_name=body.feature_name,
        psi_score=body.psi_score,
        ks_statistic=body.ks_statistic,
        ks_p_value=0.0,
        z_score=body.z_score,
        severity=body.severity,
        baseline_period="last_7d",
        current_period="last_1h",
        sample_size=1000,
    )

    diagnosis = await rca_engine.analyze(drift_report, db=db)

    return RCAResponse(
        feature_name=diagnosis.feature_name,
        probable_cause=diagnosis.probable_cause,
        cause_category=diagnosis.cause_category,
        confidence=diagnosis.confidence,
        affected_features=diagnosis.affected_features,
        remediation_steps=diagnosis.remediation_steps,
        urgency=diagnosis.urgency,
        estimated_impact=diagnosis.estimated_impact,
        model_used=diagnosis.model_used,
    )
