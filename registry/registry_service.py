from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .models import FeatureDefinition, FeatureMetric, RCAResult
from .schemas import FeatureMetricCreate, RCAResultCreate

logger = logging.getLogger(__name__)


async def get_all_features(db: AsyncSession) -> list[FeatureDefinition]:
    result = await db.execute(
        select(FeatureDefinition)
        .where(FeatureDefinition.is_active == True)
        .options(selectinload(FeatureDefinition.lineage))
        .order_by(FeatureDefinition.feature_name)
    )
    return list(result.scalars().all())


async def get_feature(db: AsyncSession, feature_name: str) -> FeatureDefinition | None:
    result = await db.execute(
        select(FeatureDefinition)
        .where(FeatureDefinition.feature_name == feature_name)
        .options(selectinload(FeatureDefinition.lineage))
    )
    return result.scalar_one_or_none()


async def record_metric(db: AsyncSession, metric: FeatureMetricCreate) -> None:
    db.add(FeatureMetric(
        feature_name=metric.feature_name,
        metric_type=metric.metric_type,
        metric_value=metric.metric_value,
        recorded_at=datetime.utcnow(),
    ))
    await db.commit()


async def record_rca(db: AsyncSession, rca: RCAResultCreate) -> RCAResult:
    obj = RCAResult(
        feature_name=rca.feature_name,
        probable_cause=rca.probable_cause,
        cause_category=rca.cause_category,
        confidence=rca.confidence,
        urgency=rca.urgency,
        remediation_steps=json.dumps(rca.remediation_steps),
        estimated_impact=rca.estimated_impact,
        model_used=rca.model_used,
        tokens_used=rca.tokens_used,
        computed_at=datetime.utcnow(),
    )
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return obj


async def get_recent_metrics(
    db: AsyncSession,
    feature_name: str,
    metric_type: str,
    limit: int = 100,
) -> list[FeatureMetric]:
    result = await db.execute(
        select(FeatureMetric)
        .where(
            FeatureMetric.feature_name == feature_name,
            FeatureMetric.metric_type == metric_type,
        )
        .order_by(FeatureMetric.recorded_at.desc())
        .limit(limit)
    )
    return list(result.scalars().all())
