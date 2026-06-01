from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_redis
from api.schemas import FeatureResponse, FeatureVector, FeatureMetadata
from online_store.key_schema import feature_key

router = APIRouter(tags=["features"])


@router.get("/features/{user_id}", response_model=FeatureResponse)
async def get_features(
    user_id: str,
    features: Optional[str] = Query(default=None, description="Comma-separated feature names"),
    include_metadata: bool = Query(default=True),
    redis=Depends(get_redis),
):
    raw: dict = await redis.hgetall(feature_key(user_id))

    if not raw:
        raise HTTPException(status_code=404, detail=f"No features found for user {user_id}")

    def _f(key: str) -> float:
        return float(raw.get(key, 0.0))

    vector = FeatureVector(
        txn_count_5m=_f("txn_count_5m"),
        avg_spend_1h=_f("avg_spend_1h"),
        session_activity_rate=_f("session_activity_rate"),
        cart_abandon_ratio=_f("cart_abandon_ratio"),
        product_interaction_freq=_f("product_interaction_freq"),
        anomaly_score=_f("anomaly_score"),
    )

    # Filter to requested features only
    if features:
        requested = set(features.split(","))
        vector_dict = vector.model_dump()
        filtered = {k: v for k, v in vector_dict.items() if k in requested}
        vector = FeatureVector(**{**{k: 0.0 for k in FeatureVector.model_fields}, **filtered})

    computed_at = raw.get("computed_at")
    freshness_seconds: float | None = None
    if computed_at:
        try:
            freshness_seconds = round(time.time() - float(computed_at), 1)
        except ValueError:
            pass

    metadata = FeatureMetadata(
        computed_at=computed_at,
        freshness_seconds=freshness_seconds,
    ) if include_metadata else FeatureMetadata()

    return FeatureResponse(user_id=user_id, features=vector, metadata=metadata)
