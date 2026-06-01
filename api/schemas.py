from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class FeatureVector(BaseModel):
    txn_count_5m: float = 0.0
    avg_spend_1h: float = 0.0
    session_activity_rate: float = 0.0
    cart_abandon_ratio: float = 0.0
    product_interaction_freq: float = 0.0
    anomaly_score: float = 0.0


class FeatureMetadata(BaseModel):
    computed_at: Optional[str] = None
    freshness_seconds: Optional[float] = None
    source: str = "online_store"
    schema_version: str = "1.0"


class FeatureResponse(BaseModel):
    user_id: str
    features: FeatureVector
    metadata: FeatureMetadata


class HealthResponse(BaseModel):
    status: str
    redis: str
    db: str
    timestamp: datetime


class RefreshRequest(BaseModel):
    user_id: str
    feature_names: Optional[list[str]] = None


class RefreshResponse(BaseModel):
    status: str
    user_id: str
    queued_features: list[str]


class RCARequest(BaseModel):
    feature_name: str
    psi_score: float
    ks_statistic: float
    z_score: float
    severity: str
    additional_context: Optional[dict] = None


class RCAResponse(BaseModel):
    feature_name: str
    probable_cause: str
    cause_category: str
    confidence: float
    affected_features: list[str]
    remediation_steps: list[str]
    urgency: str
    estimated_impact: str
    model_used: str
