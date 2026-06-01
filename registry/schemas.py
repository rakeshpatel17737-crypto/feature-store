from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class LineageItem(BaseModel):
    upstream_source: str
    transform_step: str

    model_config = {"from_attributes": True}


class FeatureDefinitionResponse(BaseModel):
    id: int
    feature_name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    transform_logic: Optional[str] = None
    window_type: Optional[str] = None
    freshness_sla_seconds: int = 300
    owner: Optional[str] = None
    version: str = "1.0"
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    lineage: list[LineageItem] = []

    model_config = {"from_attributes": True}


class FeatureMetricCreate(BaseModel):
    feature_name: str
    metric_type: str
    metric_value: float


class RCAResultCreate(BaseModel):
    feature_name: str
    probable_cause: str
    cause_category: str
    confidence: float
    urgency: str
    remediation_steps: list[str]
    estimated_impact: str
    model_used: str
    tokens_used: int
