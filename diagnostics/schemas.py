from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal, Optional


@dataclass
class DriftReport:
    feature_name: str
    psi_score: float
    ks_statistic: float
    ks_p_value: float
    z_score: float
    severity: str
    baseline_period: str
    current_period: str
    sample_size: int
    kl_divergence: float = 0.0
    computed_at: float = field(default_factory=time.time)


@dataclass
class RCADiagnosis:
    feature_name: str
    probable_cause: str
    cause_category: str
    confidence: float
    affected_features: list[str]
    remediation_steps: list[str]
    urgency: str
    estimated_impact: str
    model_used: str
    tokens_used: int = 0
    computed_at: float = field(default_factory=time.time)
    fallback_used: bool = False
