"""
Offline vs Online feature consistency validator.

Compares feature values stored in Delta Lake (offline) vs Redis (online)
for a random sample of users, detecting training-serving skew.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .config import config

logger = logging.getLogger(__name__)

FEATURE_NAMES = [
    "txn_count_5m",
    "avg_spend_1h",
    "session_activity_rate",
    "cart_abandon_ratio",
    "product_interaction_freq",
    "anomaly_score",
]


@dataclass
class ConsistencyReport:
    sample_size: int
    consistent_users: int
    inconsistent_users: int
    inconsistency_rate: float
    per_feature_deviation: dict[str, float] = field(default_factory=dict)
    worst_feature: Optional[str] = None
    max_deviation: float = 0.0
    computed_at: float = field(default_factory=time.time)

    @property
    def is_healthy(self) -> bool:
        return self.inconsistency_rate <= config.consistency_threshold


def _relative_deviation(offline_val: float, online_val: float) -> float:
    denom = abs(offline_val) + 1e-6
    return abs(offline_val - online_val) / denom


def validate_consistency(
    offline_features: dict[str, dict[str, float]],
    online_features: dict[str, dict[str, float]],
) -> ConsistencyReport:
    """
    offline_features: {user_id: {feature_name: value, ...}, ...}
    online_features:  {user_id: {feature_name: value, ...}, ...}
    """
    sample_size = len(offline_features)
    inconsistent_users = 0
    feature_deviations: dict[str, list[float]] = {f: [] for f in FEATURE_NAMES}

    for user_id, offline_vals in offline_features.items():
        online_vals = online_features.get(user_id, {})
        user_inconsistent = False

        for feature in FEATURE_NAMES:
            offline_val = float(offline_vals.get(feature, 0.0))
            online_val = float(online_vals.get(feature, 0.0))
            deviation = _relative_deviation(offline_val, online_val)
            feature_deviations[feature].append(deviation)

            if deviation > config.consistency_threshold:
                user_inconsistent = True

        if user_inconsistent:
            inconsistent_users += 1

    per_feature_deviation = {
        f: round(sum(devs) / max(len(devs), 1), 6)
        for f, devs in feature_deviations.items()
    }

    worst_feature = max(per_feature_deviation, key=per_feature_deviation.get) if per_feature_deviation else None
    max_deviation = per_feature_deviation.get(worst_feature, 0.0) if worst_feature else 0.0
    inconsistency_rate = inconsistent_users / max(sample_size, 1)

    report = ConsistencyReport(
        sample_size=sample_size,
        consistent_users=sample_size - inconsistent_users,
        inconsistent_users=inconsistent_users,
        inconsistency_rate=round(inconsistency_rate, 6),
        per_feature_deviation=per_feature_deviation,
        worst_feature=worst_feature,
        max_deviation=max_deviation,
    )

    if not report.is_healthy:
        logger.warning(
            "Consistency alert: %.1f%% inconsistent users. Worst feature: %s (deviation=%.4f)",
            inconsistency_rate * 100,
            worst_feature,
            max_deviation,
        )
    else:
        logger.info(
            "Consistency OK: %.2f%% inconsistency rate (threshold=%.1f%%)",
            inconsistency_rate * 100,
            config.consistency_threshold * 100,
        )

    return report
