"""Orchestrates PSI + KS test + Z-score drift detection across all features."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from validation.psi_calculator import compute_psi, psi_severity
from .statistical_tests import ks_test, z_score_test, kl_divergence
from .baseline_manager import load_baseline
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
class FeatureDriftResult:
    feature_name: str
    psi_score: float
    ks_statistic: float
    ks_p_value: float
    z_score: float
    kl_divergence: float
    severity: str  # ok | warn | alert
    baseline_size: int
    current_size: int
    computed_at: float = field(default_factory=time.time)

    @property
    def is_drifting(self) -> bool:
        return self.severity in ("warn", "alert")


@dataclass
class DriftReport:
    feature_results: list[FeatureDriftResult] = field(default_factory=list)
    drifting_features: list[str] = field(default_factory=list)
    computed_at: float = field(default_factory=time.time)

    @property
    def has_critical_drift(self) -> bool:
        return any(r.severity == "alert" for r in self.feature_results)


def detect_drift(current_data: dict[str, list[float]]) -> DriftReport:
    """
    current_data: {feature_name: [list of recent values], ...}

    Loads baselines from Redis, computes PSI + KS + Z-score for each feature.
    """
    results: list[FeatureDriftResult] = []
    drifting: list[str] = []

    for feature_name in FEATURE_NAMES:
        current = current_data.get(feature_name, [])
        if not current:
            continue

        baseline = load_baseline(feature_name)
        if not baseline:
            logger.warning("No baseline for %s — using current as baseline", feature_name)
            baseline = current

        psi = compute_psi(baseline, current)
        severity = psi_severity(psi, config.psi_warn_threshold, config.psi_alert_threshold)
        ks = ks_test(baseline, current, alpha=config.ks_pvalue_threshold)
        zs = z_score_test(baseline, current, threshold=config.zscore_alert_threshold)
        kl = kl_divergence(baseline, current)

        # Escalate severity if KS test is also significant
        if severity == "ok" and ks.significant:
            severity = "warn"

        result = FeatureDriftResult(
            feature_name=feature_name,
            psi_score=psi,
            ks_statistic=ks.statistic,
            ks_p_value=ks.p_value,
            z_score=zs.z_score,
            kl_divergence=round(kl, 6),
            severity=severity,
            baseline_size=len(baseline),
            current_size=len(current),
        )
        results.append(result)

        if result.is_drifting:
            drifting.append(feature_name)
            logger.warning(
                "Drift detected: %s | PSI=%.4f | KS=%.4f (p=%.4f) | Z=%.2f | Severity=%s",
                feature_name, psi, ks.statistic, ks.p_value, zs.z_score, severity,
            )

    return DriftReport(feature_results=results, drifting_features=drifting)
