"""Assembles rich diagnostic context for the LLM RCA engine."""
from __future__ import annotations

import json
import time

from .schemas import DriftReport


def build_rca_context(report: DriftReport, feature_metadata: dict | None = None) -> str:
    """Build a structured JSON context string for the LLM."""
    context = {
        "feature_name": report.feature_name,
        "drift_metrics": {
            "psi_score": report.psi_score,
            "psi_interpretation": _interpret_psi(report.psi_score),
            "ks_statistic": report.ks_statistic,
            "ks_p_value": report.ks_p_value,
            "ks_significant": report.ks_p_value < 0.05,
            "z_score": report.z_score,
            "z_score_anomalous": abs(report.z_score) > 3.0,
            "kl_divergence": report.kl_divergence,
        },
        "severity": report.severity,
        "time_context": {
            "baseline_period": report.baseline_period,
            "current_period": report.current_period,
            "sample_size": report.sample_size,
            "analysis_timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(report.computed_at)
            ),
        },
    }

    if feature_metadata:
        context["feature_metadata"] = {
            "description": feature_metadata.get("description"),
            "transform_logic": feature_metadata.get("transform_logic"),
            "window_type": feature_metadata.get("window_type"),
            "freshness_sla_seconds": feature_metadata.get("freshness_sla_seconds"),
            "owner": feature_metadata.get("owner"),
        }

    return json.dumps(context, indent=2)


def _interpret_psi(psi: float) -> str:
    if psi < 0.1:
        return "no significant shift"
    if psi < 0.2:
        return "moderate shift — monitor closely"
    return "major shift — investigate immediately"
