"""Training-serving skew detection — wraps consistency validation with alerting."""
from __future__ import annotations

import logging
import time
from typing import Optional

from .consistency_validator import ConsistencyReport

logger = logging.getLogger(__name__)

SKEW_SEVERITY_THRESHOLDS = {
    "critical": 0.10,  # > 10% inconsistency
    "high": 0.05,
    "medium": 0.02,
    "low": 0.0,
}


def classify_skew_severity(report: ConsistencyReport) -> str:
    rate = report.inconsistency_rate
    for level, threshold in SKEW_SEVERITY_THRESHOLDS.items():
        if rate > threshold:
            return level
    return "none"


def emit_skew_alert(report: ConsistencyReport, kafka_producer=None) -> None:
    severity = classify_skew_severity(report)
    if severity == "none":
        return

    alert = {
        "alert_type": "training_serving_skew",
        "severity": severity,
        "inconsistency_rate": report.inconsistency_rate,
        "worst_feature": report.worst_feature,
        "max_deviation": report.max_deviation,
        "sample_size": report.sample_size,
        "timestamp": time.time(),
    }

    logger.warning("SKEW ALERT [%s]: %s", severity.upper(), alert)

    if kafka_producer:
        import json
        kafka_producer.produce(
            topic="ecommerce.alerts.drift",
            key="skew_alert",
            value=json.dumps(alert).encode(),
        )
        kafka_producer.flush()
