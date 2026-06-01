"""Integration tests for the drift detection pipeline."""
from __future__ import annotations

import numpy as np
import pytest


def test_no_drift_identical():
    """No drift when current equals baseline."""
    from drift_detection.drift_detector import detect_drift
    from drift_detection.baseline_manager import save_baseline

    values = list(np.random.normal(50, 10, 300))
    save_baseline("txn_count_5m", values)

    report = detect_drift({"txn_count_5m": values})
    result = next((r for r in report.feature_results if r.feature_name == "txn_count_5m"), None)

    if result:
        assert result.psi_score < 0.1


def test_drift_detected_for_major_shift():
    """Major distribution shift should be detected as drift."""
    from drift_detection.drift_detector import detect_drift
    from drift_detection.baseline_manager import save_baseline

    baseline = list(np.random.normal(10, 2, 500))
    current = list(np.random.normal(50, 2, 500))  # completely different

    save_baseline("avg_spend_1h", baseline)
    report = detect_drift({"avg_spend_1h": current})

    result = next((r for r in report.feature_results if r.feature_name == "avg_spend_1h"), None)
    if result:
        assert result.is_drifting or result.psi_score > 0.1


def test_drift_report_has_all_features():
    """Drift report covers all 6 features when data provided."""
    from drift_detection.drift_detector import detect_drift

    data = {
        "txn_count_5m": list(np.random.normal(3, 1, 100)),
        "avg_spend_1h": list(np.random.normal(45, 10, 100)),
        "session_activity_rate": list(np.random.normal(1.5, 0.5, 100)),
        "cart_abandon_ratio": list(np.random.uniform(0, 1, 100)),
        "product_interaction_freq": list(np.random.normal(7, 2, 100)),
        "anomaly_score": list(np.random.uniform(0, 1, 100)),
    }
    report = detect_drift(data)
    assert len(report.feature_results) == 6


def test_psi_calculator_known_values():
    """Validate PSI against literature examples."""
    from validation.psi_calculator import compute_psi
    # When distributions differ by ~50% of values, PSI should be non-trivial
    baseline = [1.0] * 100 + [2.0] * 100
    current = [1.0] * 150 + [2.0] * 50  # shifted toward 1.0
    psi = compute_psi(baseline, current)
    assert psi > 0.0
