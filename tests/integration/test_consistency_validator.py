"""Integration tests for the offline vs online consistency validator."""
from __future__ import annotations

import pytest


def make_feature_dict(base_val: float = 5.0) -> dict:
    return {
        "txn_count_5m": base_val,
        "avg_spend_1h": base_val * 10,
        "session_activity_rate": base_val * 0.3,
        "cart_abandon_ratio": 0.2,
        "product_interaction_freq": base_val * 2,
        "anomaly_score": 0.05,
    }


def test_perfect_consistency():
    """Identical offline and online features should give 0% inconsistency."""
    from validation.consistency_validator import validate_consistency

    users = {f"usr_{i:05d}": make_feature_dict(float(i)) for i in range(1, 101)}
    report = validate_consistency(users, users)

    assert report.inconsistency_rate == 0.0
    assert report.inconsistent_users == 0
    assert report.is_healthy


def test_total_inconsistency():
    """Completely different values should give 100% inconsistency."""
    from validation.consistency_validator import validate_consistency

    offline = {f"usr_{i:05d}": make_feature_dict(float(i)) for i in range(1, 101)}
    online = {f"usr_{i:05d}": make_feature_dict(float(i) * 1000) for i in range(1, 101)}

    report = validate_consistency(offline, online)
    assert report.inconsistency_rate > 0.9
    assert not report.is_healthy


def test_partial_inconsistency():
    """50% inconsistent users should be detected."""
    from validation.consistency_validator import validate_consistency

    offline = {f"usr_{i:05d}": make_feature_dict(float(i)) for i in range(1, 101)}
    online = {}
    for i in range(1, 101):
        uid = f"usr_{i:05d}"
        if i <= 50:
            online[uid] = make_feature_dict(float(i))  # consistent
        else:
            online[uid] = make_feature_dict(float(i) * 100)  # inconsistent

    report = validate_consistency(offline, online)
    assert 0.3 < report.inconsistency_rate < 0.7


def test_worst_feature_identified():
    """The worst feature (highest deviation) should be identified."""
    from validation.consistency_validator import validate_consistency

    offline = {"usr_00001": make_feature_dict(10.0)}
    online = {"usr_00001": {
        "txn_count_5m": 10.0,  # same
        "avg_spend_1h": 10.0,  # same (offline=100, big diff)
        "session_activity_rate": 3.0,
        "cart_abandon_ratio": 0.2,
        "product_interaction_freq": 20.0,
        "anomaly_score": 0.05,
    }}
    offline["usr_00001"]["avg_spend_1h"] = 1000.0  # large deviation

    report = validate_consistency(offline, online)
    assert report.worst_feature is not None
