"""Unit tests for Redis key schema and patterns."""
from __future__ import annotations

import re


def test_feature_key_format():
    from online_store.key_schema import feature_key
    key = feature_key("usr_00123")
    assert key == "features:usr_00123"


def test_feature_meta_key_format():
    from online_store.key_schema import feature_meta_key
    key = feature_meta_key("usr_00123")
    assert key == "features:usr_00123:meta"


def test_baseline_key_format():
    from online_store.key_schema import baseline_key
    key = baseline_key("txn_count_5m")
    assert key == "baseline:dist:txn_count_5m"


def test_feature_fields_complete():
    from online_store.key_schema import FEATURE_FIELDS
    expected = {
        "txn_count_5m", "avg_spend_1h", "session_activity_rate",
        "cart_abandon_ratio", "product_interaction_freq", "anomaly_score", "computed_at",
    }
    assert set(FEATURE_FIELDS) == expected


def test_feature_key_no_colon_in_user_id():
    """Keys must not have extra colons that break Redis patterns."""
    from online_store.key_schema import feature_key
    key = feature_key("usr_99999")
    parts = key.split(":")
    assert parts[0] == "features"
    assert parts[1] == "usr_99999"
    assert len(parts) == 2
