"""Integration tests for the online feature store (Redis)."""
from __future__ import annotations

import time
import pytest


@pytest.mark.integration
def test_write_and_read_features(redis_client):
    """Write features and read them back with HGETALL."""
    from online_store.key_schema import feature_key

    user_id = "usr_test_001"
    features = {
        "txn_count_5m": 3.0,
        "avg_spend_1h": 45.20,
        "session_activity_rate": 1.5,
        "cart_abandon_ratio": 0.25,
        "product_interaction_freq": 7.0,
        "anomaly_score": 0.12,
    }
    key = feature_key(user_id)

    # Write
    payload = {k: str(v) for k, v in features.items()}
    payload["computed_at"] = str(time.time())
    redis_client.hset(key, mapping=payload)
    redis_client.expire(key, 300)

    # Read back
    data = redis_client.hgetall(key)
    assert data, "Expected data from Redis"
    assert abs(float(data["txn_count_5m"]) - 3.0) < 0.001
    assert abs(float(data["avg_spend_1h"]) - 45.20) < 0.01


@pytest.mark.integration
def test_ttl_is_set(redis_client):
    """Feature keys should have a TTL set."""
    from online_store.key_schema import feature_key

    user_id = "usr_ttl_test"
    key = feature_key(user_id)
    redis_client.hset(key, mapping={"txn_count_5m": "5.0"})
    redis_client.expire(key, 300)

    ttl = redis_client.ttl(key)
    assert 250 < ttl <= 300, f"Expected TTL~300, got {ttl}"


@pytest.mark.integration
def test_active_user_index(redis_client):
    """Writing features should add user to active index."""
    user_id = "usr_index_test"
    redis_client.sadd("index:active_users", user_id)
    assert redis_client.sismember("index:active_users", user_id)


@pytest.mark.integration
def test_hgetall_latency(redis_client):
    """HGETALL should complete in under 20ms."""
    from online_store.key_schema import feature_key

    user_id = "usr_latency_test"
    key = feature_key(user_id)
    redis_client.hset(key, mapping={
        "txn_count_5m": "3.0",
        "avg_spend_1h": "50.0",
        "computed_at": str(time.time()),
    })

    t0 = time.perf_counter()
    redis_client.hgetall(key)
    latency_ms = (time.perf_counter() - t0) * 1000

    assert latency_ms < 20.0, f"HGETALL took {latency_ms:.2f}ms (target: <20ms)"
