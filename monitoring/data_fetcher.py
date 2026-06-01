"""Data fetching utilities for the Streamlit dashboard."""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import redis as redis_lib
import requests
from sqlalchemy import create_engine, text

from .config import config

_redis_client: Optional[redis_lib.Redis] = None
_db_engine = None


def get_redis() -> redis_lib.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis_lib.Redis(
            host=config.redis_host,
            port=config.redis_port,
            decode_responses=True,
            socket_connect_timeout=3,
        )
    return _redis_client


def get_db_engine():
    global _db_engine
    if _db_engine is None:
        url = (
            f"postgresql+psycopg2://{config.postgres_user}:{config.postgres_password}"
            f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_db}"
        )
        _db_engine = create_engine(url, pool_pre_ping=True)
    return _db_engine


def fetch_feature_freshness(sample_n: int = 200) -> pd.DataFrame:
    """Returns DataFrame with user_id and freshness_seconds for sampled users."""
    client = get_redis()
    user_ids = client.srandmember("index:active_users", sample_n)

    records = []
    now = time.time()
    for uid in user_ids:
        data = client.hgetall(f"features:{uid}")
        if data and "computed_at" in data:
            try:
                age = now - float(data["computed_at"])
                records.append({"user_id": uid, "freshness_seconds": round(age, 1)})
            except ValueError:
                pass

    return pd.DataFrame(records) if records else pd.DataFrame(columns=["user_id", "freshness_seconds"])


def fetch_active_user_count() -> int:
    client = get_redis()
    return client.scard("index:active_users")


def fetch_drift_metrics(hours: int = 24) -> pd.DataFrame:
    """Load PSI + KS metrics from PostgreSQL for the last N hours."""
    try:
        engine = get_db_engine()
        query = text("""
            SELECT feature_name, metric_type, metric_value, recorded_at
            FROM feature_metrics
            WHERE metric_type IN ('psi_score', 'ks_statistic', 'consistency_rate')
              AND recorded_at > NOW() - INTERVAL ':hours hours'
            ORDER BY recorded_at DESC
            LIMIT 2000
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query.bindparams(hours=hours), conn)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["feature_name", "metric_type", "metric_value", "recorded_at"])


def fetch_api_health() -> dict:
    try:
        resp = requests.get(f"{config.feature_store_api_url}/health", timeout=3)
        return resp.json()
    except Exception:
        return {"status": "unreachable", "redis": "unknown", "db": "unknown"}


def fetch_feature_sample(user_id: str) -> dict:
    try:
        resp = requests.get(
            f"{config.feature_store_api_url}/features/{user_id}",
            timeout=3,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def fetch_rca_results(limit: int = 10) -> pd.DataFrame:
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            df = pd.read_sql(
                "SELECT feature_name, probable_cause, cause_category, confidence, urgency, computed_at "
                "FROM rca_results ORDER BY computed_at DESC LIMIT %(limit)s",
                conn,
                params={"limit": limit},
            )
        return df
    except Exception:
        return pd.DataFrame()


def fetch_throughput_metrics() -> dict:
    """Estimate events/sec from Redis active user count growth (approximation)."""
    client = get_redis()
    count = client.scard("index:active_users")
    return {
        "active_users": count,
        "estimated_events_per_sec": min(count * 0.1, 500),  # rough estimate
        "redis_keys": count,
    }
