"""Custom Airflow hooks for feature store operations."""
from __future__ import annotations

import os

from airflow.hooks.base import BaseHook


class FeatureStoreRedisHook(BaseHook):
    """Hook to interact with the feature store's Redis instance."""

    conn_name_attr = "redis_conn_id"
    default_conn_name = "feature_store_redis"

    def get_conn(self):
        import redis
        return redis.Redis(
            host=os.environ.get("REDIS_HOST", "redis"),
            port=int(os.environ.get("REDIS_PORT", 6379)),
            decode_responses=True,
        )

    def get_feature(self, user_id: str) -> dict:
        client = self.get_conn()
        return client.hgetall(f"features:{user_id}") or {}


class FeatureStoreAPIHook(BaseHook):
    """Hook to interact with the Feature Store REST API."""

    conn_name_attr = "feature_store_api_conn_id"
    default_conn_name = "feature_store_api"

    def __init__(self, api_url: str | None = None):
        self.api_url = api_url or os.environ.get("FEATURE_STORE_API_URL", "http://api:8000")

    def get_features(self, user_id: str) -> dict:
        import requests
        response = requests.get(f"{self.api_url}/features/{user_id}", timeout=5)
        response.raise_for_status()
        return response.json()

    def trigger_refresh(self, user_id: str) -> dict:
        import requests
        response = requests.post(
            f"{self.api_url}/refresh-feature",
            json={"user_id": user_id},
            timeout=5,
        )
        response.raise_for_status()
        return response.json()
