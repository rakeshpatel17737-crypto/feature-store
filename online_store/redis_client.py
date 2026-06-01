"""Sync and async Redis client wrappers for the online feature store."""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

import redis
from redis import ConnectionPool
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import config
from .key_schema import feature_key, feature_meta_key, FEATURE_FIELDS

logger = logging.getLogger(__name__)


class SyncRedisClient:
    """Synchronous client used by PySpark foreachBatch and Airflow tasks."""

    _pool: Optional[ConnectionPool] = None

    @classmethod
    def _get_pool(cls) -> ConnectionPool:
        if cls._pool is None:
            cls._pool = ConnectionPool(
                host=config.redis_host,
                port=config.redis_port,
                db=config.redis_db,
                max_connections=config.redis_max_connections,
                socket_connect_timeout=5,
                socket_timeout=2,
                decode_responses=True,
            )
        return cls._pool

    @classmethod
    def get(cls) -> redis.Redis:
        return redis.Redis(connection_pool=cls._get_pool())

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=0.5, max=5))
    def write_features(
        self, user_id: str, features: dict[str, float], computed_at: float | None = None
    ) -> None:
        client = self.get()
        key = feature_key(user_id)
        ts = computed_at or time.time()

        payload: dict[str, str] = {k: str(v) for k, v in features.items()}
        payload["computed_at"] = str(ts)

        pipe = client.pipeline()
        pipe.hset(key, mapping=payload)
        pipe.expire(key, config.feature_ttl_seconds)
        # Track active users for sampling during consistency validation
        pipe.sadd("index:active_users", user_id)
        pipe.execute()

    def read_features(self, user_id: str) -> dict[str, str] | None:
        client = self.get()
        data = client.hgetall(feature_key(user_id))
        return data if data else None

    def batch_write_features(self, records: list[dict[str, Any]]) -> None:
        """Write multiple users' features in a single pipeline round-trip."""
        client = self.get()
        pipe = client.pipeline()
        for rec in records:
            user_id = rec["user_id"]
            key = feature_key(user_id)
            payload: dict[str, str] = {
                k: str(rec[k]) for k in FEATURE_FIELDS[:-1] if k in rec
            }
            payload["computed_at"] = str(rec.get("computed_at", time.time()))
            pipe.hset(key, mapping=payload)
            pipe.expire(key, config.feature_ttl_seconds)
            pipe.sadd("index:active_users", user_id)
        pipe.execute()

    def ttl(self, user_id: str) -> int:
        return self.get().ttl(feature_key(user_id))

    def sample_active_users(self, n: int) -> list[str]:
        client = self.get()
        return list(client.srandmember("index:active_users", n))

    def store_baseline(self, feature_name: str, distribution: list[float]) -> None:
        from .key_schema import baseline_key
        client = self.get()
        client.set(baseline_key(feature_name), json.dumps(distribution), ex=86400 * 7)

    def load_baseline(self, feature_name: str) -> list[float] | None:
        from .key_schema import baseline_key
        client = self.get()
        raw = client.get(baseline_key(feature_name))
        return json.loads(raw) if raw else None


# Module-level singleton
sync_redis = SyncRedisClient()
