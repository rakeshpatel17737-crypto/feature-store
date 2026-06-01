"""Proactive TTL refresh: re-triggers feature computation before keys expire."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from .config import config
from .redis_client import sync_redis

logger = logging.getLogger(__name__)

REFRESH_THRESHOLD = int(config.feature_ttl_seconds * (1 - config.refresh_threshold_ratio))


def find_stale_users(sample_size: int = 500) -> list[str]:
    """Return user_ids whose feature TTL is below the refresh threshold."""
    users = sync_redis.sample_active_users(sample_size)
    stale: list[str] = []
    for user_id in users:
        ttl = sync_redis.ttl(user_id)
        if 0 < ttl < REFRESH_THRESHOLD:
            stale.append(user_id)
    return stale


def refresh_features_for_users(user_ids: list[str], feature_fn=None) -> int:
    """
    Recompute and write features for the given users.

    feature_fn: callable(user_id) -> dict[str, float]
    If None, writes a placeholder that extends TTL without recomputing.
    """
    refreshed = 0
    for user_id in user_ids:
        try:
            if feature_fn:
                features = feature_fn(user_id)
                sync_redis.write_features(user_id, features)
            else:
                # Just extend TTL to prevent eviction until Spark catches up
                client = sync_redis.get()
                from .key_schema import feature_key
                client.expire(feature_key(user_id), config.feature_ttl_seconds)
            refreshed += 1
        except Exception as exc:
            logger.warning("Failed to refresh features for %s: %s", user_id, exc)
    return refreshed
