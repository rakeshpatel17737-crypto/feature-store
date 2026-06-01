"""Load and store feature distribution baselines in Redis."""
from __future__ import annotations

import json
import logging
from typing import Optional

from online_store.redis_client import sync_redis
from online_store.key_schema import baseline_key

logger = logging.getLogger(__name__)

BASELINE_TTL_SECONDS = 86400 * 7  # 1 week


def save_baseline(feature_name: str, values: list[float]) -> None:
    client = sync_redis.get()
    client.set(baseline_key(feature_name), json.dumps(values), ex=BASELINE_TTL_SECONDS)
    logger.info("Saved baseline for %s (%d values)", feature_name, len(values))


def load_baseline(feature_name: str) -> Optional[list[float]]:
    client = sync_redis.get()
    raw = client.get(baseline_key(feature_name))
    if not raw:
        logger.warning("No baseline found for %s", feature_name)
        return None
    return json.loads(raw)


def baseline_exists(feature_name: str) -> bool:
    client = sync_redis.get()
    return client.exists(baseline_key(feature_name)) > 0


def refresh_all_baselines(feature_data: dict[str, list[float]]) -> None:
    """Called by Airflow weekly DAG to refresh all baselines."""
    for feature_name, values in feature_data.items():
        if values:
            save_baseline(feature_name, values)
    logger.info("Refreshed %d feature baselines", len(feature_data))
