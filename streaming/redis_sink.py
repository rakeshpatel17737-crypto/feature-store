"""foreachBatch sink writing feature vectors to Redis."""
from __future__ import annotations

import logging
import sys
import os
import time

sys.path.insert(0, "/opt")

import redis as redis_lib

logger = logging.getLogger(__name__)


def get_redis_client(host: str, port: int) -> redis_lib.Redis:
    pool = redis_lib.ConnectionPool(
        host=host,
        port=port,
        db=0,
        max_connections=10,
        socket_connect_timeout=5,
        decode_responses=True,
    )
    return redis_lib.Redis(connection_pool=pool)


def write_features_to_redis(batch_df, batch_id: int, redis_host: str, redis_port: int, ttl: int) -> None:
    """Called by Spark foreachBatch. Writes all rows in one Redis pipeline."""
    rows = batch_df.collect()
    if not rows:
        return

    client = get_redis_client(redis_host, redis_port)
    pipe = client.pipeline()

    for row in rows:
        user_id = row["user_id"]
        key = f"features:{user_id}"
        payload = {
            "txn_count_5m": str(row.get("txn_count_5m", 0.0) or 0.0),
            "avg_spend_1h": str(row.get("avg_spend_1h", 0.0) or 0.0),
            "session_activity_rate": str(row.get("session_activity_rate", 0.0) or 0.0),
            "cart_abandon_ratio": str(row.get("cart_abandon_ratio", 0.0) or 0.0),
            "product_interaction_freq": str(row.get("product_interaction_freq", 0.0) or 0.0),
            "anomaly_score": str(row.get("anomaly_score", 0.0) or 0.0),
            "computed_at": str(row.get("computed_at") or time.time()),
        }
        pipe.hset(key, mapping=payload)
        pipe.expire(key, ttl)
        pipe.sadd("index:active_users", user_id)

    pipe.execute()
    logger.info("Redis sink: batch %d wrote %d users", batch_id, len(rows))
