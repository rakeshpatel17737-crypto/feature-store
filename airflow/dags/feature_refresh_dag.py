"""Feature Refresh DAG: every 5 minutes, extend TTL for stale cached features."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "ml-platform",
    "retries": 0,
    "email_on_failure": False,
}


def find_and_refresh_stale(**context):
    import redis as redis_lib

    client = redis_lib.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    ttl_threshold = 60  # seconds remaining considered stale
    user_ids = client.srandmember("index:active_users", 1000)
    refreshed = 0

    pipe = client.pipeline()
    for uid in user_ids:
        ttl = client.ttl(f"features:{uid}")
        if 0 < ttl < ttl_threshold:
            pipe.expire(f"features:{uid}", 300)
            refreshed += 1

    if refreshed:
        pipe.execute()

    print(f"Refreshed TTL for {refreshed} stale users out of {len(user_ids)} sampled")
    context["ti"].xcom_push(key="refreshed_count", value=refreshed)


with DAG(
    dag_id="feature_store_refresh",
    default_args=default_args,
    description="Extend TTL for stale cached features every 5 minutes",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["feature-store", "refresh"],
) as dag:
    refresh = PythonOperator(
        task_id="find_and_refresh_stale",
        python_callable=find_and_refresh_stale,
    )
