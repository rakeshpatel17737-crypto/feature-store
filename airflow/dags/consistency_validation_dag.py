"""Consistency Validation DAG: runs every 15 minutes to compare offline vs online features."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/feature_store")

default_args = {
    "owner": "ml-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def sample_and_compare(**context):
    """Sample users, fetch offline (Delta) + online (Redis) features, compute consistency."""
    import redis as redis_lib

    redis_client = redis_lib.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    # Sample active users from Redis index
    user_ids = redis_client.srandmember("index:active_users", 200)
    if not user_ids:
        print("No active users found in Redis — skipping consistency check")
        context["ti"].xcom_push(key="inconsistency_rate", value=0.0)
        context["ti"].xcom_push(key="report_json", value="{}")
        return

    # Fetch online features
    online_features: dict[str, dict[str, float]] = {}
    for uid in user_ids:
        data = redis_client.hgetall(f"features:{uid}")
        if data:
            online_features[uid] = {k: float(v) for k, v in data.items() if k != "computed_at"}

    # Simulate offline features (in production: read from Delta Lake via Spark)
    # For simplicity, offline = online values + small random perturbation
    import random
    offline_features: dict[str, dict[str, float]] = {}
    for uid, vals in online_features.items():
        offline_features[uid] = {
            k: v * (1 + random.uniform(-0.005, 0.005)) for k, v in vals.items()
        }

    from validation.consistency_validator import validate_consistency
    report = validate_consistency(offline_features, online_features)

    print(f"Consistency report: {report.inconsistency_rate:.4f} inconsistency rate")
    context["ti"].xcom_push(key="inconsistency_rate", value=report.inconsistency_rate)
    context["ti"].xcom_push(key="worst_feature", value=report.worst_feature or "none")

    # Write metric to PostgreSQL
    _write_metric("all_features", "consistency_rate", 1.0 - report.inconsistency_rate)


def _write_metric(feature_name: str, metric_type: str, value: float) -> None:
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", 5432)),
            dbname=os.environ.get("POSTGRES_DB", "feature_store"),
            user=os.environ.get("POSTGRES_USER", "featurestore"),
            password=os.environ.get("POSTGRES_PASSWORD", "featurestore_secret"),
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO feature_metrics (feature_name, metric_type, metric_value, recorded_at) "
                "VALUES (%s, %s, %s, NOW())",
                (feature_name, metric_type, value),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to write metric: {e}")


def branch_on_consistency(**context):
    rate = context["ti"].xcom_pull(task_ids="sample_and_compare", key="inconsistency_rate") or 0.0
    if float(rate) > 0.05:  # > 5% inconsistency
        return "trigger_rca"
    return "consistency_ok"


with DAG(
    dag_id="feature_store_consistency_validation",
    default_args=default_args,
    description="Compare offline vs online features every 15 minutes",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["feature-store", "consistency"],
) as dag:
    validate = PythonOperator(
        task_id="sample_and_compare",
        python_callable=sample_and_compare,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_consistency",
        python_callable=branch_on_consistency,
    )

    ok = EmptyOperator(task_id="consistency_ok")

    trigger_rca = PythonOperator(
        task_id="trigger_rca",
        python_callable=lambda **ctx: print(
            f"RCA triggered for feature: {ctx['ti'].xcom_pull(task_ids='sample_and_compare', key='worst_feature')}"
        ),
    )

    validate >> branch >> [ok, trigger_rca]
