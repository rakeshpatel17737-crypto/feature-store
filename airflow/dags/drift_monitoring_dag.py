"""Drift Monitoring DAG: runs hourly to detect feature distribution drift."""
from __future__ import annotations

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
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def load_current_distributions(**context):
    """Sample recent feature values from Redis for drift analysis."""
    import redis as redis_lib
    import random

    client = redis_lib.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )

    user_ids = client.srandmember("index:active_users", 500)
    feature_data: dict[str, list[float]] = {
        "txn_count_5m": [],
        "avg_spend_1h": [],
        "session_activity_rate": [],
        "cart_abandon_ratio": [],
        "product_interaction_freq": [],
        "anomaly_score": [],
    }

    for uid in user_ids:
        data = client.hgetall(f"features:{uid}")
        for feature in feature_data:
            if feature in data:
                try:
                    feature_data[feature].append(float(data[feature]))
                except ValueError:
                    pass

    context["ti"].xcom_push(key="feature_data", value={
        k: v[:200] for k, v in feature_data.items() if v
    })
    print(f"Loaded distributions for {len(user_ids)} users")


def run_drift_detection(**context):
    """Compute PSI + KS + Z-score for each feature."""
    feature_data = context["ti"].xcom_pull(task_ids="load_current_distributions", key="feature_data")
    if not feature_data:
        print("No feature data — skipping drift detection")
        context["ti"].xcom_push(key="has_drift", value=False)
        return

    from drift_detection.drift_detector import detect_drift
    report = detect_drift(feature_data)

    drifting = report.drifting_features
    print(f"Drift detection complete. Drifting features: {drifting}")
    context["ti"].xcom_push(key="has_drift", value=report.has_critical_drift)
    context["ti"].xcom_push(key="drifting_features", value=drifting)

    # Write PSI metrics to PostgreSQL
    for result in report.feature_results:
        _write_metric(result.feature_name, "psi_score", result.psi_score)
        _write_metric(result.feature_name, "ks_statistic", result.ks_statistic)


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


def branch_on_drift(**context):
    has_drift = context["ti"].xcom_pull(task_ids="run_drift_detection", key="has_drift")
    return "run_rca" if has_drift else "drift_ok"


def run_rca_analysis(**context):
    """Trigger LLM RCA for the most severely drifting feature."""
    import asyncio
    drifting = context["ti"].xcom_pull(task_ids="run_drift_detection", key="drifting_features") or []

    if not drifting:
        print("No drifting features to analyze")
        return

    feature_name = drifting[0]
    print(f"Running LLM RCA for feature: {feature_name}")

    from diagnostics.schemas import DriftReport
    from diagnostics.rca_engine import rca_engine

    # Use placeholder metrics (in production: fetch from feature_metrics table)
    report = DriftReport(
        feature_name=feature_name,
        psi_score=0.25,
        ks_statistic=0.15,
        ks_p_value=0.02,
        z_score=3.5,
        severity="alert",
        baseline_period="last_7d",
        current_period="last_1h",
        sample_size=500,
    )

    diagnosis = asyncio.run(rca_engine.analyze(report))
    print(f"RCA diagnosis: {diagnosis.probable_cause} (confidence={diagnosis.confidence:.2f})")
    print(f"Urgency: {diagnosis.urgency} | Category: {diagnosis.cause_category}")
    print(f"Remediation: {diagnosis.remediation_steps}")


with DAG(
    dag_id="feature_store_drift_monitoring",
    default_args=default_args,
    description="Hourly feature drift detection with LLM RCA",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["feature-store", "drift"],
) as dag:
    load = PythonOperator(
        task_id="load_current_distributions",
        python_callable=load_current_distributions,
    )

    detect = PythonOperator(
        task_id="run_drift_detection",
        python_callable=run_drift_detection,
    )

    branch = BranchPythonOperator(
        task_id="branch_on_drift",
        python_callable=branch_on_drift,
    )

    ok = EmptyOperator(task_id="drift_ok")

    rca = PythonOperator(
        task_id="run_rca",
        python_callable=run_rca_analysis,
    )

    load >> detect >> branch >> [ok, rca]
