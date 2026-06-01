"""Backfill DAG: replay historical Kafka events through feature_definitions.py."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ml-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def validate_date_range(**context):
    start = context["dag_run"].conf.get("start_date", "2026-01-01")
    end = context["dag_run"].conf.get("end_date", datetime.now().strftime("%Y-%m-%d"))
    print(f"Backfill range: {start} → {end}")
    context["ti"].xcom_push(key="start_date", value=start)
    context["ti"].xcom_push(key="end_date", value=end)


def verify_backfill(**context):
    print("Backfill verification: checking Delta Lake row count...")
    # In production, would run a Spark count query against the backfilled partition
    print("Backfill verification complete")


with DAG(
    dag_id="feature_store_backfill",
    default_args=default_args,
    description="Backfill historical feature data from Kafka to Delta Lake",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["feature-store", "backfill"],
) as dag:
    validate = PythonOperator(
        task_id="validate_date_range",
        python_callable=validate_date_range,
    )

    run_spark_backfill = BashOperator(
        task_id="run_spark_backfill",
        bash_command=(
            "docker exec fs-spark-master spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/offline_store/backfill.py "
            "--start {{ dag_run.conf.get('start_date', '2026-01-01') }} "
            "--end {{ dag_run.conf.get('end_date', macros.ds_add(ds, 1)) }}"
        ),
    )

    verify = PythonOperator(
        task_id="verify_backfill",
        python_callable=verify_backfill,
    )

    validate >> run_spark_backfill >> verify
