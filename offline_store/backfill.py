"""
Historical backfill job.

Reads historical events from Kafka using startingOffsets (JSON offset map)
and processes them through the SAME feature_definitions.py as the streaming job.
This guarantees offline and online features are computed identically.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime

sys.path.insert(0, "/opt")

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from offline_store.config import config
from streaming.feature_definitions import compute_all_features
from streaming.anomaly_scorer import compute_anomaly_score
from streaming.spark_session import create_spark_session
from offline_store.delta_writer import write_features

logger = logging.getLogger(__name__)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("transaction_amount", DoubleType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("device_type", StringType()),
    StructField("schema_version", StringType()),
])


def run_backfill(
    start_date: datetime,
    end_date: datetime,
    starting_offsets: str = "earliest",
) -> None:
    spark = create_spark_session(app_name="FeatureStoreBackfill")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Backfill: %s → %s", start_date.date(), end_date.date())

    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", "ecommerce.events.raw")
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", "latest")
        .load()
    )

    events_df = (
        raw_df
        .select(F.from_json(F.col("value").cast(StringType()), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(
            F.col("user_id").isNotNull()
            & F.col("timestamp").between(start_date, end_date)
        )
    )

    features_df = compute_all_features(events_df)
    anomaly_df = compute_anomaly_score(events_df)

    full_df = (
        features_df
        .join(anomaly_df.select("user_id", "anomaly_score"), on="user_id", how="left")
        .fillna({"anomaly_score": 0.0})
        .withColumn("computed_at", F.unix_timestamp(F.col("window_start")).cast(DoubleType()))
    )

    write_features(spark, full_df, mode="append")
    logger.info("Backfill complete for %s → %s", start_date.date(), end_date.date())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    run_backfill(
        start_date=datetime.fromisoformat(args.start),
        end_date=datetime.fromisoformat(args.end),
    )
