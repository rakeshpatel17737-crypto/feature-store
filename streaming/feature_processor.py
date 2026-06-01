"""
Main PySpark Structured Streaming job.

Reads from Kafka → computes features → writes to Redis (online) + Delta Lake (offline).
"""
from __future__ import annotations

import logging
import sys

sys.path.insert(0, "/opt")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from streaming.spark_session import create_spark_session
from streaming.config import config
from streaming.feature_definitions import compute_all_features
from streaming.anomaly_scorer import compute_anomaly_score
from streaming.redis_sink import write_features_to_redis
from streaming.delta_sink import write_features_to_delta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
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
    StructField("location", StructType([
        StructField("country", StringType()),
        StructField("city", StringType()),
    ])),
    StructField("device_type", StringType()),
    StructField("schema_version", StringType()),
])


def run() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka: %s / %s", config.kafka_bootstrap_servers, config.kafka_topic_events)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic_events)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", config.kafka_consumer_group)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )

    events_df = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast(StringType()), EVENT_SCHEMA).alias("data")
        )
        .select("data.*")
        .filter(F.col("user_id").isNotNull())
        .withWatermark("timestamp", config.watermark_duration)
    )

    # Compute all windowed features
    features_df = compute_all_features(events_df)

    # Compute anomaly scores separately (different windows)
    anomaly_df = compute_anomaly_score(events_df)

    # Join anomaly scores into feature vector
    full_features_df = (
        features_df
        .join(
            anomaly_df.select("user_id", "anomaly_score"),
            on="user_id",
            how="left",
        )
        .fillna({"anomaly_score": 0.0})
    )

    redis_host = config.redis_host
    redis_port = config.redis_port
    ttl = config.feature_ttl_seconds
    delta_path = config.delta_table_path
    checkpoint_base = config.spark_checkpoint_dir

    def redis_batch_writer(batch_df, batch_id):
        write_features_to_redis(batch_df, batch_id, redis_host, redis_port, ttl)

    def delta_batch_writer(batch_df, batch_id):
        write_features_to_delta(batch_df, batch_id, delta_path, spark)

    # Start Redis stream
    redis_query = (
        full_features_df.writeStream
        .foreachBatch(redis_batch_writer)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/redis")
        .trigger(processingTime=config.processing_time)
        .queryName("redis_sink")
        .start()
    )

    # Start Delta stream
    delta_query = (
        full_features_df.writeStream
        .foreachBatch(delta_batch_writer)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/delta")
        .trigger(processingTime=config.processing_time)
        .queryName("delta_sink")
        .start()
    )

    logger.info("Streaming queries started: redis_sink, delta_sink")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
