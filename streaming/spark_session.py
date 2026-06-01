from __future__ import annotations

from pyspark.sql import SparkSession

from .config import config


def create_spark_session(app_name: str = "FeatureStoreStreaming") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(config.spark_master_url)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", config.spark_checkpoint_dir)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )
