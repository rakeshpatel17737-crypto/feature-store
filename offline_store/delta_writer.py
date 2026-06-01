from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import config

logger = logging.getLogger(__name__)


def write_features(spark: SparkSession, df: DataFrame, mode: str = "append") -> None:
    table_path = f"{config.delta_table_path}/user_features"

    enriched = (
        df
        .withColumn("event_date", F.to_date(F.from_unixtime(F.col("computed_at"))))
        .withColumn("user_prefix", F.substring(F.col("user_id"), 1, 3))
    )

    (
        enriched
        .write
        .format("delta")
        .mode(mode)
        .partitionBy("event_date", "user_prefix")
        .option("delta.logRetentionDuration", "interval 30 days")
        .option("delta.dataSkippingNumIndexedCols", "4")
        .save(table_path)
    )
    logger.info("Wrote %d rows to Delta Lake at %s", df.count(), table_path)


def optimize_table(spark: SparkSession) -> None:
    """Run OPTIMIZE + ZORDER for efficient ML training reads."""
    table_path = f"{config.delta_table_path}/user_features"
    spark.sql(f"""
        OPTIMIZE delta.`{table_path}`
        ZORDER BY (user_id, computed_at)
    """)
    logger.info("OPTIMIZE + ZORDER completed")
