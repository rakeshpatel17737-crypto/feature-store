"""foreachBatch sink writing feature vectors to Delta Lake."""
from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def write_features_to_delta(
    batch_df: DataFrame,
    batch_id: int,
    delta_path: str,
    spark: SparkSession,
) -> None:
    """Called by Spark foreachBatch. Merges feature rows into Delta Lake."""
    if batch_df.count() == 0:
        return

    enriched = (
        batch_df
        .withColumn("event_date", F.to_date(F.from_unixtime(F.col("computed_at"))))
        .withColumn("user_prefix", F.substring(F.col("user_id"), 1, 3))
    )

    table_path = f"{delta_path}/user_features"

    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, table_path):
            delta_table = DeltaTable.forPath(spark, table_path)
            (
                delta_table.alias("target")
                .merge(
                    enriched.alias("source"),
                    "target.user_id = source.user_id AND target.event_date = source.event_date",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                enriched
                .write
                .format("delta")
                .mode("overwrite")
                .partitionBy("event_date", "user_prefix")
                .save(table_path)
            )
    except Exception as exc:
        logger.error("Delta sink error batch %d: %s", batch_id, exc)
        # Fallback: append parquet if Delta fails
        (
            enriched
            .write
            .mode("append")
            .partitionBy("event_date")
            .parquet(f"{delta_path}/fallback_parquet")
        )

    logger.info("Delta sink: batch %d wrote %d rows", batch_id, enriched.count())
