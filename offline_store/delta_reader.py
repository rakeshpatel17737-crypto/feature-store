"""Point-in-time correct reads from Delta Lake (time-travel)."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from .config import config

logger = logging.getLogger(__name__)


def read_features_at(
    spark: SparkSession,
    as_of: Optional[datetime] = None,
    version: Optional[int] = None,
) -> DataFrame:
    """
    Point-in-time correct read. Used by ML training pipelines to guarantee
    no data leakage and consistent feature values.

    as_of: read state of the table at this timestamp
    version: read at a specific Delta version (takes precedence over as_of)
    """
    table_path = f"{config.delta_table_path}/user_features"
    reader = spark.read.format("delta")

    if version is not None:
        reader = reader.option("versionAsOf", version)
        logger.info("Reading Delta table at version %d", version)
    elif as_of is not None:
        reader = reader.option("timestampAsOf", as_of.isoformat())
        logger.info("Reading Delta table as of %s", as_of.isoformat())
    else:
        logger.info("Reading Delta table (latest)")

    return reader.load(table_path)


def read_features_for_training(
    spark: SparkSession,
    as_of: datetime,
    user_ids: Optional[list[str]] = None,
) -> DataFrame:
    """
    Training-safe feature read. Always uses point-in-time to prevent skew.
    Optionally filters to a specific user cohort.
    """
    df = read_features_at(spark, as_of=as_of)

    if user_ids:
        df = df.filter(df.user_id.isin(user_ids))

    return df.select(
        "user_id",
        "txn_count_5m",
        "avg_spend_1h",
        "session_activity_rate",
        "cart_abandon_ratio",
        "product_interaction_freq",
        "anomaly_score",
        "computed_at",
        "event_date",
    )


def get_table_history(spark: SparkSession, limit: int = 10) -> DataFrame:
    table_path = f"{config.delta_table_path}/user_features"
    return spark.sql(f"DESCRIBE HISTORY delta.`{table_path}` LIMIT {limit}")
