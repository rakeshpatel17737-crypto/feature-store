"""Partitioning utilities for Delta Lake feature tables."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Add derived partition columns: event_date and user_prefix."""
    return (
        df
        .withColumn("event_date", F.to_date(F.from_unixtime(F.col("computed_at"))))
        .withColumn("user_prefix", F.substring(F.col("user_id"), 1, 3))
    )


def partition_predicate(date_str: str, user_prefix: str | None = None) -> str:
    """Build a partition filter predicate string for Delta Lake reads."""
    predicate = f"event_date = '{date_str}'"
    if user_prefix:
        predicate += f" AND user_prefix = '{user_prefix}'"
    return predicate
