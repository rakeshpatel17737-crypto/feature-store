"""Z-score anomaly scoring for transaction amounts."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def compute_anomaly_score(df: DataFrame) -> DataFrame:
    """
    Z-score of mean purchase amount in a 5-min window vs 24-hour rolling stats.
    Z = (x - μ) / (σ + ε)

    Uses a 24h sliding window for the reference distribution and a 5-min
    window for the current value. High absolute Z-score indicates anomaly.
    """
    purchases = df.filter(F.col("event_type") == "purchase")

    # 24-hour reference stats
    baseline = (
        purchases
        .groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "24 hours", "5 minutes"),
        )
        .agg(
            F.avg("transaction_amount").cast(DoubleType()).alias("mean_24h"),
            F.stddev("transaction_amount").cast(DoubleType()).alias("std_24h"),
        )
    )

    # 5-minute current average
    current = (
        purchases
        .groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "5 minutes"),
        )
        .agg(F.avg("transaction_amount").cast(DoubleType()).alias("avg_5m"))
    )

    # Join and compute Z-score (using user_id; windows will overlap for the same user)
    scored = (
        current
        .join(baseline.drop("window"), on="user_id", how="left")
        .withColumn(
            "anomaly_score",
            F.when(
                F.col("mean_24h").isNotNull(),
                F.abs(
                    (F.col("avg_5m") - F.col("mean_24h"))
                    / (F.coalesce(F.col("std_24h"), F.lit(1.0)) + F.lit(1e-6))
                ).cast(DoubleType()),
            ).otherwise(F.lit(0.0)),
        )
        .select("user_id", "window", "anomaly_score")
    )
    return scored
