"""
AUTHORITATIVE feature computation logic.

Both the streaming job and the offline backfill import from here to guarantee
identical transforms and prevent training-serving skew.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def compute_txn_count_5m(df: DataFrame) -> DataFrame:
    """COUNT of purchase events in 5-minute tumbling windows."""
    return (
        df.filter(F.col("event_type") == "purchase")
        .groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "5 minutes"),
        )
        .agg(F.count("*").cast(DoubleType()).alias("txn_count_5m"))
        .select("user_id", "window", "txn_count_5m")
    )


def compute_avg_spend_1h(df: DataFrame) -> DataFrame:
    """AVG transaction amount in 1-hour sliding window (slide=5min)."""
    return (
        df.filter(F.col("event_type") == "purchase")
        .groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "1 hour", "5 minutes"),
        )
        .agg(F.avg("transaction_amount").cast(DoubleType()).alias("avg_spend_1h"))
        .select("user_id", "window", "avg_spend_1h")
    )


def compute_session_activity_rate(df: DataFrame) -> DataFrame:
    """Events per distinct session in 5-minute tumbling window."""
    return (
        df.groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "5 minutes"),
        )
        .agg(
            F.count("*").cast(DoubleType()).alias("event_count"),
            F.countDistinct("session_id").cast(DoubleType()).alias("session_count"),
        )
        .withColumn(
            "session_activity_rate",
            F.when(F.col("session_count") > 0, F.col("event_count") / F.col("session_count"))
            .otherwise(F.lit(0.0)),
        )
        .select("user_id", "window", "session_activity_rate")
    )


def compute_cart_abandon_ratio(df: DataFrame) -> DataFrame:
    """Fraction of add-to-cart events without a subsequent purchase in 30-min window."""
    cart_events = (
        df.filter(F.col("event_type").isin("add_to_cart", "purchase"))
        .groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "30 minutes"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))
            .cast(DoubleType())
            .alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))
            .cast(DoubleType())
            .alias("purchase_count"),
        )
        .withColumn(
            "cart_abandon_ratio",
            F.when(
                F.col("add_to_cart_count") > 0,
                F.greatest(
                    (F.col("add_to_cart_count") - F.col("purchase_count")) / F.col("add_to_cart_count"),
                    F.lit(0.0),
                ),
            ).otherwise(F.lit(0.0)),
        )
    )
    return cart_events.select("user_id", "window", "cart_abandon_ratio")


def compute_product_interaction_freq(df: DataFrame) -> DataFrame:
    """COUNT of page_view events in 1-hour tumbling window."""
    return (
        df.filter(F.col("event_type") == "page_view")
        .groupBy(
            F.col("user_id"),
            F.window(F.col("timestamp"), "1 hour"),
        )
        .agg(F.count("*").cast(DoubleType()).alias("product_interaction_freq"))
        .select("user_id", "window", "product_interaction_freq")
    )


def compute_all_features(df: DataFrame) -> DataFrame:
    """
    Compute all features and join into a single row per user per window.
    Uses the smallest window (5 min) as the base and left-joins others.
    """
    base = compute_txn_count_5m(df).alias("base")
    avg_spend = compute_avg_spend_1h(df).alias("avg_spend")
    session_rate = compute_session_activity_rate(df).alias("session_rate")
    cart_abandon = compute_cart_abandon_ratio(df).alias("cart_abandon")
    product_freq = compute_product_interaction_freq(df).alias("product_freq")

    # Join on user_id; windows may differ, so coalesce
    result = (
        base
        .join(
            avg_spend.drop("window"),
            on="user_id",
            how="left",
        )
        .join(
            session_rate.drop("window"),
            on="user_id",
            how="left",
        )
        .join(
            cart_abandon.drop("window"),
            on="user_id",
            how="left",
        )
        .join(
            product_freq.drop("window"),
            on="user_id",
            how="left",
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("computed_at", F.unix_timestamp(F.current_timestamp()).cast(DoubleType()))
        .fillna(0.0)
        .select(
            "user_id",
            "window_start",
            "window_end",
            "computed_at",
            "txn_count_5m",
            "avg_spend_1h",
            "session_activity_rate",
            "cart_abandon_ratio",
            "product_interaction_freq",
        )
    )
    return result
