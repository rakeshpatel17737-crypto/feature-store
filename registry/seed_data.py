"""Seed the feature registry with the 6 authoritative feature definitions."""
from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from .models import FeatureDefinition, FeatureLineage

logger = logging.getLogger(__name__)

FEATURE_SEEDS = [
    {
        "feature_name": "txn_count_5m",
        "display_name": "Transaction Count (5 min)",
        "description": "Number of purchase transactions in the last 5-minute tumbling window.",
        "transform_logic": "COUNT(event_type='purchase') OVER TUMBLING WINDOW 5 MINUTES",
        "window_type": "tumbling_5m",
        "freshness_sla_seconds": 60,
        "owner": "ml-platform",
        "version": "1.0",
        "lineage": [
            {"upstream_source": "kafka:ecommerce.events.raw", "transform_step": "filter event_type=purchase"},
            {"upstream_source": "pyspark:structured_streaming", "transform_step": "count_window_5m"},
        ],
    },
    {
        "feature_name": "avg_spend_1h",
        "display_name": "Average Spend (1 hour)",
        "description": "Average purchase transaction amount in a sliding 1-hour window.",
        "transform_logic": "AVG(transaction_amount WHERE event_type='purchase') OVER SLIDING WINDOW 1H/5MIN",
        "window_type": "sliding_1h",
        "freshness_sla_seconds": 300,
        "owner": "ml-platform",
        "version": "1.0",
        "lineage": [
            {"upstream_source": "kafka:ecommerce.events.raw", "transform_step": "filter event_type=purchase"},
            {"upstream_source": "pyspark:structured_streaming", "transform_step": "avg_window_1h"},
        ],
    },
    {
        "feature_name": "session_activity_rate",
        "display_name": "Session Activity Rate (5 min)",
        "description": "Ratio of total events to distinct sessions in the last 5 minutes.",
        "transform_logic": "COUNT(events) / COUNT(DISTINCT session_id) OVER TUMBLING WINDOW 5 MINUTES",
        "window_type": "tumbling_5m",
        "freshness_sla_seconds": 60,
        "owner": "ml-platform",
        "version": "1.0",
        "lineage": [
            {"upstream_source": "kafka:ecommerce.events.raw", "transform_step": "all_events"},
            {"upstream_source": "pyspark:structured_streaming", "transform_step": "session_rate_5m"},
        ],
    },
    {
        "feature_name": "cart_abandon_ratio",
        "display_name": "Cart Abandonment Ratio (30 min)",
        "description": "Fraction of add-to-cart events not followed by a purchase in 30 minutes.",
        "transform_logic": "add_to_cart_count_without_purchase / total_add_to_cart OVER TUMBLING 30 MINUTES",
        "window_type": "tumbling_30m",
        "freshness_sla_seconds": 300,
        "owner": "ml-platform",
        "version": "1.0",
        "lineage": [
            {"upstream_source": "kafka:ecommerce.events.raw", "transform_step": "filter add_to_cart + purchase"},
            {"upstream_source": "pyspark:structured_streaming", "transform_step": "cart_abandon_30m"},
        ],
    },
    {
        "feature_name": "product_interaction_freq",
        "display_name": "Product Interaction Frequency (1 hour)",
        "description": "Number of product page views in the last 1-hour tumbling window.",
        "transform_logic": "COUNT(event_type='page_view') OVER TUMBLING WINDOW 1 HOUR",
        "window_type": "tumbling_1h",
        "freshness_sla_seconds": 300,
        "owner": "ml-platform",
        "version": "1.0",
        "lineage": [
            {"upstream_source": "kafka:ecommerce.events.raw", "transform_step": "filter page_view"},
            {"upstream_source": "pyspark:structured_streaming", "transform_step": "pageview_count_1h"},
        ],
    },
    {
        "feature_name": "anomaly_score",
        "display_name": "Transaction Anomaly Score (5 min)",
        "description": "Z-score of current transaction amount vs 24-hour rolling mean/stddev. High values indicate potential fraud.",
        "transform_logic": "(avg_txn_5m - mean_24h) / stddev_24h",
        "window_type": "tumbling_5m",
        "freshness_sla_seconds": 60,
        "owner": "fraud-detection",
        "version": "1.0",
        "lineage": [
            {"upstream_source": "kafka:ecommerce.events.raw", "transform_step": "filter purchase"},
            {"upstream_source": "pyspark:structured_streaming", "transform_step": "zscore_24h"},
        ],
    },
]


async def seed_features(db: AsyncSession) -> None:
    for seed in FEATURE_SEEDS:
        existing = await db.execute(
            select(FeatureDefinition).where(
                FeatureDefinition.feature_name == seed["feature_name"]
            )
        )
        if existing.scalar_one_or_none():
            continue

        lineage_data = seed.pop("lineage", [])
        feature = FeatureDefinition(**seed)
        db.add(feature)
        await db.flush()

        for step in lineage_data:
            db.add(FeatureLineage(feature_name=feature.feature_name, **step))

        seed["lineage"] = lineage_data  # restore

    await db.commit()
    logger.info("Feature registry seeded with %d features", len(FEATURE_SEEDS))
