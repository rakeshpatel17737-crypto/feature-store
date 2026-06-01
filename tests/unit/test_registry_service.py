"""Unit tests for the feature registry service."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def make_mock_feature(name: str = "txn_count_5m"):
    feature = MagicMock()
    feature.feature_name = name
    feature.display_name = "Transaction Count 5m"
    feature.description = "Count of purchases"
    feature.transform_logic = "COUNT(purchase)"
    feature.window_type = "tumbling_5m"
    feature.freshness_sla_seconds = 60
    feature.owner = "ml-platform"
    feature.version = "1.0"
    feature.is_active = True
    feature.lineage = []
    return feature


@pytest.mark.asyncio
async def test_get_all_features_returns_list():
    from registry.registry_service import get_all_features
    mock_db = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [
        make_mock_feature("txn_count_5m"),
        make_mock_feature("avg_spend_1h"),
    ]
    mock_db.execute = AsyncMock(return_value=mock_result)

    features = await get_all_features(mock_db)
    assert len(features) == 2


@pytest.mark.asyncio
async def test_get_feature_not_found():
    from registry.registry_service import get_feature
    mock_db = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=mock_result)

    result = await get_feature(mock_db, "nonexistent_feature")
    assert result is None


@pytest.mark.asyncio
async def test_record_metric_commits():
    from registry.registry_service import record_metric
    from registry.schemas import FeatureMetricCreate

    mock_db = AsyncMock()
    metric = FeatureMetricCreate(
        feature_name="txn_count_5m",
        metric_type="psi_score",
        metric_value=0.05,
    )
    await record_metric(mock_db, metric)
    mock_db.add.assert_called_once()
    mock_db.commit.assert_called_once()
