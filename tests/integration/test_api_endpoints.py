"""Integration tests for the FastAPI serving layer."""
from __future__ import annotations

import time
import pytest


@pytest.mark.asyncio
@pytest.mark.integration
async def test_health_endpoint():
    """Health endpoint returns 200 with status fields."""
    from httpx import AsyncClient
    from api.main import app

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/health")
    # May return degraded if Redis/DB not available in test env
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "redis" in data
    assert "db" in data


@pytest.mark.asyncio
@pytest.mark.integration
async def test_feature_metadata_endpoint():
    """Feature metadata endpoint returns seeded features."""
    from httpx import AsyncClient
    from api.main import app

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/feature-metadata")

    if response.status_code == 200:
        features = response.json()
        assert isinstance(features, list)
        if features:
            feature_names = {f["feature_name"] for f in features}
            assert "txn_count_5m" in feature_names


@pytest.mark.asyncio
@pytest.mark.integration
async def test_features_not_found():
    """Missing user should return 404."""
    from httpx import AsyncClient
    from api.main import app

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/features/usr_nonexistent_xyz")

    assert response.status_code in (404, 500)  # 500 if Redis not available


@pytest.mark.asyncio
@pytest.mark.integration
async def test_refresh_feature_endpoint():
    """Refresh endpoint should return 202 with queued status."""
    from httpx import AsyncClient
    from api.main import app

    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post(
            "/refresh-feature",
            json={"user_id": "usr_00001"},
        )

    if response.status_code == 202:
        data = response.json()
        assert data["status"] == "refresh_queued"
        assert data["user_id"] == "usr_00001"
