"""Shared pytest fixtures for unit and integration tests."""
from __future__ import annotations

import os
import pytest


# ── Unit test environment overrides ────────────────────────────────────────────
@pytest.fixture(autouse=True, scope="session")
def set_test_env():
    os.environ.setdefault("REDIS_HOST", "localhost")
    os.environ.setdefault("REDIS_PORT", "6379")
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5432")
    os.environ.setdefault("POSTGRES_DB", "feature_store_test")
    os.environ.setdefault("POSTGRES_USER", "featurestore")
    os.environ.setdefault("POSTGRES_PASSWORD", "featurestore_secret")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test-key")
    os.environ.setdefault("ANTHROPIC_MODEL", "claude-sonnet-4-5")


# ── Integration test fixtures (testcontainers) ──────────────────────────────
@pytest.fixture(scope="session")
def redis_container():
    try:
        from testcontainers.redis import RedisContainer
        with RedisContainer("redis:7.2-alpine") as container:
            yield container
    except ImportError:
        pytest.skip("testcontainers not installed")


@pytest.fixture(scope="session")
def redis_client(redis_container):
    import redis
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = redis.Redis(host=host, port=int(port), decode_responses=True)
    yield client
    client.close()


@pytest.fixture(scope="session")
def postgres_container():
    try:
        from testcontainers.postgres import PostgresContainer
        with PostgresContainer("postgres:16-alpine") as container:
            yield container
    except ImportError:
        pytest.skip("testcontainers not installed")
