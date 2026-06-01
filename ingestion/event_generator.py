"""Continuous synthetic e-commerce event generator.

Statistical realism:
- Users: Zipf distribution over 10k pool (power users generate more events)
- Amounts: log-normal (μ=3.7, σ=1.2) → median ~$40, long tail to ~$500
- Event types: weighted (page_view 50%, cart 20%, purchase 15%, search 10%, abandon 5%)
- Fraud injection: 2% of purchases are 10× normal (simulated anomalies)
"""
from __future__ import annotations

import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Generator

import numpy as np
from faker import Faker

from .config import config
from .kafka_producer import FeatureStoreProducer
from .schemas import EcommerceEvent, Location

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()

_EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "abandon"]
_EVENT_WEIGHTS = [0.50, 0.20, 0.15, 0.10, 0.05]

_CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "beauty", "food", "toys"]
_DEVICE_TYPES = ["mobile", "desktop", "tablet"]
_DEVICE_WEIGHTS = [0.60, 0.30, 0.10]

_CITIES_BY_COUNTRY = {
    "US": ["New York", "Los Angeles", "Chicago", "Austin", "Seattle"],
    "UK": ["London", "Manchester", "Birmingham"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "IN": ["Mumbai", "Bangalore", "Delhi"],
    "BR": ["São Paulo", "Rio de Janeiro"],
}
_COUNTRIES = list(_CITIES_BY_COUNTRY.keys())

_rng = np.random.default_rng(seed=42)


def _zipf_user_pool(n: int, num_users: int) -> list[str]:
    """Sample n user_ids from a Zipf distribution over num_users."""
    ranks = _rng.zipf(a=1.5, size=n)
    ranks = np.clip(ranks, 1, num_users)
    return [f"usr_{r:05d}" for r in ranks]


def _random_location() -> Location:
    country = random.choice(_COUNTRIES)
    city = random.choice(_CITIES_BY_COUNTRY[country])
    return Location(country=country, city=city)


def _random_amount(event_type: str, is_fraud: bool) -> float:
    if event_type != "purchase":
        return 0.0
    amount = float(np.exp(_rng.normal(loc=3.7, scale=1.2)))
    amount = round(max(1.0, min(amount, 2000.0)), 2)
    if is_fraud:
        amount = round(amount * 10.0, 2)
    return amount


def _generate_session_id() -> str:
    return f"ses_{uuid.uuid4().hex[:12]}"


def event_stream(batch_size: int = 50) -> Generator[list[EcommerceEvent], None, None]:
    """Yield batches of synthetic events continuously."""
    session_map: dict[str, str] = {}

    while True:
        user_ids = _zipf_user_pool(batch_size, config.num_users)
        event_types = random.choices(_EVENT_TYPES, weights=_EVENT_WEIGHTS, k=batch_size)
        device_types = random.choices(_DEVICE_TYPES, weights=_DEVICE_WEIGHTS, k=batch_size)
        categories = random.choices(_CATEGORIES, k=batch_size)

        events: list[EcommerceEvent] = []
        for user_id, event_type, device_type, category in zip(
            user_ids, event_types, device_types, categories
        ):
            if user_id not in session_map or random.random() < 0.05:
                session_map[user_id] = _generate_session_id()
            session_id = session_map[user_id]

            is_fraud = event_type == "purchase" and random.random() < 0.02

            events.append(
                EcommerceEvent(
                    user_id=user_id,
                    session_id=session_id,
                    event_type=event_type,
                    timestamp=datetime.now(tz=timezone.utc),
                    transaction_amount=_random_amount(event_type, is_fraud),
                    product_id=f"prod_{random.randint(1, 5000):05d}",
                    category=category,
                    location=_random_location(),
                    device_type=device_type,
                )
            )
        yield events


def run() -> None:
    producer = FeatureStoreProducer()
    topic = config.kafka_topic_events
    tps = config.event_rate_tps
    batch_size = min(tps, 200)
    sleep_interval = batch_size / tps

    total_sent = 0
    start = time.monotonic()

    def _shutdown(sig, frame):
        producer.flush(timeout=10)
        elapsed = time.monotonic() - start
        logger.info("Shutdown. Sent %d events in %.1fs (%.0f/s)", total_sent, elapsed, total_sent / max(elapsed, 1))
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Event generator starting: %d TPS, topic=%s", tps, topic)

    for batch in event_stream(batch_size=batch_size):
        for event in batch:
            producer.produce(topic=topic, key=event.user_id, value=event.to_kafka_bytes())
            total_sent += 1

        producer.poll(timeout=0.0)
        time.sleep(sleep_interval)

        if total_sent % 10_000 == 0:
            elapsed = time.monotonic() - start
            logger.info("Sent %d events | %.0f events/sec", total_sent, total_sent / elapsed)


if __name__ == "__main__":
    run()
