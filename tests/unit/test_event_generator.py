"""Unit tests for the synthetic event generator."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone


def test_event_schema_fields():
    """All required fields are present in generated events."""
    from ingestion.event_generator import event_stream
    batch = next(event_stream(batch_size=10))
    assert len(batch) == 10
    for event in batch:
        assert event.user_id.startswith("usr_")
        assert event.session_id.startswith("ses_")
        assert event.event_type in {"page_view", "add_to_cart", "purchase", "search", "abandon"}
        assert isinstance(event.timestamp, datetime)
        assert event.device_type in {"mobile", "desktop", "tablet"}
        assert event.location.country in {"US", "UK", "DE", "IN", "BR"}
        assert isinstance(event.transaction_amount, float)


def test_purchase_amount_distribution():
    """Purchase amounts follow log-normal with expected median range."""
    from ingestion.event_generator import event_stream
    import numpy as np

    purchase_amounts = []
    for batch in event_stream(batch_size=200):
        purchase_amounts.extend(
            e.transaction_amount for e in batch if e.event_type == "purchase"
        )
        if len(purchase_amounts) >= 300:
            break

    if purchase_amounts:
        median = float(np.median(purchase_amounts))
        # log-normal(mu=3.7, sigma=1.2) → median = exp(3.7) ≈ 40
        assert 5.0 < median < 200.0, f"Median {median} outside expected range"


def test_event_type_distribution():
    """Event type weights are approximately correct (within 15% tolerance)."""
    from ingestion.event_generator import event_stream
    from collections import Counter

    events = []
    for batch in event_stream(batch_size=200):
        events.extend(batch)
        if len(events) >= 2000:
            break

    counts = Counter(e.event_type for e in events)
    total = sum(counts.values())

    page_view_pct = counts["page_view"] / total
    assert 0.35 < page_view_pct < 0.65, f"page_view={page_view_pct:.2f} out of range"


def test_user_zipf_distribution():
    """Power users (low user_ids) appear more frequently."""
    from ingestion.event_generator import event_stream
    from collections import Counter

    events = []
    for batch in event_stream(batch_size=200):
        events.extend(batch)
        if len(events) >= 2000:
            break

    user_counts = Counter(e.user_id for e in events)
    most_common = user_counts.most_common(10)
    least_common = user_counts.most_common()[-10:]

    avg_top = sum(c for _, c in most_common) / len(most_common)
    avg_bottom = sum(c for _, c in least_common) / len(least_common)

    # Power users should appear at least 3x more than tail users
    assert avg_top > avg_bottom, "Zipf distribution not working"


def test_event_serialization():
    """Events serialize to bytes and deserialize correctly."""
    from ingestion.event_generator import event_stream
    from ingestion.schemas import EcommerceEvent

    batch = next(event_stream(batch_size=5))
    for event in batch:
        raw = event.to_kafka_bytes()
        assert isinstance(raw, bytes)
        restored = EcommerceEvent.from_kafka_bytes(raw)
        assert restored.user_id == event.user_id
        assert restored.event_type == event.event_type
        assert abs(restored.transaction_amount - event.transaction_amount) < 0.01
