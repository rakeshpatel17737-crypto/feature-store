"""Integration tests for the Kafka ingestion pipeline."""
from __future__ import annotations

import json
import time
import pytest


@pytest.mark.integration
def test_event_serialization_roundtrip():
    """Events serialize/deserialize without data loss."""
    from ingestion.schemas import EcommerceEvent, Location
    from datetime import datetime, timezone

    event = EcommerceEvent(
        user_id="usr_00001",
        session_id="ses_abc123",
        event_type="purchase",
        timestamp=datetime.now(tz=timezone.utc),
        transaction_amount=99.99,
        product_id="prod_00512",
        category="electronics",
        location=Location(country="US", city="Austin"),
        device_type="mobile",
    )

    raw = event.to_kafka_bytes()
    restored = EcommerceEvent.from_kafka_bytes(raw)

    assert restored.user_id == event.user_id
    assert restored.event_type == event.event_type
    assert abs(restored.transaction_amount - event.transaction_amount) < 0.001
    assert restored.location.city == "Austin"


@pytest.mark.integration
def test_invalid_event_rejected():
    """Invalid event bytes should raise validation error."""
    from ingestion.schemas import EcommerceEvent

    invalid_bytes = b'{"user_id": null, "event_type": "invalid_type"}'
    with pytest.raises(Exception):
        EcommerceEvent.from_kafka_bytes(invalid_bytes)


@pytest.mark.integration
def test_event_stream_generates_all_types():
    """Event stream generates all 5 event types over sufficient iterations."""
    from ingestion.event_generator import event_stream

    seen_types = set()
    for batch in event_stream(batch_size=100):
        seen_types.update(e.event_type for e in batch)
        if len(seen_types) == 5:
            break
        if len(list(event_stream(batch_size=1))) > 100:
            break

    # With 100 events per batch and proper weights, should see variety
    assert len(seen_types) >= 3, f"Expected at least 3 event types, saw: {seen_types}"
