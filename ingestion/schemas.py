from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class Location(BaseModel):
    country: str
    city: str


class EcommerceEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    session_id: str
    event_type: Literal["page_view", "add_to_cart", "purchase", "search", "abandon"]
    timestamp: datetime
    transaction_amount: float
    product_id: str
    category: str
    location: Location
    device_type: Literal["mobile", "desktop", "tablet"]
    schema_version: str = "1.0"

    def to_kafka_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_bytes(cls, data: bytes) -> "EcommerceEvent":
        return cls.model_validate_json(data)
