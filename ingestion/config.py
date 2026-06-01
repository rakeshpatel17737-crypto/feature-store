from pydantic_settings import BaseSettings
from pydantic import Field


class IngestionConfig(BaseSettings):
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_events: str = Field(default="ecommerce.events.raw", alias="KAFKA_TOPIC_EVENTS")
    event_rate_tps: int = Field(default=100, alias="EVENT_RATE_TPS")
    num_users: int = Field(default=10_000, alias="NUM_USERS")
    producer_batch_size: int = Field(default=100, alias="PRODUCER_BATCH_SIZE")
    producer_linger_ms: int = Field(default=10, alias="PRODUCER_LINGER_MS")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = IngestionConfig()
