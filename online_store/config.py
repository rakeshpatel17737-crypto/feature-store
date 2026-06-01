from pydantic_settings import BaseSettings
from pydantic import Field


class OnlineStoreConfig(BaseSettings):
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_max_connections: int = Field(default=20, alias="REDIS_MAX_CONNECTIONS")
    feature_ttl_seconds: int = Field(default=300, alias="FEATURE_TTL_SECONDS")
    refresh_threshold_ratio: float = 0.80  # Trigger refresh at 80% of TTL

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = OnlineStoreConfig()
