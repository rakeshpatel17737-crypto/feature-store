from pydantic_settings import BaseSettings
from pydantic import Field


class MonitoringConfig(BaseSettings):
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="feature_store", alias="POSTGRES_DB")
    postgres_user: str = Field(default="featurestore", alias="POSTGRES_USER")
    postgres_password: str = Field(default="featurestore_secret", alias="POSTGRES_PASSWORD")
    feature_store_api_url: str = Field(default="http://localhost:8000", alias="FEATURE_STORE_API_URL")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = MonitoringConfig()
