from pydantic_settings import BaseSettings
from pydantic import Field


class APIConfig(BaseSettings):
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    environment: str = Field(default="development", alias="ENVIRONMENT")
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_max_connections: int = Field(default=20, alias="REDIS_MAX_CONNECTIONS")
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field(default="claude-sonnet-4-5", alias="ANTHROPIC_MODEL")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = APIConfig()
