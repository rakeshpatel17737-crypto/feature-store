from pydantic_settings import BaseSettings
from pydantic import Field


class DiagnosticsConfig(BaseSettings):
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field(default="claude-sonnet-4-5", alias="ANTHROPIC_MODEL")
    rca_max_tokens: int = Field(default=1024, alias="RCA_MAX_TOKENS")
    rca_timeout_seconds: int = Field(default=30, alias="RCA_TIMEOUT_SECONDS")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = DiagnosticsConfig()
