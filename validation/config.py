from pydantic_settings import BaseSettings
from pydantic import Field


class ValidationConfig(BaseSettings):
    consistency_threshold: float = Field(default=0.01, alias="CONSISTENCY_THRESHOLD")
    sample_size: int = Field(default=1000, alias="VALIDATION_SAMPLE_SIZE")
    psi_warn_threshold: float = Field(default=0.1, alias="PSI_WARN_THRESHOLD")
    psi_alert_threshold: float = Field(default=0.2, alias="PSI_ALERT_THRESHOLD")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = ValidationConfig()
