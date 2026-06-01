from pydantic_settings import BaseSettings
from pydantic import Field


class DriftConfig(BaseSettings):
    psi_warn_threshold: float = Field(default=0.1, alias="PSI_WARN_THRESHOLD")
    psi_alert_threshold: float = Field(default=0.2, alias="PSI_ALERT_THRESHOLD")
    ks_pvalue_threshold: float = Field(default=0.05, alias="KS_PVALUE_THRESHOLD")
    zscore_alert_threshold: float = Field(default=3.0, alias="ZSCORE_ALERT_THRESHOLD")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = DriftConfig()
