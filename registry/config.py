from pydantic_settings import BaseSettings
from pydantic import Field


class RegistryConfig(BaseSettings):
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="feature_store", alias="POSTGRES_DB")
    postgres_user: str = Field(default="featurestore", alias="POSTGRES_USER")
    postgres_password: str = Field(default="featurestore_secret", alias="POSTGRES_PASSWORD")

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def sync_database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = RegistryConfig()
