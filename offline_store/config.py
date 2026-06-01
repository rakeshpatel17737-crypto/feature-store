from pydantic_settings import BaseSettings
from pydantic import Field


class OfflineStoreConfig(BaseSettings):
    delta_table_path: str = Field(default="/data/feature_store", alias="DELTA_TABLE_PATH")
    spark_master_url: str = Field(default="local[*]", alias="SPARK_MASTER_URL")
    spark_checkpoint_dir: str = Field(default="/tmp/spark-checkpoints", alias="SPARK_CHECKPOINT_DIR")
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = OfflineStoreConfig()
