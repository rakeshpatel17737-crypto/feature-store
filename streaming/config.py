from pydantic_settings import BaseSettings
from pydantic import Field


class StreamingConfig(BaseSettings):
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_events: str = Field(default="ecommerce.events.raw", alias="KAFKA_TOPIC_EVENTS")
    kafka_consumer_group: str = Field(default="feature-processor-cg", alias="KAFKA_CONSUMER_GROUP")
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    feature_ttl_seconds: int = Field(default=300, alias="FEATURE_TTL_SECONDS")
    delta_table_path: str = Field(default="/data/feature_store", alias="DELTA_TABLE_PATH")
    spark_master_url: str = Field(default="local[*]", alias="SPARK_MASTER_URL")
    spark_checkpoint_dir: str = Field(default="/tmp/spark-checkpoints", alias="SPARK_CHECKPOINT_DIR")
    processing_time: str = "30 seconds"
    watermark_duration: str = "10 minutes"

    model_config = {"env_file": ".env", "extra": "ignore", "populate_by_name": True}


config = StreamingConfig()
