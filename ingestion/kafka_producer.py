from __future__ import annotations

import logging
from typing import Callable

from confluent_kafka import Producer, KafkaException
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from .config import config

logger = logging.getLogger(__name__)


def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Kafka delivery failed", extra={"topic": msg.topic(), "error": str(err)})


class FeatureStoreProducer:
    def __init__(self) -> None:
        self._producer = Producer({
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "batch.size": config.producer_batch_size * 1024,
            "linger.ms": config.producer_linger_ms,
            "compression.type": "snappy",
            "acks": "1",
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def produce(self, topic: str, key: str, value: bytes) -> None:
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                on_delivery=_delivery_report,
            )
        except KafkaException as exc:
            logger.error("Kafka produce error: %s", exc)
            raise

    def flush(self, timeout: float = 5.0) -> int:
        return self._producer.flush(timeout)

    def poll(self, timeout: float = 0.0) -> int:
        return self._producer.poll(timeout)
