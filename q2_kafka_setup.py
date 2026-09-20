"""Idempotent Kafka topic setup."""

from __future__ import annotations

import logging
import os
import time


LOGGER = logging.getLogger(__name__)


def env_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def create_topic() -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "server_metrics")
    retries = env_int("KAFKA_STARTUP_RETRIES", 12)
    delay = float(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "2"))
    for attempt in range(1, retries + 1):
        admin = None
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers,
                                     client_id="aiops-topic-setup")
            try:
                admin.create_topics([NewTopic(
                    name=topic,
                    num_partitions=env_int("KAFKA_PARTITIONS", 1),
                    replication_factor=env_int("KAFKA_REPLICATION_FACTOR", 1),
                )])
                LOGGER.info("Created Kafka topic %s", topic)
            except TopicAlreadyExistsError:
                LOGGER.info("Kafka topic %s already exists", topic)
            return
        except Exception as exc:
            LOGGER.warning("Kafka topic setup attempt %d/%d failed: %s",
                           attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(delay)
        finally:
            if admin is not None:
                admin.close()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    create_topic()