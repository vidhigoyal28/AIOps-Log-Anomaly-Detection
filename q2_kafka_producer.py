"""Kafka producer for deterministic server metrics."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any


LOGGER = logging.getLogger(__name__)
STOP = False


def validate_metric(metric: dict[str, Any]) -> dict[str, Any]:
    required = {"server_id", "cpu_usage", "memory_usage", "disk_usage", "timestamp"}
    if set(metric) != required or not isinstance(metric["server_id"], str):
        raise ValueError("metric has invalid fields")
    for field in ("cpu_usage", "memory_usage", "disk_usage"):
        if not isinstance(metric[field], (int, float)) or not 0 <= metric[field] <= 100:
            raise ValueError(f"invalid {field}")
    if not isinstance(metric["timestamp"], str) or not metric["timestamp"]:
        raise ValueError("invalid timestamp")
    return metric


def generate_metric(server_id: str, index: int) -> dict[str, Any]:
    return validate_metric({
        "server_id": server_id,
        "cpu_usage": float(45 + (index * 7) % 51),
        "memory_usage": float(55 + (index * 5) % 41),
        "disk_usage": float(35 + (index * 9) % 56),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


def stop_handler(signum: int, _frame: Any) -> None:
    global STOP
    STOP = True
    LOGGER.info("Received signal %s; stopping producer", signum)


def produce_messages() -> int:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all",
        retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", "5")),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    topic = os.getenv("KAFKA_TOPIC", "server_metrics")
    server_id = os.getenv("SERVER_ID", "server01")
    count = int(os.getenv("KAFKA_MESSAGE_COUNT", "10"))
    interval = float(os.getenv("KAFKA_MESSAGE_INTERVAL_SECONDS", "0.1"))
    sent = 0
    try:
        for index in range(count):
            if STOP:
                break
            metric = generate_metric(server_id, index)
            metadata = producer.send(topic, metric).get(timeout=10)
            sent += 1
            LOGGER.info("Published metric %d to %s partition=%d offset=%d",
                        sent, metadata.topic, metadata.partition, metadata.offset)
            time.sleep(interval)
        producer.flush(timeout=10)
        return sent
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    produce_messages()