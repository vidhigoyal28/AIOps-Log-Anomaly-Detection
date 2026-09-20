"""Validated Kafka consumer with explicit high CPU alert behavior."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Any, Iterable


LOGGER = logging.getLogger(__name__)
STOP = False
REQUIRED_FIELDS = {"server_id", "cpu_usage", "memory_usage", "disk_usage", "timestamp"}


def validate_metric(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != REQUIRED_FIELDS:
        raise ValueError("metric must contain exactly the required fields")
    if not isinstance(value["server_id"], str) or not value["server_id"]:
        raise ValueError("server_id must be a non-empty string")
    for field in ("cpu_usage", "memory_usage", "disk_usage"):
        if (isinstance(value[field], bool) or
                not isinstance(value[field], (int, float)) or
                not 0 <= value[field] <= 100):
            raise ValueError(f"{field} must be a number from 0 to 100")
    if not isinstance(value["timestamp"], str):
        raise ValueError("timestamp must be a string")
    try:
        datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be ISO-8601") from exc
    return value


def process_message(raw: Any, logger: logging.Logger = LOGGER) -> bool:
    try:
        value = raw if isinstance(raw, dict) else json.loads(raw)
        metric = validate_metric(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Rejected malformed metric: %s", exc)
        return False
    logger.info("Valid metric: %s", metric)
    if metric["cpu_usage"] > 80:
        logger.warning("ALERT: High CPU detected on %s", metric["server_id"])
    return True


def stop_handler(signum: int, _frame: Any) -> None:
    global STOP
    STOP = True
    LOGGER.info("Received signal %s; stopping consumer", signum)


def consume(max_messages: int | None = None) -> int:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "server_metrics"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "aiops-consumer"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=True,
        consumer_timeout_ms=int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "10000")),
    )
    processed = 0
    try:
        while not STOP and (max_messages is None or processed < max_messages):
            try:
                for message in consumer:
                    if STOP:
                        break
                    if process_message(message.value):
                        processed += 1
                    if max_messages is not None and processed >= max_messages:
                        break
            except Exception as exc:
                LOGGER.warning("Temporary Kafka consumer failure: %s", exc)
                time.sleep(float(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "2")))
    finally:
        consumer.close()
    return processed


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    consume()