"""Integrated Kafka AIOps monitor."""

from __future__ import annotations

import logging
import json
import os
import signal
import time
from typing import Any

from q3_kafka_consumer import process_message, validate_metric


LOGGER = logging.getLogger(__name__)
STOP = False


def stop_handler(signum: int, _frame: Any) -> None:
    global STOP
    STOP = True
    LOGGER.info("Received signal %s; stopping integrated monitor", signum)


def monitor(consumer: Any, max_messages: int | None = None) -> dict[str, int]:
    processed = 0
    anomalies = 0
    messages = iter(consumer)
    while not STOP and (max_messages is None or processed < max_messages):
        try:
            message = next(messages)
        except StopIteration:
            break
        except Exception as exc:
            LOGGER.warning("Temporary Kafka failure: %s", exc)
            time.sleep(float(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "2")))
            continue
        try:
            value = message.value
            metric = validate_metric(value if isinstance(value, dict) else
                                     json.loads(value))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.warning("Rejected malformed metric: %s", exc)
            continue
        if process_message(metric):
            processed += 1
            if metric["cpu_usage"] > float(
                    os.getenv("CPU_ANOMALY_THRESHOLD", "80")):
                anomalies += 1
            LOGGER.info("Running totals: processed_metrics=%d running_anomalies=%d",
                        processed, anomalies)
    report = {"processed_metrics": processed, "total_anomalies": anomalies}
    LOGGER.info("Final report: Total anomalies detected: %d", anomalies)
    return report


def run(max_messages: int | None = None) -> dict[str, int]:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "server_metrics"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "aiops-integrated-monitor"),
        auto_offset_reset="earliest",
        consumer_timeout_ms=int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "10000")),
    )
    try:
        return monitor(consumer, max_messages)
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    run()