"""Deterministic CPU anomaly detection example."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


LOGGER = logging.getLogger(__name__)
DEFAULT_THRESHOLD = 80.0


@dataclass(frozen=True)
class ServerRecord:
    timestamp: str
    cpu_usage: float
    memory_usage: float
    response_time_ms: float


def sample_records() -> list[ServerRecord]:
    cpu_values = [45, 50, 52, 48, 55, 95, 60, 58, 62, 65,
                  67, 70, 97, 72, 68, 75, 80, 79, 92, 78]
    memory_values = [50, 52, 51, 53, 55, 60, 62, 61, 64, 65,
                     67, 68, 70, 71, 72, 74, 76, 78, 80, 79]
    response_values = [200, 210, 220, 205, 230, 250, 260, 270, 280, 290,
                       300, 310, 320, 330, 340, 350, 360, 370, 380, 390]
    return [
        ServerRecord(f"10:{index:02d}", cpu, memory, response)
        for index, (cpu, memory, response) in enumerate(
            zip(cpu_values, memory_values, response_values)
        )
    ]


def validate_records(records: Iterable[ServerRecord]) -> list[ServerRecord]:
    validated = list(records)
    if len(validated) != 20:
        raise ValueError("the sample dataset must contain exactly 20 records")
    for record in validated:
        if not record.timestamp or not 0 <= record.cpu_usage <= 100:
            raise ValueError(f"invalid record: {record!r}")
        if not 0 <= record.memory_usage <= 100 or record.response_time_ms < 0:
            raise ValueError(f"invalid record: {record!r}")
    return validated


def calculate_statistics(records: Iterable[ServerRecord]) -> dict[str, dict[str, float]]:
    values = {
        "cpu_usage": [record.cpu_usage for record in records],
        "memory_usage": [record.memory_usage for record in records],
        "response_time_ms": [record.response_time_ms for record in records],
    }
    return {
        name: {
            "average": mean(series),
            "minimum": min(series),
            "maximum": max(series),
            "standard_deviation": pstdev(series),
        }
        for name, series in values.items()
    }


def detect_cpu_anomalies(
    records: Iterable[ServerRecord], threshold: float = DEFAULT_THRESHOLD
) -> list[ServerRecord]:
    if not 0 <= threshold <= 100:
        raise ValueError("CPU threshold must be between 0 and 100")
    return [record for record in records if record.cpu_usage > threshold]


def create_chart(
    records: Iterable[ServerRecord],
    anomalies: Iterable[ServerRecord],
    threshold: float,
    output_path: str = "q1_anomalies.png",
) -> None:
    record_list = list(records)
    anomaly_list = list(anomalies)
    plt.figure(figsize=(10, 5))
    plt.plot([record.timestamp for record in record_list],
             [record.cpu_usage for record in record_list], marker="o",
             label="CPU usage")
    plt.scatter([record.timestamp for record in anomaly_list],
                [record.cpu_usage for record in anomaly_list],
                color="red", label="Anomaly", zorder=3)
    plt.axhline(threshold, color="orange", linestyle="--",
                label=f"Threshold ({threshold:g}%)")
    plt.xlabel("Timestamp")
    plt.ylabel("CPU usage (%)")
    plt.title("Server CPU anomaly detection")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    threshold = float(os.getenv("Q1_CPU_THRESHOLD", str(DEFAULT_THRESHOLD)))
    records = validate_records(sample_records())
    anomalies = detect_cpu_anomalies(records, threshold)
    LOGGER.info("Total records: %d", len(records))
    LOGGER.info("Anomaly count: %d", len(anomalies))
    LOGGER.info("Statistics: %s", calculate_statistics(records))
    for record in records:
        status = "ANOMALY" if record in anomalies else "normal"
        print(f"{record.timestamp} CPU={record.cpu_usage:g}% status={status}")
    create_chart(records, anomalies, threshold)
    LOGGER.info("Saved chart to q1_anomalies.png")


if __name__ == "__main__":
    main()