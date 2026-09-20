"""Airflow DAG for the AIOps metric workflow."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any


LOGGER = logging.getLogger(__name__)


def collect_metrics(**_context: Any) -> dict[str, float]:
    metrics = {"cpu_usage": 87.0, "memory_usage": 65.0,
               "response_time_ms": 420.0}
    LOGGER.info("Collected metrics: %s", metrics)
    return metrics


def process_metrics(ti: Any, **_context: Any) -> dict[str, float]:
    metrics = ti.xcom_pull(task_ids="collect_metrics")
    if not isinstance(metrics, dict) or set(metrics) != {
            "cpu_usage", "memory_usage", "response_time_ms"}:
        raise ValueError("collected metrics are invalid")
    if not 0 <= metrics["cpu_usage"] <= 100 or not 0 <= metrics["memory_usage"] <= 100:
        raise ValueError("percentage metric out of range")
    if metrics["response_time_ms"] < 0:
        raise ValueError("response time cannot be negative")
    LOGGER.info("Processed metrics: %s", metrics)
    return metrics


def detect_anomaly(ti: Any, **_context: Any) -> bool:
    metrics = ti.xcom_pull(task_ids="process_metrics")
    threshold = float(os.getenv("CPU_ANOMALY_THRESHOLD", "80"))
    anomaly = metrics["cpu_usage"] > threshold
    LOGGER.info("CPU anomaly=%s CPU=%s threshold=%s",
                anomaly, metrics["cpu_usage"], threshold)
    return anomaly


def generate_report(ti: Any, **_context: Any) -> dict[str, Any]:
    report = {
        "metrics": ti.xcom_pull(task_ids="process_metrics"),
        "anomaly": ti.xcom_pull(task_ids="detect_anomaly"),
    }
    LOGGER.info("Final AIOps report: %s", report)
    return report


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    dag = DAG(
        dag_id=os.getenv("AIRFLOW_DAG_ID", "aiops_monitor"),
        start_date=datetime(2024, 1, 1),
        schedule=os.getenv("AIRFLOW_SCHEDULE", "@hourly"),
        catchup=False,
        default_args={
            "owner": os.getenv("AIRFLOW_OWNER", "aiops"),
            "retries": int(os.getenv("AIRFLOW_RETRIES", "2")),
            "retry_delay": timedelta(
                seconds=int(os.getenv("AIRFLOW_RETRY_DELAY_SECONDS", "30"))
            ),
        },
    )
    with dag:
        collect_task = PythonOperator(task_id="collect_metrics", python_callable=collect_metrics)
        process_task = PythonOperator(task_id="process_metrics", python_callable=process_metrics)
        detect_task = PythonOperator(task_id="detect_anomaly", python_callable=detect_anomaly)
        report_task = PythonOperator(task_id="generate_report", python_callable=generate_report)
        collect_task >> process_task >> detect_task >> report_task
except ImportError:
    dag = None
