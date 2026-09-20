# AIOps Log Anomaly Detection Lab

## Setup

```bash
python -m pip install -r requirements.txt
docker compose up -d kafka
python q2_kafka_setup.py
```

The Compose file runs a single-node KRaft broker at `localhost:9092` with
automatic topic creation disabled. Configuration is controlled with environment
variables such as `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_PARTITIONS`,
`KAFKA_REPLICATION_FACTOR`, `KAFKA_STARTUP_RETRIES`, and `CPU_ANOMALY_THRESHOLD`.

## Question 1

Run `python q1_anomaly_detection.py`. It validates the deterministic 20-record
dataset, reports statistics and the three CPU anomalies (`10:05`, `10:12`, and
`10:18`), and writes the headless chart to `q1_anomalies.png`.

## Kafka producer and consumer

```bash
python q2_kafka_producer.py
KAFKA_GROUP_ID=lab-consumer KAFKA_MESSAGE_COUNT=10 python q3_kafka_consumer.py
```

The producer defaults to `server01`, publishes JSON metrics with acknowledgements,
and supports `SERVER_ID`, `KAFKA_MESSAGE_COUNT`, and
`KAFKA_MESSAGE_INTERVAL_SECONDS`. The consumer validates every field, rejects
malformed messages, and emits the high CPU alert only when CPU is greater than
80 percent; exactly 80 percent is normal.

## Airflow

Copy `q4_airflow_aiops_dag.py` into the Airflow DAGs directory. Its four tasks
are `collect_metrics`, `process_metrics`, `detect_anomaly`, and
`generate_report`, connected in that order. Airflow settings use
`AIRFLOW_SCHEDULE`, `AIRFLOW_RETRIES`, `AIRFLOW_RETRY_DELAY_SECONDS`, and
`CPU_ANOMALY_THRESHOLD`.

## Integrated monitor

```bash
KAFKA_GROUP_ID=lab-monitor python q5_integrated_aiops_monitor.py
```

Use `max_messages` from Python for deterministic tests. The monitor shares the
Question 3 validation behavior, logs running totals, and reports total detected
anomalies before clean shutdown.

## Validation

```bash
python -m py_compile q*.py anomaly_detection.py
python -m unittest discover
docker compose config
git diff --check
```

Airflow is optional for local structural validation; when it is unavailable the
DAG module exposes `dag = None` while its task functions remain importable.