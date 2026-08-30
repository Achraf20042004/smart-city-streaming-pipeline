"""Hourly health checks for Kafka, ClickHouse, Trino, and Prometheus."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "smartcity",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="smartcity_health_check",
    default_args=default_args,
    description="Verify smart city pipeline services and data availability",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smartcity", "monitoring"],
) as dag:
    check_clickhouse = BashOperator(
        task_id="check_clickhouse_row_count",
        bash_command=(
            'count=$(curl -sf "http://clickhouse:8123/?query=SELECT%20count()%20FROM%20smartcity.vehicle_data") && '
            'echo "vehicle_data rows: $count" && '
            'test "$count" -ge 0'
        ),
    )

    check_trino = BashOperator(
        task_id="check_trino_api",
        bash_command='curl -sf http://trino:8080/v1/info | grep -q nodeVersion',
    )

    check_kafka_exporter = BashOperator(
        task_id="check_kafka_metrics",
        bash_command=(
            'curl -sf http://kafka-exporter:9308/metrics | grep -q kafka_topic_partition_current_offset'
        ),
    )

    check_prometheus = BashOperator(
        task_id="check_prometheus_targets",
        bash_command=(
            'curl -sf http://prometheus:9090/api/v1/targets | grep -q \'"health":"up"\''
        ),
    )

    [check_clickhouse, check_trino, check_kafka_exporter, check_prometheus]
