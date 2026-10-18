"""Hourly data quality checks on the ClickHouse real-time tables."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "smartcity",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def clickhouse_check(task_id: str, label: str, query: str, condition: str) -> BashOperator:
    """Run a scalar query on ClickHouse and fail the task unless `test "$value" <condition>` holds."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
value=$(curl -sf -X POST 'http://clickhouse:8123/' --data-binary @- <<'EOF'
{query}
EOF
) && echo "{label}: $value" && test "$value" {condition}
""",
    )


with DAG(
    dag_id="smartcity_data_quality",
    default_args=default_args,
    description="Freshness, dead-letter, duplicate and value-range checks",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smartcity", "quality"],
) as dag:
    # Fails when the producer has not sent anything for an hour.
    check_freshness = clickhouse_check(
        "check_vehicle_freshness",
        "minutes since last vehicle event",
        "SELECT dateDiff('minute', max(ingested_at), now()) FROM smartcity.vehicle_data",
        "-le 60",
    )

    check_dead_letter = clickhouse_check(
        "check_dead_letter_queue",
        "rejected messages in the last hour",
        "SELECT count() FROM smartcity.kafka_errors WHERE received_at > now() - INTERVAL 1 HOUR",
        "-eq 0",
    )

    check_duplicates = clickhouse_check(
        "check_vehicle_duplicates",
        "duplicate vehicle ids in the last hour",
        """
SELECT count() - uniqExact(id)
FROM smartcity.vehicle_data
WHERE ingested_at > now() - INTERVAL 1 HOUR
""",
        "-eq 0",
    )

    check_ranges = clickhouse_check(
        "check_value_ranges",
        "out-of-range values in the last hour",
        """
SELECT
    (SELECT countIf(speed < 0 OR speed > 300)
     FROM smartcity.vehicle_data
     WHERE ingested_at > now() - INTERVAL 1 HOUR)
  + (SELECT countIf(humidity < 0 OR humidity > 100 OR airQualityIndex < 0)
     FROM smartcity.weather_data
     WHERE ingested_at > now() - INTERVAL 1 HOUR)
""",
        "-eq 0",
    )

    [check_freshness, check_dead_letter, check_duplicates, check_ranges]
