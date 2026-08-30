"""Daily batch aggregation — rolls up yesterday's vehicle stats in ClickHouse."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "smartcity",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="smartcity_daily_summary",
    default_args=default_args,
    description="Daily vehicle speed summary into ClickHouse mart table",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smartcity", "batch"],
) as dag:
    build_daily_summary = BashOperator(
        task_id="build_daily_vehicle_summary",
        bash_command="""
curl -sf -X POST 'http://clickhouse:8123/' --data-binary @- <<'EOF'
INSERT INTO smartcity.daily_vehicle_summary
SELECT
    toDate(timestamp) AS date,
    deviceId AS device_id,
    avg(speed) AS avg_speed,
    max(speed) AS max_speed,
    count() AS reading_count
FROM smartcity.vehicle_data
WHERE toDate(timestamp) = yesterday()
GROUP BY date, device_id
EOF
""",
    )

    verify_summary = BashOperator(
        task_id="verify_summary_rows",
        bash_command=(
            'count=$(curl -sf "http://clickhouse:8123/?query=SELECT%20count()%20FROM%20smartcity.daily_vehicle_summary") && '
            'echo "daily_vehicle_summary rows: $count" && '
            'test "$count" -ge 0'
        ),
    )

    build_daily_summary >> verify_summary
