# Smart City Streaming Pipeline

A local end-to-end data platform that simulates a vehicle driving from London to Birmingham. The car sends IoT-style events (GPS, traffic cameras, weather, emergencies) into Kafka. From there the data flows through two paths: Spark writes Parquet files to a data lake queryable with Trino, and ClickHouse ingests the same topics directly for fast analytics.

The stack runs in Docker. The only host-side Python dependency is the Kafka producer (`requirements.txt`). No cloud account needed.

## What you get

- **Streaming ingestion**  Python producer → Kafka (5 topics)
- **Batch/stream processing**  Spark Structured Streaming → MinIO (S3-compatible) → Hive Metastore
- **SQL on the lake**  Trino
- **Real-time OLAP**  ClickHouse (Kafka engine + MergeTree)
- **Orchestration**  Airflow (health checks + daily summaries)
- **Monitoring**  Prometheus + Grafana
- **CI**  GitHub Actions smoke tests on push

## Architecture

```
Producer (Python)
       │
       ▼
    Kafka ──────► ClickHouse (real-time)
       │
       ▼
 Spark Streaming
       │
       ▼
  MinIO (Parquet) ──► Hive Metastore ──► Trino

Airflow: scheduled health checks + daily rollups
Prometheus/Grafana: metrics from Kafka, ClickHouse, MinIO
```

## Prerequisites

- Docker Desktop with Compose v2
- Python 3.9+ (only for running the producer on the host)
- 12 GB RAM recommended for the full stack (8 GB minimum, may be tight)

**Hive Metastore jar:** `hive-jars/aws-java-sdk-bundle-*.jar` (~280 MB) is not in git (GitHub file size limit). It is downloaded automatically by `scripts/setup.ps1`, or manually:

```powershell
.\scripts\download-hive-jars.ps1
```

```bash
bash scripts/download-hive-jars.sh
```

## Quick start

### Windows (recommended)

```powershell
cd project

# One-shot bootstrap: compose up, Kafka topics, MinIO bucket, ClickHouse schema
.\scripts\setup.ps1

# Python venv for the producer
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Start Spark streaming (not started automatically by compose)
.\scripts\run-spark.ps1

# Start the data simulator
python main.py
```

If `confluent_kafka` DLLs are blocked by Windows App Control, run the producer in Docker instead:

```powershell
.\scripts\run-producer.ps1
```

### Linux / macOS

```bash
cd project

# 0. Download the large Hive jar (not stored in git)
bash scripts/download-hive-jars.sh

# 1. Start the stack (Kafka topics and MinIO bucket are created automatically)
docker compose up -d

# 2. Wait until core services are healthy, then apply ClickHouse schema (safe to re-run)
for sql in clickhouse/init/*.sql; do
  docker exec -i clickhouse clickhouse-client --multiquery < "$sql"
done

# 3. Python venv for the producer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Start Spark streaming job
docker exec -u root -e IVY_HOME=/tmp/.ivy project-spark-master-1 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/spark-city.py

# 5. Start the data simulator
python main.py
```

### What `docker compose up` does automatically

- Starts all services (Kafka, Spark cluster, MinIO, Hive, ClickHouse, Trino, Airflow, monitoring)
- Creates the five Kafka topics (`kafka-init` container)
- Creates the MinIO bucket `spark-streaming-data` (`minio-init` container)
- Initializes Airflow DB and admin user (`airflow-init` container)

### What you must do manually

1. **Apply ClickHouse schema** , run `init-clickhouse` after ClickHouse is healthy (see steps above).
2. **Submit the Spark streaming job** : Spark workers start with compose, but `spark-city.py` must be submitted separately.
3. **Run the producer** : start after ClickHouse schema is in place so real-time tables can consume messages.

### Producer behavior

The simulator publishes one batch of five events every **5 seconds** while the vehicle moves from London to Birmingham (~100 steps, **~8 minutes** total). It then stops automatically. Re-run `python main.py` to generate another journey.

### Python dependencies

`requirements.txt` contains only what the host-side producer needs:

```
confluent-kafka==2.15.0
simplejson==4.1.1
```

That is sufficient. Spark (`pyspark`), Airflow, Trino, and ClickHouse all run inside their Docker images and do not use this file.

## Web interfaces

| Service     | URL                        | Login                   |
|-------------|----------------------------|-------------------------|
| Spark UI    | http://localhost:9090      | :                       |
| Trino       | http://localhost:8080      | :                       |
| ClickHouse  | http://localhost:8123/play | :                       |
| Grafana     | http://localhost:3000      | admin / admin           |
| Prometheus  | http://localhost:9091      | :                       |
| Airflow     | http://localhost:8085      | admin / admin           |
| MinIO       | http://localhost:9001      | minioadmin / minioadmin |

Airflow may take a minute or two to become ready after the rest of the stack is up.

## Kafka topics

| Topic           | Content                         |
|-----------------|---------------------------------|
| vehicle_data    | speed, location, make, model    |
| gps_data        | GPS speed, location, direction  |
| traffic_data    | camera snapshots                |
| weather_data    | temperature, AQI, precipitation |
| emergency_data  | incidents (accident, fire, etc.) |

Spark also writes `vehicle_speed_1min`, a windowed aggregate (average/max speed and reading count per vehicle per minute). It relies on the 2-minute watermark, so each window lands in the lake about 2 minutes after it closes.

## Querying the data

Give the pipeline 30–60 seconds after starting the producer before querying.

**Trino** (data lake):

```bash
docker exec -it trino trino --execute "SELECT COUNT(*) FROM hive.smartcity.vehicle_data"
```

**ClickHouse** (real-time):

```bash
docker exec clickhouse clickhouse-client --query "SELECT count() FROM smartcity.vehicle_data"
```

**Verify the Spark → MinIO → Hive path:**

```bash
docker exec project-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/verify_pipeline.py
```

## Data quality

Every event must carry a non-empty `id` and `deviceId` and a parseable `timestamp`. The same rule is applied on both paths, and rejected events are kept rather than dropped:

| Path       | Invalid events go to                     | How                                                                 |
|------------|------------------------------------------|---------------------------------------------------------------------|
| ClickHouse | `smartcity.kafka_errors` (dead-letter)   | `kafka_handle_error_mode = 'stream'` + `*_errors_mv` views          |
| Spark      | `smartcity.rejected_events` (Hive table) | invalid rows are filtered out of the topic tables and quarantined   |

The `smartcity_data_quality` DAG then checks, every hour: data freshness, dead-letter volume, duplicate ids and value ranges.

```sql
SELECT topic, error, raw_message FROM smartcity.kafka_errors ORDER BY received_at DESC LIMIT 10;
```

**Upgrading an existing ClickHouse volume:** `CREATE ... IF NOT EXISTS` does not modify existing objects. Either start from a fresh volume (`docker compose down -v`) or drop the `*_kafka` tables and `*_mv` views, add the column (`ALTER TABLE smartcity.<table> ADD COLUMN IF NOT EXISTS ingested_at DateTime DEFAULT now()`) and re-run `scripts/init-clickhouse.ps1`.

## Airflow DAGs

| DAG                      | Schedule | What it does                                        |
|--------------------------|----------|-----------------------------------------------------|
| smartcity_health_check   | hourly   | pings ClickHouse, Trino, Kafka exporter, Prometheus |
| smartcity_data_quality   | hourly   | freshness, dead-letter, duplicate and range checks  |
| smartcity_daily_summary  | daily    | rolls up yesterday's vehicle stats in ClickHouse    |

`smartcity_data_quality` is expected to fail its freshness check when the producer has not run in the last hour.

Trigger them manually from the Airflow UI after the stack is running.

**Note on `smartcity_daily_summary`:** it aggregates rows where `toDate(timestamp) = yesterday()`. On the first day (or if you only ran the producer today), the INSERT may produce zero rows. That is expected : wait until the next calendar day, or adjust the DAG query for testing.

## Project layout

```
project/
├── main.py                     # producer entry point (delegates to jobs/main.py)
├── requirements.txt            # host-side producer deps only
├── docker-compose.yml
├── jobs/
│   ├── main.py                 # Kafka data simulator
│   ├── config.py               # MinIO settings for Spark jobs
│   ├── spark-city.py           # Spark structured streaming job
│   └── verify_pipeline.py      # smoke test for the lake path
├── clickhouse/
│   ├── init/                   # ClickHouse DDL (Kafka engine + MergeTree)
│   └── config.d/               # Prometheus metrics endpoint
├── airflow/dags/               # Airflow DAGs
├── monitoring/                 # Prometheus + Grafana config
├── scripts/                    # setup and helper scripts
│   ├── setup.ps1               # full Windows bootstrap
│   ├── download-hive-jars.ps1  # fetch AWS SDK bundle for Hive
│   ├── download-hive-jars.sh   # same, for Linux/macOS
│   ├── init-clickhouse.ps1     # apply ClickHouse schema
│   ├── run-spark.ps1           # submit Spark streaming job
│   ├── stop-spark.ps1          # stop running Spark apps
│   ├── reset-checkpoints.ps1   # clear Spark checkpoints in MinIO
│   └── run-producer.ps1        # Docker-based producer (Windows workaround)
├── hive-jars/                  # Hive JDBC/S3 jars (AWS bundle downloaded at setup)
├── hive-conf/                  # Hive Metastore S3/MinIO settings
├── trino-catalog/              # Trino Hive connector config
├── spark-conf/                 # Spark defaults (JAR packages)
└── .github/workflows/ci.yml
```

## Restarting Spark cleanly

If a streaming job was killed mid-run, clear checkpoints before resubmitting:

**Windows:**

```powershell
.\scripts\stop-spark.ps1         # stop running Spark apps
.\scripts\reset-checkpoints.ps1  # clear checkpoints
.\scripts\run-spark.ps1          # submit a fresh job
```

**Linux / macOS:**

```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc rm --recursive --force local/spark-streaming-data/checkpoints/
docker exec -u root -e IVY_HOME=/tmp/.ivy project-spark-master-1 \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/spark-city.py
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| ClickHouse tables empty | Apply schema first (`init-clickhouse`), then run the producer. ClickHouse only ingests messages produced after its Kafka tables exist. |
| `confluent_kafka` DLL blocked (Windows) | Use `.\scripts\run-producer.ps1` instead of `python main.py`. |
| Spark job fails after crash | Run restart steps above to clear stale checkpoints. |
| Trino returns 0 rows | Confirm Spark job is running and the producer has been sending data for at least one micro-batch (~10 s). |
| Container name `project-spark-master-1` not found | Docker Compose names containers `{folder}_{service}_{instance}`. If you renamed the project folder, replace `project` in commands with your folder name. Also update the network name in `scripts/run-producer.ps1` (defaults to `project_datamasterylab`). |

## CI

Pushing to `main` or `master` triggers a GitHub Actions workflow that starts the Docker stack and runs smoke tests against Kafka, ClickHouse, Trino, and Prometheus. Airflow is not part of CI to keep runs fast.

## Notes

- All credentials are hardcoded for local dev. Do not expose this stack to the internet as-is.
- MinIO replaces S3, Hive Metastore replaces Glue, Docker Kafka replaces MSK. The design mirrors a production AWS layout without the cost.
- Kafka topics and the MinIO bucket are created on first `docker compose up`; ClickHouse schema must still be applied manually.
