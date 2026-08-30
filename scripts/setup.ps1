# Full local setup: start stack, create Kafka topics, create MinIO bucket.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

Set-Location $root

Write-Host "Ensuring Hive Metastore jars are present..."
& "$PSScriptRoot\download-hive-jars.ps1"

Write-Host "Starting Docker Compose stack..."
docker compose up -d

Write-Host "Waiting for Zookeeper..."
do { Start-Sleep -Seconds 2 } until ((docker inspect zookeeper --format '{{.State.Health.Status}}' 2>$null) -eq 'healthy')

Write-Host "Waiting for Kafka broker..."
do { Start-Sleep -Seconds 3 } until (docker exec broker kafka-broker-api-versions --bootstrap-server localhost:9092 2>$null)

Write-Host "Creating Kafka topics..."
& "$PSScriptRoot\init-kafka-topics.ps1"

Write-Host "Creating MinIO bucket..."
& "$PSScriptRoot\init-minio.ps1"

Write-Host "Waiting for ClickHouse..."
do { Start-Sleep -Seconds 2 } until ((docker inspect clickhouse --format '{{.State.Health.Status}}' 2>$null) -eq 'healthy')

Write-Host "Applying ClickHouse schema..."
& "$PSScriptRoot\init-clickhouse.ps1"

Write-Host ""
Write-Host "Setup complete."
Write-Host "  1. Start Spark streaming:  .\scripts\run-spark.ps1"
Write-Host "  2. Run Kafka producer:     python main.py"
Write-Host "  3. Verify pipeline:        docker exec project-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/spark/jobs/verify_pipeline.py"
Write-Host "  4. Query ClickHouse:       http://localhost:8123/play  (or .\scripts\init-clickhouse.ps1 if tables missing)"
Write-Host "  5. Grafana dashboards:     http://localhost:3000  (admin / admin)"
Write-Host "  6. Prometheus metrics:   http://localhost:9091"
Write-Host "  7. Airflow orchestration:  http://localhost:8085  (admin / admin)"
