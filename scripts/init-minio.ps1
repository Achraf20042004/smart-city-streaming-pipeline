# Create the MinIO bucket used by Spark streaming checkpoints and parquet output.
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>$null
docker exec minio mc mb --ignore-existing local/spark-streaming-data
docker exec minio mc ls local/
