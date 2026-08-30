# Remove Spark streaming checkpoints so a new job can start cleanly.
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>$null
docker exec minio mc rm --recursive --force local/spark-streaming-data/checkpoints/ 2>$null
Write-Host "Checkpoints cleared."
