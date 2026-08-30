# Submit the Spark streaming job (matches the manual docker exec command).
# Run scripts/reset-checkpoints.ps1 first if a previous streaming job was killed abruptly.
docker exec -u root -e IVY_HOME=/tmp/.ivy project-spark-master-1 `
  /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/jobs/spark-city.py
