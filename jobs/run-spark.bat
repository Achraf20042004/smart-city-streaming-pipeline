@echo off
docker exec -it project-spark-master-1 /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/spark-city.py
