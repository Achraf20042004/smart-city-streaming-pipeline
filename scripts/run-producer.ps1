# Run the Kafka producer inside Docker (works around Windows App Control blocking confluent_kafka DLLs)
docker run --rm --network project_datamasterylab `
  -v "${PWD}/jobs:/app" `
  -w /app `
  -e KAFKA_BOOTSTRAP_SERVERS=broker:29092 `
  python:3.11-slim `
  sh -c "pip install -q confluent-kafka==2.15.0 simplejson==4.1.1 && python main.py"
