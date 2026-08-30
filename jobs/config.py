MINIO_CONFIG = {
    "endpoint": "http://minio:9000",       # nom du service Docker (pas localhost, si Spark tourne dans un conteneur)
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "spark-streaming-data",
}