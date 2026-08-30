from pyspark.sql import SparkSession
from config import MINIO_CONFIG

spark = (
    SparkSession.builder.appName("PipelineVerify")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG.get("access_key"))
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG.get("secret_key"))
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG.get("endpoint"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=== Hive tables in smartcity ===")
spark.sql("SHOW TABLES IN smartcity").show(truncate=False)

for table in [
    "vehicle_data",
    "gps_data",
    "traffic_data",
    "weather_data",
    "emergency_data",
]:
    count = spark.sql(f"SELECT COUNT(*) AS rows FROM smartcity.{table}").collect()[0][0]
    print(f"{table}: {count} rows")

print("=== Sample vehicle rows ===")
spark.sql("SELECT id, deviceId, timestamp, location, speed, make FROM smartcity.vehicle_data LIMIT 3").show(
    truncate=False
)

spark.stop()
