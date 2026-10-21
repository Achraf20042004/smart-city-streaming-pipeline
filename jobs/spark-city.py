import os
import sys
from functools import reduce
from pyspark.sql import Column, DataFrame, SparkSession
from config import MINIO_CONFIG
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.functions import avg, col, count, from_json, lit, when, window
from pyspark.sql.functions import max as spark_max

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,"
    "org.apache.hadoop:hadoop-aws:3.4.1,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

CHECKPOINT_BASE = "s3a://spark-streaming-data/checkpoints"
DATA_BASE = "s3a://spark-streaming-data/data"
CORRUPT_RECORD = "_corrupt_record"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("SmartCityStreaming")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONFIG["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONFIG["secret_key"])
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONFIG["endpoint"])
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


def read_kafka_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            col("topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_value"),
        )
        .withColumn(
            "data",
            from_json(
                col("raw_value"),
                StructType(schema.fields + [StructField(CORRUPT_RECORD, StringType(), True)]),
                {"columnNameOfCorruptRecord": CORRUPT_RECORD},
            ),
        )
    )


def has_required_fields() -> Column:
    return (
        col("data.id").isNotNull()
        & (col("data.id") != "")
        & col("data.deviceId").isNotNull()
        & (col("data.deviceId") != "")
        & col("data.timestamp").isNotNull()
    )


def is_valid_event() -> Column:
    # Same rule as the ClickHouse materialized views: every event needs an id,
    # a deviceId and a parseable timestamp, and no field may have a wrong type
    # (from_json would otherwise just turn that field into NULL).
    return has_required_fields() & col(f"data.{CORRUPT_RECORD}").isNull()


def valid_events(parsed_df: DataFrame) -> DataFrame:
    return (
        parsed_df.filter(is_valid_event())
        .select("data.*")
        .drop(CORRUPT_RECORD)
        .withWatermark("timestamp", "2 minutes")
    )


def rejected_events(parsed_df: DataFrame) -> DataFrame:
    unparseable = (
        col("data.id").isNull()
        & col("data.deviceId").isNull()
        & col("data.timestamp").isNull()
    )
    return parsed_df.filter(~is_valid_event()).select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        when(unparseable, lit("unparseable message"))
        .when(~has_required_fields(), lit("missing id/deviceId or invalid timestamp"))
        .otherwise(lit("invalid field type"))
        .alias("reason"),
        "raw_value",
    )


def stream_writer(input_df: DataFrame, checkpoint_folder: str, output: str):
    return (
        input_df.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_folder)
        .option("path", output)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


def vehicle_speed_per_minute(vehicle_df: DataFrame) -> DataFrame:
    # Relies on the 2-minute watermark: a window is emitted once, after it can
    # no longer receive late events, and its state is then dropped.
    return (
        vehicle_df.groupBy(window(col("timestamp"), "1 minute"), col("deviceId"))
        .agg(
            avg("speed").alias("avg_speed"),
            spark_max("speed").alias("max_speed"),
            count("*").alias("readings"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "deviceId",
            "avg_speed",
            "max_speed",
            "readings",
        )
    )


def create_hive_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS smartcity")

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.vehicle_data (
            id STRING, deviceId STRING, timestamp TIMESTAMP, location STRING,
            speed DOUBLE, make STRING, model STRING,
            year INT, fuelType STRING, vehicleType STRING
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/vehicle_data'
        """
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.gps_data (
            id STRING, deviceId STRING, timestamp TIMESTAMP, location STRING,
            speed DOUBLE, direction STRING
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/gps_data'
        """
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.traffic_data (
            id STRING, deviceId STRING, cameraId STRING, location STRING,
            timestamp TIMESTAMP, snapshot STRING
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/traffic_data'
        """
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.weather_data (
            id STRING, deviceId STRING, timestamp TIMESTAMP, location STRING,
            temperature DOUBLE, weatherCondition STRING, precipitation DOUBLE,
            windspeed DOUBLE, humidity INT, airQualityIndex DOUBLE
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/weather_data'
        """
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.emergency_data (
            id STRING, deviceId STRING, incidentId STRING, type STRING,
            timestamp TIMESTAMP, location STRING, status STRING, description STRING
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/emergency_data'
        """
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.vehicle_speed_1min (
            window_start TIMESTAMP, window_end TIMESTAMP, deviceId STRING,
            avg_speed DOUBLE, max_speed DOUBLE, readings BIGINT
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/vehicle_speed_1min'
        """
    )

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS smartcity.rejected_events (
            topic STRING, kafka_partition INT, kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP, reason STRING, raw_value STRING
        )
        USING PARQUET
        LOCATION '{DATA_BASE}/rejected_events'
        """
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    vehicle_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("fuelType", StringType(), True),
            StructField("vehicleType", StringType(), True),
        ]
    )

    gps_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
        ]
    )

    traffic_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("cameraId", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("snapshot", StringType(), True),
        ]
    )

    weather_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("weatherCondition", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("airQualityIndex", DoubleType(), True),
        ]
    )

    emergency_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("incidentId", StringType(), True),
            StructField("type", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("status", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )

    create_hive_tables(spark)

    topics = {
        "vehicle_data": vehicle_schema,
        "gps_data": gps_schema,
        "traffic_data": traffic_schema,
        "weather_data": weather_schema,
        "emergency_data": emergency_schema,
    }
    parsed = {
        topic: read_kafka_topic(spark, topic, schema) for topic, schema in topics.items()
    }

    for topic, parsed_df in parsed.items():
        stream_writer(
            valid_events(parsed_df),
            f"{CHECKPOINT_BASE}/{topic}",
            f"{DATA_BASE}/{topic}",
        )

    stream_writer(
        vehicle_speed_per_minute(valid_events(parsed["vehicle_data"])),
        f"{CHECKPOINT_BASE}/vehicle_speed_1min",
        f"{DATA_BASE}/vehicle_speed_1min",
    )

    # Invalid events from every topic go to a single quarantine table
    # instead of being dropped silently.
    stream_writer(
        reduce(DataFrame.unionByName, [rejected_events(df) for df in parsed.values()]),
        f"{CHECKPOINT_BASE}/rejected_events",
        f"{DATA_BASE}/rejected_events",
    )

    print("Smart City streaming job started. Waiting for data...", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
