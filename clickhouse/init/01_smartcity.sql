CREATE DATABASE IF NOT EXISTS smartcity;

-- ── vehicle_data ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS smartcity.vehicle_data_kafka
(
    id String,
    deviceId String,
    timestamp String,
    location String,
    speed Float64,
    direction String,
    make String,
    model String,
    year Int32,
    fuelType String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:29092',
    kafka_topic_list = 'vehicle_data',
    kafka_group_name = 'clickhouse_vehicle',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS smartcity.vehicle_data
(
    id String,
    deviceId String,
    timestamp DateTime64(3),
    location String,
    speed Float64,
    direction String,
    make String,
    model String,
    year Int32,
    fuelType String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (deviceId, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.vehicle_data_mv
TO smartcity.vehicle_data AS
SELECT
    id,
    deviceId,
    parseDateTime64BestEffortOrZero(timestamp) AS timestamp,
    location,
    speed,
    direction,
    make,
    model,
    year,
    fuelType
FROM smartcity.vehicle_data_kafka
WHERE length(_error) = 0
  AND id != ''
  AND deviceId != ''
  AND parseDateTime64BestEffortOrNull(vehicle_data_kafka.timestamp) IS NOT NULL;

-- ── gps_data ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS smartcity.gps_data_kafka
(
    id String,
    deviceId String,
    timestamp String,
    location String,
    speed Float64,
    direction String,
    vehicleType String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:29092',
    kafka_topic_list = 'gps_data',
    kafka_group_name = 'clickhouse_gps',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS smartcity.gps_data
(
    id String,
    deviceId String,
    timestamp DateTime64(3),
    location String,
    speed Float64,
    direction String,
    vehicleType String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (deviceId, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.gps_data_mv
TO smartcity.gps_data AS
SELECT
    id,
    deviceId,
    parseDateTime64BestEffortOrZero(timestamp) AS timestamp,
    location,
    speed,
    direction,
    vehicleType
FROM smartcity.gps_data_kafka
WHERE length(_error) = 0
  AND id != ''
  AND deviceId != ''
  AND parseDateTime64BestEffortOrNull(gps_data_kafka.timestamp) IS NOT NULL;

-- ── traffic_data ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS smartcity.traffic_data_kafka
(
    id String,
    deviceId String,
    cameraId String,
    location String,
    timestamp String,
    snapshot String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:29092',
    kafka_topic_list = 'traffic_data',
    kafka_group_name = 'clickhouse_traffic',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS smartcity.traffic_data
(
    id String,
    deviceId String,
    cameraId String,
    location String,
    timestamp DateTime64(3),
    snapshot String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (deviceId, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.traffic_data_mv
TO smartcity.traffic_data AS
SELECT
    id,
    deviceId,
    cameraId,
    location,
    parseDateTime64BestEffortOrZero(timestamp) AS timestamp,
    snapshot
FROM smartcity.traffic_data_kafka
WHERE length(_error) = 0
  AND id != ''
  AND deviceId != ''
  AND parseDateTime64BestEffortOrNull(traffic_data_kafka.timestamp) IS NOT NULL;

-- ── weather_data ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS smartcity.weather_data_kafka
(
    id String,
    deviceId String,
    location String,
    timestamp String,
    temperature Float64,
    weatherCondition String,
    precipitation Float64,
    windspeed Float64,
    humidity Int32,
    airQualityIndex Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:29092',
    kafka_topic_list = 'weather_data',
    kafka_group_name = 'clickhouse_weather',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS smartcity.weather_data
(
    id String,
    deviceId String,
    location String,
    timestamp DateTime64(3),
    temperature Float64,
    weatherCondition String,
    precipitation Float64,
    windspeed Float64,
    humidity Int32,
    airQualityIndex Float64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (deviceId, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.weather_data_mv
TO smartcity.weather_data AS
SELECT
    id,
    deviceId,
    location,
    parseDateTime64BestEffortOrZero(timestamp) AS timestamp,
    temperature,
    weatherCondition,
    precipitation,
    windspeed,
    humidity,
    airQualityIndex
FROM smartcity.weather_data_kafka
WHERE length(_error) = 0
  AND id != ''
  AND deviceId != ''
  AND parseDateTime64BestEffortOrNull(weather_data_kafka.timestamp) IS NOT NULL;

-- ── emergency_data ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS smartcity.emergency_data_kafka
(
    id String,
    deviceId String,
    incidentId String,
    type String,
    timestamp String,
    location String,
    status String,
    description String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:29092',
    kafka_topic_list = 'emergency_data',
    kafka_group_name = 'clickhouse_emergency',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS smartcity.emergency_data
(
    id String,
    deviceId String,
    incidentId String,
    type String,
    timestamp DateTime64(3),
    location String,
    status String,
    description String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (deviceId, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.emergency_data_mv
TO smartcity.emergency_data AS
SELECT
    id,
    deviceId,
    incidentId,
    type,
    parseDateTime64BestEffortOrZero(timestamp) AS timestamp,
    location,
    status,
    description
FROM smartcity.emergency_data_kafka
WHERE length(_error) = 0
  AND id != ''
  AND deviceId != ''
  AND parseDateTime64BestEffortOrNull(emergency_data_kafka.timestamp) IS NOT NULL;
