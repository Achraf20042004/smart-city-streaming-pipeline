-- Dead-letter table: every Kafka message rejected by the materialized views in
-- 01_smartcity.sql lands here instead of being skipped silently.
--   * parse errors (invalid JSON, wrong types) come from kafka_handle_error_mode = 'stream'
--   * rule violations: empty id / deviceId or unparseable timestamp
CREATE TABLE IF NOT EXISTS smartcity.kafka_errors
(
    received_at DateTime DEFAULT now(),
    topic String,
    partition UInt64,
    offset UInt64,
    error String,
    raw_message String
)
ENGINE = MergeTree
ORDER BY (topic, received_at)
TTL received_at + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.vehicle_data_errors_mv
TO smartcity.kafka_errors AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    if(length(_error) > 0, _error, 'missing id/deviceId or invalid timestamp') AS error,
    if(length(_error) > 0, _raw_message,
       toJSONString(map('id', id, 'deviceId', deviceId, 'timestamp', timestamp))) AS raw_message
FROM smartcity.vehicle_data_kafka
WHERE length(_error) > 0
   OR id = ''
   OR deviceId = ''
   OR parseDateTime64BestEffortOrNull(timestamp) IS NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.gps_data_errors_mv
TO smartcity.kafka_errors AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    if(length(_error) > 0, _error, 'missing id/deviceId or invalid timestamp') AS error,
    if(length(_error) > 0, _raw_message,
       toJSONString(map('id', id, 'deviceId', deviceId, 'timestamp', timestamp))) AS raw_message
FROM smartcity.gps_data_kafka
WHERE length(_error) > 0
   OR id = ''
   OR deviceId = ''
   OR parseDateTime64BestEffortOrNull(timestamp) IS NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.traffic_data_errors_mv
TO smartcity.kafka_errors AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    if(length(_error) > 0, _error, 'missing id/deviceId or invalid timestamp') AS error,
    if(length(_error) > 0, _raw_message,
       toJSONString(map('id', id, 'deviceId', deviceId, 'timestamp', timestamp))) AS raw_message
FROM smartcity.traffic_data_kafka
WHERE length(_error) > 0
   OR id = ''
   OR deviceId = ''
   OR parseDateTime64BestEffortOrNull(timestamp) IS NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.weather_data_errors_mv
TO smartcity.kafka_errors AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    if(length(_error) > 0, _error, 'missing id/deviceId or invalid timestamp') AS error,
    if(length(_error) > 0, _raw_message,
       toJSONString(map('id', id, 'deviceId', deviceId, 'timestamp', timestamp))) AS raw_message
FROM smartcity.weather_data_kafka
WHERE length(_error) > 0
   OR id = ''
   OR deviceId = ''
   OR parseDateTime64BestEffortOrNull(timestamp) IS NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS smartcity.emergency_data_errors_mv
TO smartcity.kafka_errors AS
SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    if(length(_error) > 0, _error, 'missing id/deviceId or invalid timestamp') AS error,
    if(length(_error) > 0, _raw_message,
       toJSONString(map('id', id, 'deviceId', deviceId, 'timestamp', timestamp))) AS raw_message
FROM smartcity.emergency_data_kafka
WHERE length(_error) > 0
   OR id = ''
   OR deviceId = ''
   OR parseDateTime64BestEffortOrNull(timestamp) IS NULL;
