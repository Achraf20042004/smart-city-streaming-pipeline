CREATE TABLE IF NOT EXISTS smartcity.daily_vehicle_summary
(
    date Date,
    device_id String,
    avg_speed Float64,
    max_speed Float64,
    reading_count UInt64
)
ENGINE = MergeTree
ORDER BY (date, device_id);
