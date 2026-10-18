# Developed Trino analytics queries for dashboard activity (MinIO reads + lake analytics)
$queries = @(
@'
WITH vehicle_stats AS (
  SELECT
    deviceId,
    make,
    model,
    fuelType,
    COUNT(*) AS readings,
    ROUND(AVG(speed), 2) AS avg_speed,
    ROUND(MAX(speed), 2) AS max_speed,
    ROUND(MIN(speed), 2) AS min_speed,
    ROUND(STDDEV(speed), 2) AS speed_stddev
  FROM hive.smartcity.vehicle_data
  GROUP BY deviceId, make, model, fuelType
)
SELECT *
FROM vehicle_stats
ORDER BY avg_speed DESC
'@,

@'
SELECT
  make,
  model,
  fuelType,
  COUNT(*) AS trip_events,
  ROUND(AVG(speed), 2) AS avg_speed,
  ROUND(APPROX_PERCENTILE(speed, 0.5), 2) AS median_speed,
  ROUND(APPROX_PERCENTILE(speed, 0.95), 2) AS p95_speed
FROM hive.smartcity.vehicle_data
GROUP BY make, model, fuelType
ORDER BY trip_events DESC
'@,

@'
SELECT
  DATE_TRUNC('minute', CAST(timestamp AS timestamp)) AS minute_bucket,
  COUNT(*) AS vehicle_events,
  ROUND(AVG(speed), 2) AS avg_speed,
  ROUND(MAX(speed), 2) AS peak_speed
FROM hive.smartcity.vehicle_data
GROUP BY 1
ORDER BY 1
'@,

@'
WITH ranked AS (
  SELECT
    deviceId,
    CAST(timestamp AS timestamp) AS ts,
    speed,
    location,
    ROW_NUMBER() OVER (PARTITION BY deviceId ORDER BY speed DESC) AS speed_rank
  FROM hive.smartcity.vehicle_data
)
SELECT deviceId, ts, speed, location
FROM ranked
WHERE speed_rank <= 5
ORDER BY deviceId, speed_rank
'@,

@'
SELECT
  vehicleType,
  COUNT(*) AS gps_points,
  ROUND(AVG(speed), 2) AS avg_gps_speed,
  ROUND(MAX(speed), 2) AS max_gps_speed,
  ROUND(STDDEV(speed), 2) AS speed_volatility
FROM hive.smartcity.gps_data
GROUP BY vehicleType
ORDER BY gps_points DESC
'@,

@'
SELECT
  deviceId,
  COUNT(*) AS gps_readings,
  ROUND(AVG(speed), 2) AS avg_speed,
  COUNT(DISTINCT direction) AS direction_changes
FROM hive.smartcity.gps_data
GROUP BY deviceId
ORDER BY gps_readings DESC
'@,

@'
SELECT
  cameraId,
  COUNT(*) AS snapshots,
  COUNT(DISTINCT deviceId) AS vehicles_seen,
  COUNT(DISTINCT location) AS locations
FROM hive.smartcity.traffic_data
GROUP BY cameraId
ORDER BY snapshots DESC
'@,

@'
SELECT
  deviceId,
  cameraId,
  location,
  COUNT(*) AS captures,
  MIN(CAST(timestamp AS timestamp)) AS first_seen,
  MAX(CAST(timestamp AS timestamp)) AS last_seen
FROM hive.smartcity.traffic_data
GROUP BY deviceId, cameraId, location
ORDER BY captures DESC
LIMIT 25
'@,

@'
SELECT
  weatherCondition,
  COUNT(*) AS observations,
  ROUND(AVG(temperature), 1) AS avg_temp_c,
  ROUND(AVG(precipitation), 2) AS avg_precip_mm,
  ROUND(AVG(windspeed), 2) AS avg_wind,
  ROUND(AVG(humidity), 0) AS avg_humidity_pct,
  ROUND(AVG(airQualityIndex), 1) AS avg_aqi
FROM hive.smartcity.weather_data
GROUP BY weatherCondition
ORDER BY observations DESC
'@,

@'
SELECT
  deviceId,
  ROUND(AVG(temperature), 1) AS avg_temp,
  ROUND(MAX(temperature), 1) AS max_temp,
  ROUND(MIN(temperature), 1) AS min_temp,
  ROUND(AVG(airQualityIndex), 1) AS avg_aqi,
  SUM(CASE WHEN airQualityIndex > 100 THEN 1 ELSE 0 END) AS unhealthy_aqi_readings
FROM hive.smartcity.weather_data
GROUP BY deviceId
ORDER BY avg_aqi DESC
'@,

@'
SELECT
  type AS incident_type,
  status,
  COUNT(*) AS incidents,
  COUNT(DISTINCT deviceId) AS affected_vehicles,
  COUNT(DISTINCT incidentId) AS unique_incidents
FROM hive.smartcity.emergency_data
GROUP BY type, status
ORDER BY incidents DESC
'@,

@'
SELECT
  deviceId,
  type,
  status,
  description,
  CAST(timestamp AS timestamp) AS incident_time,
  location
FROM hive.smartcity.emergency_data
ORDER BY CAST(timestamp AS timestamp) DESC
LIMIT 30
'@,

@'
SELECT
  v.deviceId,
  COUNT(*) AS matched_events,
  ROUND(AVG(v.speed), 2) AS avg_vehicle_speed,
  ROUND(AVG(g.speed), 2) AS avg_gps_speed,
  ROUND(AVG(w.temperature), 1) AS avg_temperature,
  ROUND(AVG(w.airQualityIndex), 1) AS avg_aqi
FROM hive.smartcity.vehicle_data v
JOIN hive.smartcity.gps_data g
  ON v.deviceId = g.deviceId AND v.timestamp = g.timestamp
JOIN hive.smartcity.weather_data w
  ON v.deviceId = w.deviceId AND v.timestamp = w.timestamp
GROUP BY v.deviceId
ORDER BY matched_events DESC
'@,

@'
SELECT
  v.deviceId,
  v.speed AS vehicle_speed,
  g.speed AS gps_speed,
  ABS(v.speed - g.speed) AS speed_delta,
  w.temperature,
  w.weatherCondition,
  w.airQualityIndex,
  e.type AS emergency_type,
  e.status AS emergency_status
FROM hive.smartcity.vehicle_data v
LEFT JOIN hive.smartcity.gps_data g
  ON v.deviceId = g.deviceId AND v.timestamp = g.timestamp
LEFT JOIN hive.smartcity.weather_data w
  ON v.deviceId = w.deviceId AND v.timestamp = w.timestamp
LEFT JOIN hive.smartcity.emergency_data e
  ON v.deviceId = e.deviceId AND v.timestamp = e.timestamp
WHERE v.speed > 20
ORDER BY speed_delta DESC
LIMIT 40
'@,

@'
WITH journey AS (
  SELECT
    deviceId,
    CAST(timestamp AS timestamp) AS ts,
    speed,
    location,
    LAG(speed) OVER (PARTITION BY deviceId ORDER BY CAST(timestamp AS timestamp)) AS prev_speed,
    LAG(CAST(timestamp AS timestamp)) OVER (PARTITION BY deviceId ORDER BY CAST(timestamp AS timestamp)) AS prev_ts
  FROM hive.smartcity.vehicle_data
)
SELECT
  deviceId,
  ts,
  speed,
  prev_speed,
  ROUND(speed - prev_speed, 2) AS acceleration,
  location
FROM journey
WHERE prev_speed IS NOT NULL
ORDER BY ABS(speed - prev_speed) DESC
LIMIT 30
'@,

@'
SELECT
  fuelType,
  COUNT(*) AS events,
  ROUND(AVG(speed), 2) AS avg_speed,
  ROUND(AVG(year), 0) AS avg_vehicle_year,
  COUNT(DISTINCT make) AS makes,
  COUNT(DISTINCT model) AS models
FROM hive.smartcity.vehicle_data
GROUP BY fuelType
ORDER BY events DESC
'@,

@'
SELECT
  DATE_TRUNC('hour', CAST(timestamp AS timestamp)) AS hour_bucket,
  COUNT(*) AS emergency_count,
  COUNT(DISTINCT type) AS incident_types,
  COUNT(DISTINCT deviceId) AS vehicles_involved
FROM hive.smartcity.emergency_data
GROUP BY 1
ORDER BY 1
'@,

@'
SELECT
  t.deviceId,
  t.cameraId,
  v.speed,
  v.make,
  v.model,
  w.temperature,
  w.precipitation,
  CAST(t.timestamp AS timestamp) AS capture_time
FROM hive.smartcity.traffic_data t
JOIN hive.smartcity.vehicle_data v
  ON t.deviceId = v.deviceId AND t.timestamp = v.timestamp
JOIN hive.smartcity.weather_data w
  ON t.deviceId = w.deviceId AND t.timestamp = w.timestamp
ORDER BY v.speed DESC
LIMIT 35
'@,

@'
SELECT
  'vehicle_data' AS dataset, COUNT(*) AS rows, COUNT(DISTINCT deviceId) AS devices FROM hive.smartcity.vehicle_data
UNION ALL
SELECT 'gps_data', COUNT(*), COUNT(DISTINCT deviceId) FROM hive.smartcity.gps_data
UNION ALL
SELECT 'traffic_data', COUNT(*), COUNT(DISTINCT deviceId) FROM hive.smartcity.traffic_data
UNION ALL
SELECT 'weather_data', COUNT(*), COUNT(DISTINCT deviceId) FROM hive.smartcity.weather_data
UNION ALL
SELECT 'emergency_data', COUNT(*), COUNT(DISTINCT deviceId) FROM hive.smartcity.emergency_data
ORDER BY rows DESC
'@,

@'
SELECT
  deviceId,
  direction,
  COUNT(*) AS heading_count,
  ROUND(AVG(speed), 2) AS avg_speed_on_heading
FROM hive.smartcity.vehicle_data
GROUP BY deviceId, direction
ORDER BY heading_count DESC
'@,

@'
SELECT
  window_start,
  deviceId,
  ROUND(avg_speed, 2) AS avg_speed,
  ROUND(max_speed, 2) AS max_speed,
  readings
FROM hive.smartcity.vehicle_speed_1min
ORDER BY window_start DESC
LIMIT 20
'@
)

$i = 0
$ok = 0
$fail = 0
foreach ($q in $queries) {
  $i++
  Write-Host "`n========== Query $i / $($queries.Count) =========="
  $result = docker exec trino trino --execute $q 2>&1
  if ($LASTEXITCODE -eq 0) {
    $ok++
    $result | Select-Object -First 8
    if (($result | Measure-Object -Line).Lines -gt 8) { Write-Host "... ($(( $result | Measure-Object -Line).Lines)) lines total)" }
  } else {
    $fail++
    Write-Host "FAILED:" -ForegroundColor Red
    $result | Select-Object -First 5
  }
}
Write-Host "`nFinished: $ok succeeded, $fail failed out of $i queries"
