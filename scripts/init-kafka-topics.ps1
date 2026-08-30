$topics = @("vehicle_data", "gps_data", "traffic_data", "weather_data", "emergency_data")

foreach ($topic in $topics) {
    docker exec broker kafka-topics `
        --bootstrap-server localhost:9092 `
        --create --if-not-exists `
        --topic $topic `
        --partitions 1 `
        --replication-factor 1
}

docker exec broker kafka-topics --bootstrap-server localhost:9092 --list
