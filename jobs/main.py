import os
import sys
import uuid
from confluent_kafka import Producer
import simplejson as json
from datetime import datetime, timedelta
import random
import time

# Ensure delivery logs appear immediately when run as `python main.py`
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

LONDON_COORDINATES= {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES= {"latitude": 52.4862, "longitude": -1.8904}

NUM_STEPS = 100
#Calculate movement increments
LATITUDE_INCREMENT= (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"])/NUM_STEPS
LONGITUDE_INCREMENT= (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"])/NUM_STEPS


#environment Variables for configuration , ces lignes ne cree aucun topic
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
#KAFKA_BOOTSTRAP_SERVERS
#→ variable Python dans laquelle on va stocker l’adresse de Kafka.
#os.getenv(...)
#→ cherche une variable d’environnement dans le système.
#'KAFKA_BOOTSTRAP_SERVERS'
#→ nom de la variable d’environnement recherchée.
#'localhost:9092'
#→ valeur par défaut si cette variable d’environnement n’existe pas
VEHICLE_TOPIC=os.getenv('VEHICLE_TOPIC','vehicle_data') #dans mon programme Python, le topic véhicule s’appellera vehicle_data par défaut.
#Le topic vehicle_data contient la vitesse (speed) et la localisation (location) 
GPS_TOPIC=os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC','weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC','emergency_data')



start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

SPEED_MIN_KMH = 0
SPEED_MAX_KMH = 140
COORD_JITTER_DEG = 0.0005
#cela veut dire +ou- 0.0005 sur la longitude ou l attitude a chaque pas (qui simule le trajet)


def new_device_id():
    return f"Vehicle-{random.randint(1, 999):03d}"

#d) est écrit sur 3 chiffres minimum, complété par des zéros à gauche (0).


def new_camera_id():
    return f"Camera-{random.randint(1, 999):03d}"


def apply_speed_jitter(speed):
    jittered = speed * (1 + random.uniform(-0.05, 0.05))
    return round(max(SPEED_MIN_KMH, min(SPEED_MAX_KMH, jittered)), 2)


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60))
    return start_time



def format_location(location):
    if isinstance(location, str):
        return location
    if isinstance(location, tuple):
        return f"{location[0]},{location[1]}"
    return f"{location['latitude']},{location['longitude']}"


def generate_gps_data(device_id, timestamp, location, vehicle_speed):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': format_location(location),
        'speed': apply_speed_jitter(vehicle_speed),
        'direction': random.choice(['North', 'North-East', 'East', 'South-East', 'South'])
    }









def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id, #identifiant de la voiture (plus précisément, du boîtier connecté installé dedans
        'cameraId': camera_id,
        'location': format_location(location),
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': format_location(location),
        'timestamp': timestamp,
        'temperature': random.uniform(-5,26),
        'weatherCondition': random.choice(['Sunny','Cloudy','Rain','Snow']),
        'precipitation': random.uniform(0,25),
        'windspeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'airQualityIndex': random.uniform(0,500)
    }




def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': format_location(location),
        'status':random.choice(['Active','Resolved']),
        'description': 'Description of the incident'
    }






def simulate_vehicle_movement():
    global start_location

    #move towards birmongham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    start_location['latitude'] += random.uniform(-COORD_JITTER_DEG, COORD_JITTER_DEG)
    start_location['longitude'] += random.uniform(-COORD_JITTER_DEG, COORD_JITTER_DEG)
    return start_location

def generate_vehicle_data(device_id, current_speed):
    location = simulate_vehicle_movement()
    speed = apply_speed_jitter(current_speed)
    return {
        'id' : uuid.uuid4(),
        'deviceId' : device_id,
        'timestamp': get_next_time().isoformat(),
        'location': format_location(location),
        'speed' : speed,
        'make' : 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType':'Hybrid',
        'vehicleType': 'private'
    }




def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')



def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed :{err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()





def simulate_journey(producer, device_id):
    current_speed = random.uniform(20, 60)
    camera_id = new_camera_id()
    while True:
        vehicle_data = generate_vehicle_data(device_id, current_speed)
        current_speed = vehicle_data['speed']
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], current_speed)
        traffic_camera_data = generate_traffic_camera_data(
            device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id
        )
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],vehicle_data['location'])

        if (start_location['latitude'] >= BIRMINGHAM_COORDINATES['latitude']
            and start_location['longitude'] <= BIRMINGHAM_COORDINATES['longitude']):
            print("Vehicle has reached Birmingha. Simulation ending ...")
            break


        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)
        





if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers' : KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}') 
    }
    producer = Producer(producer_config)

    try:
        simulate_journey(producer, new_device_id())

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occurred:{e}')