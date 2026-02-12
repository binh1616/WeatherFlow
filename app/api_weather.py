import requests
import time
import json
from datetime import datetime
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor, as_completed

# ────────────────────────────────────────────────
# CẤU HÌNH
# ────────────────────────────────────────────────
HEADERS = {
    'User-Agent': 'WeatherToKafka (your.real.email@example.com)',  # THAY EMAIL THẬT
    'Accept': 'application/geo+json'
}

# Top ~100 thành phố lớn nhất Mỹ (dân số descending, tọa độ trung tâm)
# Dữ liệu từ Census/Wikipedia/GeoNames/simplemaps ~2024-2025
LOCATIONS = {
    "new_york": {"lat": 40.7128, "lon": -74.0060},
    "los_angeles": {"lat": 34.0522, "lon": -118.2437},
    "chicago": {"lat": 41.8781, "lon": -87.6298},
    "houston": {"lat": 29.7604, "lon": -95.3698},
    "phoenix": {"lat": 33.4484, "lon": -112.0740},
    "philadelphia": {"lat": 39.9526, "lon": -75.1652},
    "san_antonio": {"lat": 29.4241, "lon": -98.4936},
    "san_diego": {"lat": 32.7157, "lon": -117.1611},
    "dallas": {"lat": 32.7767, "lon": -96.7970},
    "san_jose": {"lat": 37.3382, "lon": -121.8863},
    "austin": {"lat": 30.2672, "lon": -97.7431},
    "jacksonville": {"lat": 30.3322, "lon": -81.6557},
    "fort_worth": {"lat": 32.7555, "lon": -97.3308},
    "columbus": {"lat": 39.9612, "lon": -82.9988},
    "charlotte": {"lat": 35.2271, "lon": -80.8431},
    "indianapolis": {"lat": 39.7684, "lon": -86.1581},
    "san_francisco": {"lat": 37.7749, "lon": -122.4194},
    "seattle": {"lat": 47.6062, "lon": -122.3321},
    "denver": {"lat": 39.7392, "lon": -104.9903},
    "oklahoma_city": {"lat": 35.4676, "lon": -97.5164},
    "nashville": {"lat": 36.1627, "lon": -86.7816},
    "el_paso": {"lat": 31.7619, "lon": -106.4850},
    "washington": {"lat": 38.9072, "lon": -77.0369},
    "boston": {"lat": 42.3601, "lon": -71.0589},
    "las_vegas": {"lat": 36.1699, "lon": -115.1398},
    "portland": {"lat": 45.5152, "lon": -122.6784},
    "detroit": {"lat": 42.3314, "lon": -83.0458},
    "louisville": {"lat": 38.2527, "lon": -85.7585},
    "memphis": {"lat": 35.1495, "lon": -90.0490},
    "baltimore": {"lat": 39.2904, "lon": -76.6122},
    "milwaukee": {"lat": 43.0389, "lon": -87.9065},
    "albuquerque": {"lat": 35.0844, "lon": -106.6504},
    "tucson": {"lat": 32.2226, "lon": -110.9747},
    "fresno": {"lat": 36.7378, "lon": -119.7871},
    "sacramento": {"lat": 38.5816, "lon": -121.4944},
    "mesa": {"lat": 33.4152, "lon": -111.8315},
    "kansas_city": {"lat": 39.0997, "lon": -94.5786},
    "atlanta": {"lat": 33.7490, "lon": -84.3880},
    "omaha": {"lat": 41.2565, "lon": -95.9345},
    "colorado_springs": {"lat": 38.8339, "lon": -104.8214},
    "raleigh": {"lat": 35.7796, "lon": -78.6382},
    "miami": {"lat": 25.7617, "lon": -80.1918},
    "long_beach": {"lat": 33.7701, "lon": -118.1937},
    "virginia_beach": {"lat": 36.8508, "lon": -76.2859},
    "oakland": {"lat": 37.8044, "lon": -122.2711},
    "minneapolis": {"lat": 44.9778, "lon": -93.2650},
    "tulsa": {"lat": 36.1540, "lon": -95.9928},
    "bakersfield": {"lat": 35.3733, "lon": -119.0187},
    "wichita": {"lat": 37.6872, "lon": -97.3301},
    "arlington": {"lat": 32.7357, "lon": -97.1081},
    "new_orleans": {"lat": 29.9511, "lon": -90.0715},
    "cleveland": {"lat": 41.4993, "lon": -81.6944},
    "tampa": {"lat": 27.9506, "lon": -82.4572},
    "aurora": {"lat": 39.7294, "lon": -104.8319},
    "honolulu": {"lat": 21.3069, "lon": -157.8583},
    "anaheim": {"lat": 33.8353, "lon": -117.9145},
    "santa_ana": {"lat": 33.7456, "lon": -117.8678},
    "corpus_christi": {"lat": 27.8003, "lon": -97.3960},
    "riverside": {"lat": 33.9533, "lon": -117.3962},
    "lexington": {"lat": 38.0406, "lon": -84.5037},
    "stockton": {"lat": 37.9577, "lon": -121.2908},
    "st_louis": {"lat": 38.6270, "lon": -90.1994},
    "pittsburgh": {"lat": 40.4406, "lon": -79.9959},
    "st_paul": {"lat": 44.9537, "lon": -93.0900},
    "anchorage": {"lat": 61.2181, "lon": -149.9003},
    "cincinnati": {"lat": 39.1031, "lon": -84.5120},
    "henderson": {"lat": 36.0395, "lon": -114.9817},
    "greensboro": {"lat": 36.0726, "lon": -79.7920},
    "plano": {"lat": 33.0198, "lon": -96.6989},
    "newark": {"lat": 40.7357, "lon": -74.1724},
    "toledo": {"lat": 41.6528, "lon": -83.5379},
    "lincoln": {"lat": 40.8136, "lon": -96.7026},
    "orlando": {"lat": 28.5383, "lon": -81.3792},
    "chula_vista": {"lat": 32.6401, "lon": -117.0842},
    "irvine": {"lat": 33.6846, "lon": -117.8265},
    "fort_wayne": {"lat": 41.0793, "lon": -85.1394},
    "jersey_city": {"lat": 40.7282, "lon": -74.0776},
    "st_petersburg": {"lat": 27.7676, "lon": -82.6403},
    "laredo": {"lat": 27.5036, "lon": -99.5075},
    "madison": {"lat": 43.0731, "lon": -89.4012},
    "chandler": {"lat": 33.3062, "lon": -111.8413},
    "norfolk": {"lat": 36.8508, "lon": -76.2859},
    "durham": {"lat": 35.9940, "lon": -78.8986},
    "lubbock": {"lat": 33.5779, "lon": -101.8552},
    "winston_salem": {"lat": 36.0999, "lon": -80.2442},
    "garland": {"lat": 32.9126, "lon": -96.6389},
    "glendale": {"lat": 33.5387, "lon": -112.1860},
    "hialeah": {"lat": 25.8576, "lon": -80.2781},
    "reno": {"lat": 39.5296, "lon": -119.8138},
    "baton_rouge": {"lat": 30.4515, "lon": -91.1871},
    "irving": {"lat": 32.8140, "lon": -96.9489},
    "chesapeake": {"lat": 36.7682, "lon": -76.2875},
    "scottsdale": {"lat": 33.4942, "lon": -111.9261},
    "north_las_vegas": {"lat": 36.1989, "lon": -115.1175},
    "fremont": {"lat": 37.5485, "lon": -121.9886},
    "gilbert": {"lat": 33.3528, "lon": -111.7890},
    "san_bernardino": {"lat": 34.1083, "lon": -117.2898},
    "boise": {"lat": 43.6150, "lon": -116.2023},
    # Nếu muốn thêm ~200-300 nữa, tải CSV từ https://simplemaps.com/data/us-cities (free basic)
}

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  
KAFKA_TOPIC = 'weather-data'
KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'weather-producer-python',
    'acks': 1,
    'retries': 3,
}

# ────────────────────────────────────────────────
# HÀM HỖ TRỢ (giữ nguyên)
# ────────────────────────────────────────────────
def get_point_data(lat, lon):
    url = f"https://api.weather.gov/points/{lat},{lon}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Lỗi lấy point ({lat},{lon}): {e}")
        return None

def get_nearest_station_id(stations_url):
    if not stations_url:
        return None
    try:
        r = requests.get(stations_url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        features = data.get('features', [])
        if features:
            station = features[0]
            props = station.get('properties', {})
            station_id = props.get('stationIdentifier')
            if station_id:
                return station_id
            if 'id' in station:
                return station['id'].rstrip('/').split('/')[-1]
    except Exception as e:
        print(f"Lỗi lấy stations: {e}")
    return None

def get_latest_observation(station_id):
    if not station_id:
        return None
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        props = data.get('properties', {})
        def get_value(obj, default=None):
            return obj.get('value') if isinstance(obj, dict) else default
        obs = {
            'station_id': station_id,
            'observation_time': props.get('observationTime'),
            'temperature_c': get_value(props.get('temperature')),
            'dewpoint_c': get_value(props.get('dewpoint')),
            'wind_speed_kmh': get_value(props.get('windSpeed')),
            'wind_direction_deg': get_value(props.get('windDirection')),
            'wind_gust_kmh': get_value(props.get('windGust')),
            'humidity_percent': get_value(props.get('relativeHumidity')),
            'pressure_hpa': get_value(props.get('barometricPressure'), 0) / 100 if get_value(props.get('barometricPressure')) else None,
            'visibility_km': get_value(props.get('visibility'), 0) / 1000 if get_value(props.get('visibility')) else None,
            'text_description': props.get('textDescription'),
            'icon': props.get('icon'),
            'present_weather': [p.get('raw') for p in props.get('presentWeather', [])],
            'fetched_at': datetime.utcnow().isoformat() + 'Z',
            'location': None  # fill sau
        }
        return obs
    except Exception as e:
        print(f"Lỗi lấy observation {station_id}: {e}")
        return None

# ────────────────────────────────────────────────
# KHỞI TẠO KAFKA PRODUCER
# ────────────────────────────────────────────────
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# ────────────────────────────────────────────────
# CACHE STATIONS (chạy 1 lần đầu)
# ────────────────────────────────────────────────
print("Khởi động producer thời tiết → Kafka")
print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
print(f"Monitoring {len(LOCATIONS)} major US cities")
print("Lấy station IDs một lần...\n")

stations = {}
for name, pos in LOCATIONS.items():
    point = get_point_data(pos['lat'], pos['lon'])
    if point:
        stations_url = point['properties'].get('observationStations')
        station_id = get_nearest_station_id(stations_url)
        stations[name] = {
            'station_id': station_id,
            'city': name.replace('_', ' ').title(),
            'lat': pos['lat'],
            'lon': pos['lon']
        }
        status = station_id or 'Không tìm thấy'
        print(f"{stations[name]['city']}: station = {status}")
    else:
        print(f"Không lấy được metadata cho {name}")

if not any(info['station_id'] for info in stations.values()):
    print("Không tìm thấy trạm nào → dừng chương trình.")
    exit(1)

# ────────────────────────────────────────────────
# MAIN LOOP - PARALLEL FETCH
# ────────────────────────────────────────────────
def fetch_and_send_for_city(name, info):
    if not info['station_id']:
        return
    obs = get_latest_observation(info['station_id'])
    if obs:
        obs['location'] = {
            'city': info['city'],
            'latitude': info['lat'],
            'longitude': info['lon']
        }
        key = name.encode('utf-8')
        value = json.dumps(obs, ensure_ascii=False).encode('utf-8')
        producer.produce(
            KAFKA_TOPIC,
            key=key,
            value=value,
            callback=delivery_report
        )
        # Debug print
        print(f"→ Đã gửi cho {info['city']}:")
        print(f"   Thời gian: {obs.get('observation_time', 'N/A')}")
        print(f"   Nhiệt độ: {obs.get('temperature_c', 'N/A')} °C")
        print(f"   Mô tả: {obs.get('text_description', 'N/A')}")
    else:
        print(f"→ Không lấy được dữ liệu mới cho {info['city']}")

try:
    while True:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\n──────────── {now_str} ────────────")

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(fetch_and_send_for_city, name, info)
                for name, info in stations.items()
            ]
            for future in as_completed(futures):
                future.result()  # raise nếu có lỗi trong thread

        producer.flush(timeout=10)  # Đảm bảo gửi hết
        print(f"Chờ 362 phút ({362//60} giờ {362%60} phút) cho lần tiếp theo...\n")
        time.sleep(362 * 60)

except KeyboardInterrupt:
    print("\nDừng producer...")
finally:
    producer.flush(30)
    producer.poll(0)
    print("Kafka producer đã đóng.")