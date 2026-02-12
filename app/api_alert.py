import requests
import json
import time
from datetime import datetime
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor, as_completed

# Config Kafka Producer
KAFKA_BOOTSTRAP = 'kafka:9092'
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'client.id': 'weather-alerts-producer',
    'acks': 1,
    'compression.type': 'gzip'
})

# Headers bắt buộc cho NWS API - THAY EMAIL THẬT VÀO ĐÂY
HEADERS = {
    "User-Agent": "(your_real_email@example.com) weather-alerts-producer",
    "Accept": "application/geo+json"
}

# Danh sách ~100 thành phố lớn nhất Mỹ (giữ nguyên như trước)
CITIES = {
    "new_york": {"lat": 40.7128, "lon": -74.0060, "key": "new_york"},
    "los_angeles": {"lat": 34.0522, "lon": -118.2437, "key": "los_angeles"},
    "chicago": {"lat": 41.8781, "lon": -87.6298, "key": "chicago"},
    "houston": {"lat": 29.7604, "lon": -95.3698, "key": "houston"},
    "phoenix": {"lat": 33.4484, "lon": -112.0740, "key": "phoenix"},
    "philadelphia": {"lat": 39.9526, "lon": -75.1652, "key": "philadelphia"},
    "san_antonio": {"lat": 29.4241, "lon": -98.4936, "key": "san_antonio"},
    "san_diego": {"lat": 32.7157, "lon": -117.1611, "key": "san_diego"},
    "dallas": {"lat": 32.7767, "lon": -96.7970, "key": "dallas"},
    "san_jose": {"lat": 37.3382, "lon": -121.8863, "key": "san_jose"},
    "austin": {"lat": 30.2672, "lon": -97.7431, "key": "austin"},
    "jacksonville": {"lat": 30.3322, "lon": -81.6557, "key": "jacksonville"},
    "fort_worth": {"lat": 32.7555, "lon": -97.3308, "key": "fort_worth"},
    "columbus": {"lat": 39.9612, "lon": -82.9988, "key": "columbus"},
    "charlotte": {"lat": 35.2271, "lon": -80.8431, "key": "charlotte"},
    "indianapolis": {"lat": 39.7684, "lon": -86.1581, "key": "indianapolis"},
    "san_francisco": {"lat": 37.7749, "lon": -122.4194, "key": "san_francisco"},
    "seattle": {"lat": 47.6062, "lon": -122.3321, "key": "seattle"},
    "denver": {"lat": 39.7392, "lon": -104.9903, "key": "denver"},
    "oklahoma_city": {"lat": 35.4676, "lon": -97.5164, "key": "oklahoma_city"},
    "nashville": {"lat": 36.1627, "lon": -86.7816, "key": "nashville"},
    "el_paso": {"lat": 31.7619, "lon": -106.4850, "key": "el_paso"},
    "washington_dc": {"lat": 38.9072, "lon": -77.0369, "key": "washington_dc"},
    "boston": {"lat": 42.3601, "lon": -71.0589, "key": "boston"},
    "las_vegas": {"lat": 36.1699, "lon": -115.1398, "key": "las_vegas"},
    "portland": {"lat": 45.5152, "lon": -122.6784, "key": "portland"},
    "detroit": {"lat": 42.3314, "lon": -83.0458, "key": "detroit"},
    "louisville": {"lat": 38.2527, "lon": -85.7585, "key": "louisville"},
    "memphis": {"lat": 35.1495, "lon": -90.0490, "key": "memphis"},
    "baltimore": {"lat": 39.2904, "lon": -76.6122, "key": "baltimore"},
    "milwaukee": {"lat": 43.0389, "lon": -87.9065, "key": "milwaukee"},
    "albuquerque": {"lat": 35.0844, "lon": -106.6504, "key": "albuquerque"},
    "tucson": {"lat": 32.2226, "lon": -110.9747, "key": "tucson"},
    "fresno": {"lat": 36.7378, "lon": -119.7871, "key": "fresno"},
    "sacramento": {"lat": 38.5816, "lon": -121.4944, "key": "sacramento"},
    "mesa": {"lat": 33.4152, "lon": -111.8315, "key": "mesa"},
    "kansas_city": {"lat": 39.0997, "lon": -94.5786, "key": "kansas_city"},
    "atlanta": {"lat": 33.7490, "lon": -84.3880, "key": "atlanta"},
    "omaha": {"lat": 41.2565, "lon": -95.9345, "key": "omaha"},
    "colorado_springs": {"lat": 38.8339, "lon": -104.8214, "key": "colorado_springs"},
    "raleigh": {"lat": 35.7796, "lon": -78.6382, "key": "raleigh"},
    "miami": {"lat": 25.7617, "lon": -80.1918, "key": "miami"},
    "long_beach": {"lat": 33.7701, "lon": -118.1937, "key": "long_beach"},
    "virginia_beach": {"lat": 36.8508, "lon": -76.2859, "key": "virginia_beach"},
    "oakland": {"lat": 37.8044, "lon": -122.2711, "key": "oakland"},
    "minneapolis": {"lat": 44.9778, "lon": -93.2650, "key": "minneapolis"},
    "tulsa": {"lat": 36.1540, "lon": -95.9928, "key": "tulsa"},
    "bakersfield": {"lat": 35.3733, "lon": -119.0187, "key": "bakersfield"},
    "wichita": {"lat": 37.6872, "lon": -97.3301, "key": "wichita"},
    "arlington": {"lat": 32.7357, "lon": -97.1081, "key": "arlington"},
    "new_orleans": {"lat": 29.9511, "lon": -90.0715, "key": "new_orleans"},
    "cleveland": {"lat": 41.4993, "lon": -81.6944, "key": "cleveland"},
    "tampa": {"lat": 27.9506, "lon": -82.4572, "key": "tampa"},
    "aurora": {"lat": 39.7294, "lon": -104.8319, "key": "aurora"},
    "honolulu": {"lat": 21.3069, "lon": -157.8583, "key": "honolulu"},
    "anaheim": {"lat": 33.8353, "lon": -117.9145, "key": "anaheim"},
    "santa_ana": {"lat": 33.7456, "lon": -117.8678, "key": "santa_ana"},
    "corpus_christi": {"lat": 27.8003, "lon": -97.3960, "key": "corpus_christi"},
    "riverside": {"lat": 33.9533, "lon": -117.3962, "key": "riverside"},
    "lexington": {"lat": 38.0406, "lon": -84.5037, "key": "lexington"},
    "stockton": {"lat": 37.9577, "lon": -121.2908, "key": "stockton"},
    "st_louis": {"lat": 38.6270, "lon": -90.1994, "key": "st_louis"},
    "pittsburgh": {"lat": 40.4406, "lon": -79.9959, "key": "pittsburgh"},
    "st_paul": {"lat": 44.9537, "lon": -93.08996, "key": "st_paul"},
    "anchorage": {"lat": 61.2181, "lon": -149.9003, "key": "anchorage"},
    "cincinnati": {"lat": 39.1031, "lon": -84.5120, "key": "cincinnati"},
    "henderson": {"lat": 36.0395, "lon": -114.9817, "key": "henderson"},
    "greensboro": {"lat": 36.0726, "lon": -79.79198, "key": "greensboro"},
    "plano": {"lat": 33.0198, "lon": -96.6989, "key": "plano"},
    "newark": {"lat": 40.7357, "lon": -74.1724, "key": "newark"},
    "toledo": {"lat": 41.6528, "lon": -83.5379, "key": "toledo"},
    "lincoln": {"lat": 40.8136, "lon": -96.7026, "key": "lincoln"},
    "orlando": {"lat": 28.5383, "lon": -81.3792, "key": "orlando"},
    "chula_vista": {"lat": 32.6401, "lon": -117.0842, "key": "chula_vista"},
    "irvine": {"lat": 33.6846, "lon": -117.8265, "key": "irvine"},
    "fort_wayne": {"lat": 41.0793, "lon": -85.1394, "key": "fort_wayne"},
    "jersey_city": {"lat": 40.7282, "lon": -74.0776, "key": "jersey_city"},
    "st_petersburg": {"lat": 27.7676, "lon": -82.6403, "key": "st_petersburg"},
    "laredo": {"lat": 27.5036, "lon": -99.5075, "key": "laredo"},
    "madison": {"lat": 43.0731, "lon": -89.4012, "key": "madison"},
    "chandler": {"lat": 33.3062, "lon": -111.8413, "key": "chandler"},
    "norfolk": {"lat": 36.8508, "lon": -76.2859, "key": "norfolk"},
    "durham": {"lat": 35.9940, "lon": -78.8986, "key": "durham"},
    "lubbock": {"lat": 33.5779, "lon": -101.8552, "key": "lubbock"},
    "winston_salem": {"lat": 36.0999, "lon": -80.2442, "key": "winston_salem"},
    "garland": {"lat": 32.9126, "lon": -96.6389, "key": "garland"},
    "glendale": {"lat": 33.5387, "lon": -112.1860, "key": "glendale"},
    "hialeah": {"lat": 25.8576, "lon": -80.2781, "key": "hialeah"},
    "reno": {"lat": 39.5296, "lon": -119.8138, "key": "reno"},
    "baton_rouge": {"lat": 30.4515, "lon": -91.1871, "key": "baton_rouge"},
    "irving": {"lat": 32.8140, "lon": -96.9489, "key": "irving"},
    "chesapeake": {"lat": 36.7682, "lon": -76.2875, "key": "chesapeake"},
    "scottsdale": {"lat": 33.4942, "lon": -111.9261, "key": "scottsdale"},
    "north_las_vegas": {"lat": 36.1989, "lon": -115.1175, "key": "north_las_vegas"},
    "fremont": {"lat": 37.5485, "lon": -121.9886, "key": "fremont"},
    "gilbert": {"lat": 33.3528, "lon": -111.7890, "key": "gilbert"},
    "san_bernardino": {"lat": 34.1083, "lon": -117.2898, "key": "san_bernardino"},
    "boise": {"lat": 43.6150, "lon": -116.2023, "key": "boise"},
}

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered alert to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")

def fetch_alerts_for_city(city_name, config):
    url = f"https://api.weather.gov/alerts/active?point={config['lat']},{config['lon']}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        alerts = data.get("features", [])
        num_alerts = len(alerts)
        timestamp = datetime.utcnow().isoformat()
        print(f"[{timestamp}] {city_name.upper()}: Found {num_alerts} active alerts.")

        for alert in alerts:
            props = alert["properties"]
            payload = {
                "city": city_name,
                "fetched_at": timestamp,
                "id": alert.get("id"),
                "event": props.get("event"),
                "headline": props.get("headline"),
                "severity": props.get("severity"),
                "urgency": props.get("urgency"),
                "certainty": props.get("certainty"),
                "sent": props.get("sent"),
                "effective": props.get("effective"),
                "expires": props.get("expires"),
                "areaDesc": props.get("areaDesc"),
                "description": props.get("description"),
                "instruction": props.get("instruction"),
                "response": props.get("response"),
            }

            producer.produce(
                topic="weather-alerts",
                key=config["key"].encode('utf-8'),
                value=json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )

        producer.flush(timeout=5)  # Flush per city để an toàn, nhưng vì parallel thì mỗi thread flush riêng

        return num_alerts  # Trả về để log nếu cần

    except requests.exceptions.RequestException as e:
        print(f"Error fetching alerts for {city_name}: {e}")
        return 0
    except Exception as e:
        print(f"Unexpected error for {city_name}: {e}")
        return 0

def fetch_and_send_alerts_parallel():
    with ThreadPoolExecutor(max_workers=20) as executor:  # Chạy song song 20 threads (điều chỉnh nếu cần, tránh overload NWS)
        future_to_city = {executor.submit(fetch_alerts_for_city, city_name, config): city_name 
                          for city_name, config in CITIES.items()}
        
        for future in as_completed(future_to_city):
            city_name = future_to_city[future]
            try:
                num_alerts = future.result()
                # Có thể log thêm nếu cần
            except Exception as e:
                print(f"Thread error for {city_name}: {e}")

if __name__ == "__main__":
    print("Starting weather alerts producer (poll every 1 minute, parallel fetch)...")
    print(f"Monitoring {len(CITIES)} major US cities")
    while True:
        fetch_and_send_alerts_parallel()
        time.sleep(3600)  # 1 phút = 60 giây