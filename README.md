# WeatherFlow

**Real-time Weather Data Processing Pipeline**

WeatherFlow is a powerful **real-time** weather data processing system built with **Apache Flink**, **Kafka**, and **MySQL**. It ingests current weather observations and alerts from APIs, performs aggregations and filtering, stores enriched data in MySQL, and uses **CDC** (Change Data Capture) to stream changes into Kafka in Debezium JSON format.

### Features

- Real-time ingestion of weather **observations** and **alerts** via Python services
- **Flink streaming jobs**:
  - Aggregates average temperature, humidity, wind speed, and pressure **per city** (per day or per minute)
  - Filters and enriches weather alerts (severity levels, active status, duration in hours)
- **MySQL** storage with upsert semantics (PRIMARY KEY NOT ENFORCED)
- **MySQL CDC** → Kafka topic `weather-alerts-changelog` (Debezium JSON full envelope: before/after/op)
- Python background services for API fetching, database initialization, Kafka topic creation
- Full Docker Compose stack: Flink standalone cluster, Kafka, MySQL, Python service

### System Architecture

![WeatherFlow System Architecture](weatherflow-architecture.jpg)




### Tech Stack

- **Streaming Engine**: Apache Flink 1.20 (PyFlink + Flink SQL)
- **Messaging**: Apache Kafka
- **Storage**: MySQL 8.x (JDBC sink + CDC source)
- **CDC Format**: Debezium JSON
- **Python Libraries**: requests, FastAPI/uvicorn (optional), venv
- **Deployment**: Docker Compose (Flink JobManager + TaskManagers, Kafka, MySQL, Python service)

### Getting Started

#### Prerequisites

- Docker & Docker Compose
- Git

#### Quick Start ###



1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/weatherflow.git
   cd weatherflow
   ```

2. Start the entire stack:
  ```bash
  docker compose up -d --build
  ```

3. Wait ~2 minutes for all services (Flink cluster, Kafka, MySQL, Python ingestion) to initialize and start processing data

4. Access the Flink Web UI to monitor jobs:
  ```bash
http://localhost:8081
  ```

5. View real-time data flowing into Kafka topics:
  ```bash
# Current weather observations
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic weather-current-observations --from-beginning

# Weather alerts
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic weather-alerts --from-beginning

# CDC changelog (Debezium JSON format - inserts/updates/deletes)
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic weather-alerts-changelog --from-beginning
  ```
Note: Press Ctrl+C to stop the consumer when you're done viewing.

6. After ~1 hour (due to the 1-hour tumbling window in the aggregation job), check aggregated data in MySQL:
  ```bash
# Aggregated averages per city (temperature, humidity, wind, pressure)
docker compose exec mysql mysql -u flink -pflink flink_db -e "SELECT * FROM weather_avg_per_minute LIMIT 10;"

# Active weather alerts with enrichment
docker compose exec mysql mysql -u flink -pflink flink_db -e "SELECT * FROM weather_active_alerts LIMIT 10;"
  ```

7. When finished (to clean up and remove volumes for a fresh restart):
  ```bash
docker compose down -v
  ```


License
MIT License
Acknowledgments

Apache Flink & Flink CDC
Apache Kafka & Debezium
Weather data sources (e.g., OpenWeatherMap, National Weather Service APIs)
