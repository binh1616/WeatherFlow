# weather_avg_pipeline_sql_debug.py
# PyFlink 1.20 - Chạy lần lượt từng câu lệnh SQL để debug
# Pipeline: weather-current-observations (Kafka) → avg per day theo city → MySQL sink

from pyflink.table import EnvironmentSettings, TableEnvironment

# Tạo TableEnvironment streaming
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Checkpointing
table_env.get_config().set("execution.checkpointing.interval", "10s")

print("=== Bắt đầu debug từng câu lệnh SQL ===")

# ─────────────────────────────────────────────────────────────
# Bước 1: DROP TABLE IF EXISTS weather_source
# ─────────────────────────────────────────────────────────────
print("\n[1] DROP TABLE IF EXISTS weather_source")
table_env.execute_sql("DROP TABLE IF EXISTS weather_source")
print("-> Đã chạy DROP source table (nếu tồn tại)")

# ─────────────────────────────────────────────────────────────
# Bước 2: CREATE TABLE weather_source (Kafka)
# ─────────────────────────────────────────────────────────────
print("\n[2] CREATE TABLE weather_source")
table_env.execute_sql("""
CREATE TABLE weather_source (
    station_id STRING,
    observation_time STRING,
    temperature_c DOUBLE,
    dewpoint_c DOUBLE,
    wind_speed_kmh DOUBLE,
    wind_direction_deg DOUBLE,
    wind_gust_kmh DOUBLE,
    humidity_percent DOUBLE,
    pressure_hpa DOUBLE,
    visibility_km DOUBLE,
    text_description STRING,
    icon STRING,
    present_weather ARRAY<STRING>,
    fetched_at STRING,
    location ROW<
        city STRING,
        latitude DOUBLE,
        longitude DOUBLE
    >,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'weather-data',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-weather-proctime',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
""")
print("-> Đã tạo source table weather_source từ Kafka")

# ─────────────────────────────────────────────────────────────
# Bước 3: DROP TABLE IF EXISTS weather_avg_sink
# ─────────────────────────────────────────────────────────────
print("\n[3] DROP TABLE IF EXISTS weather_avg_sink")
table_env.execute_sql("DROP TABLE IF EXISTS weather_avg_sink")
print("-> Đã chạy DROP sink table (nếu tồn tại)")

# ─────────────────────────────────────────────────────────────
# Bước 4: CREATE TABLE weather_avg_sink (JDBC)
# ─────────────────────────────────────────────────────────────
print("\n[4] CREATE TABLE weather_avg_sink")
table_env.execute_sql("""
CREATE TABLE weather_avg_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    city STRING,
    avg_temperature_c DOUBLE,
    avg_humidity DOUBLE,
    avg_wind_speed_kmh DOUBLE,
    avg_pressure_hpa DOUBLE,
    PRIMARY KEY (window_start, city) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/flink_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC',
    'table-name' = 'weather_avg_per_minute',
    'username' = 'flink',
    'password' = 'flink',
    'sink.buffer-flush.max-rows' = '1',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '5'
)
""")
print("-> Đã tạo sink table weather_avg_sink vào MySQL")

# ─────────────────────────────────────────────────────────────
# Bước 5: INSERT INTO weather_avg_sink (aggregation)
# ─────────────────────────────────────────────────────────────
print("\n[5] INSERT INTO weather_avg_sink (aggregation per day)")
statement_set = table_env.create_statement_set()
statement_set.add_insert_sql("""
INSERT INTO weather_avg_sink
SELECT
    TUMBLE_START(proctime, INTERVAL '1' DAY) AS window_start,
    TUMBLE_END(proctime, INTERVAL '1' DAY) AS window_end,
    location.city AS city,
    AVG(temperature_c) AS avg_temperature_c,
    AVG(humidity_percent) AS avg_humidity,
    AVG(wind_speed_kmh) AS avg_wind_speed_kmh,
    AVG(pressure_hpa) AS avg_pressure_hpa
FROM weather_source
GROUP BY
    TUMBLE(proctime, INTERVAL '1' DAY),
    location.city
""")

# Chạy job streaming (không block, job chạy nền)
statement_set.execute()
print("-> Job aggregation đã được submit và đang chạy.")
print("  Kiểm tra Flink UI: http://jobmanager:8081")
print("  Theo dõi table weather_avg_per_minute trong MySQL để xem dữ liệu.")
print("  Để dừng job: dùng Flink UI hoặc lệnh flink cancel <job_id>")