# alerts_pipeline_sql.py
# PyFlink 1.20 - Execute the exact same SQL logic from your original file
# Kafka source → MySQL sink (filtered & enriched alerts)

from pyflink.table import EnvironmentSettings, TableEnvironment

# Tạo TableEnvironment ở chế độ streaming
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Cấu hình checkpointing (tùy chọn, khớp với setup của bạn)
table_env.get_config().set("execution.checkpointing.interval", "10s")
# table_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")  # default là EXACTLY_ONCE

# ─────────────────────────────────────────────────────────────
# 1. Đăng ký source table (Kafka)
# ─────────────────────────────────────────────────────────────
table_env.execute_sql("""
CREATE TABLE weather_alerts_kafka (
    city STRING,
    fetched_at STRING,
    id STRING,
    event STRING,
    headline STRING,
    severity STRING,
    urgency STRING,
    certainty STRING,
    sent STRING,
    effective STRING,
    expires STRING,
    areaDesc STRING,
    description STRING,
    instruction STRING,
    response STRING,

    event_time AS TO_TIMESTAMP(fetched_at, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'weather-alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-alerts-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
""")

# ─────────────────────────────────────────────────────────────
# 2. Đăng ký sink table (JDBC MySQL)
# ─────────────────────────────────────────────────────────────
table_env.execute_sql("""
CREATE TABLE weather_active_alerts_sink (
    city STRING,
    alert_id STRING,
    event STRING,
    headline STRING,
    severity STRING,
    severity_level INT,
    urgency STRING,
    certainty STRING,
    sent TIMESTAMP(3),
    effective TIMESTAMP(3),
    expires TIMESTAMP(3),
    is_active BOOLEAN,
    alert_duration_hours DOUBLE,
    area_desc STRING,
    description STRING,
    instruction STRING,
    response STRING,

    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/flink_db?useSSL=false&serverTimezone=UTC',
    'table-name' = 'weather_active_alerts',
    'username' = 'flink',
    'password' = 'flink',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3'
)
""")

# ─────────────────────────────────────────────────────────────
# 3. Thực thi INSERT query (logic chính)
# ─────────────────────────────────────────────────────────────
table_env.execute_sql("""
INSERT INTO weather_active_alerts_sink
SELECT
    city,
    id AS alert_id,
    event,
    headline,
    severity,
    CASE severity
        WHEN 'Extreme' THEN 4
        WHEN 'Severe' THEN 3
        WHEN 'Moderate' THEN 2
        WHEN 'Minor' THEN 1
        ELSE 0
    END AS severity_level,
    urgency,
    certainty,
    TO_TIMESTAMP(sent, 'yyyy-MM-dd''T''HH:mm:ssXXX') AS sent,
    TO_TIMESTAMP(effective, 'yyyy-MM-dd''T''HH:mm:ssXXX') AS effective,
    TO_TIMESTAMP(expires, 'yyyy-MM-dd''T''HH:mm:ssXXX') AS expires,
    (expires IS NULL OR TO_TIMESTAMP(expires, 'yyyy-MM-dd''T''HH:mm:ssXXX') > CURRENT_TIMESTAMP)
     AND (effective IS NULL OR TO_TIMESTAMP(effective, 'yyyy-MM-dd''T''HH:mm:ssXXX') <= CURRENT_TIMESTAMP)
    AS is_active,
    TIMESTAMPDIFF(SECOND,
        TO_TIMESTAMP(effective, 'yyyy-MM-dd''T''HH:mm:ssXXX'),
        TO_TIMESTAMP(expires, 'yyyy-MM-dd''T''HH:mm:ssXXX')
    ) / 3600.0 AS alert_duration_hours,
    areaDesc AS area_desc,
    description,
    instruction,
    response
FROM weather_alerts_kafka
WHERE severity IN ('Moderate', 'Severe', 'Extreme')
""")

# Chạy job streaming (không cần .wait() vì job chạy vô thời hạn)
print("Job submitted. Check Flink UI at http://jobmanager:8081")
# table_env.execute("Weather Alerts SQL Pipeline")  # Nếu muốn đặt tên job rõ ràng hơn