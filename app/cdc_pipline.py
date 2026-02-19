# alerts_cdc_pipeline_sql_debug.py
# PyFlink 1.20 - Chạy lần lượt từng lệnh SQL cho MySQL CDC → Kafka changelog
# Pipeline: weather_active_alerts (MySQL) → Debezium JSON changelog vào Kafka topic 'weather-alerts-changelog'

from pyflink.table import EnvironmentSettings, TableEnvironment

# Tạo TableEnvironment streaming
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Checkpointing
table_env.get_config().set("execution.checkpointing.interval", "10s")

print("=== Bắt đầu debug từng lệnh SQL cho MySQL CDC → Kafka Changelog ===")

# ─────────────────────────────────────────────────────────────
# Bước 1: SET checkpointing interval (đã set ở config, nhưng chạy lại lệnh SET nếu cần)
# ─────────────────────────────────────────────────────────────
print("\n[1] SET 'execution.checkpointing.interval' = '10s'")
table_env.get_config().set("execution.checkpointing.interval", "10s")
# Hoặc dùng execute_sql nếu bạn muốn chạy lệnh SET qua SQL
# table_env.execute_sql("SET 'execution.checkpointing.interval' = '10s'")
print("-> Checkpointing interval đã set thành 10s")

# ─────────────────────────────────────────────────────────────
# Bước 2: CREATE TABLE mysql_alerts_cdc_source (MySQL CDC)
# ─────────────────────────────────────────────────────────────
print("\n[2] CREATE TABLE mysql_alerts_cdc_source")
table_env.execute_sql("""
CREATE TABLE mysql_alerts_cdc_source (
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
    insert_time TIMESTAMP(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'root',
    'database-name' = 'flink_db',
    'table-name' = 'weather_active_alerts',
    'scan.startup.mode' = 'initial',
    'debezium.database.serverTimezone' = 'UTC',
    'debezium.snapshot.mode' = 'initial'
)
""")
print("-> Đã tạo source table mysql_alerts_cdc_source (MySQL CDC)")
print("  - Flink sẽ snapshot toàn bộ table 'weather_active_alerts' lần đầu")
print("  - Sau đó capture binlog changes realtime")

# ─────────────────────────────────────────────────────────────
# Bước 3: CREATE TABLE kafka_changelog_sink (Kafka với debezium-json)
# ─────────────────────────────────────────────────────────────
print("\n[3] CREATE TABLE kafka_changelog_sink")
table_env.execute_sql("""
CREATE TABLE kafka_changelog_sink (
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
    insert_time TIMESTAMP(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'weather-alerts-changelog',
    'properties.bootstrap.servers' = 'kafka:9092',
    'value.format' = 'debezium-json',
    'value.debezium-json.ignore-parse-errors' = 'true'
)
""")
print("-> Đã tạo sink table kafka_changelog_sink (Kafka topic 'weather-alerts-changelog')")
print("  - Format: debezium-json (full envelope: before, after, op)")

# ─────────────────────────────────────────────────────────────
# Bước 4: INSERT INTO kafka_changelog_sink SELECT * FROM mysql_alerts_cdc_source
# ─────────────────────────────────────────────────────────────
print("\n[4] INSERT INTO kafka_changelog_sink SELECT * FROM mysql_alerts_cdc_source")
# Tạo StatementSet để submit job streaming
statement_set = table_env.create_statement_set()
statement_set.add_insert_sql("""
INSERT INTO kafka_changelog_sink
SELECT * FROM mysql_alerts_cdc_source
""")

# Thực thi job (chạy nền, không block)
statement_set.execute()
print("-> Job CDC → Kafka đã được submit và đang chạy.")
print("  - Flink sẽ snapshot toàn bộ table trước, sau đó stream changes realtime.")
print("  - Kiểm tra Flink UI: http://jobmanager:8081")
print("  - Kiểm tra Kafka topic: kafka-console-consumer --bootstrap-server kafka:9092 --topic weather-alerts-changelog --from-beginning")
print("  - Để dừng job: dùng Flink UI hoặc lệnh flink cancel <job_id>")