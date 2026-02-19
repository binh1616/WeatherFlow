import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host": "mysql",   # đổi thành "mysql" nếu chạy từ container khác
    "port": 3306,
    "user": "root",
    "password": "root"
}

def run():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("✅ Connected to MySQL")

        # ------------------ CREATE DATABASE ------------------
        cursor.execute("CREATE DATABASE IF NOT EXISTS flink_db;")
        cursor.execute("USE flink_db;")

        # ------------------ TABLE 1 ------------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_avg_per_hours (
            window_start DATETIME,
            window_end DATETIME,
            city VARCHAR(100),
            avg_temperature_c DOUBLE,
            avg_humidity DOUBLE,
            avg_wind_speed_kmh DOUBLE,
            avg_pressure_hpa DOUBLE,
            PRIMARY KEY (window_start, city)
        );
        """)

        # ------------------ TABLE 2 ------------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_active_alerts (
            city VARCHAR(50),
            alert_id VARCHAR(255) PRIMARY KEY,
            event VARCHAR(100),
            headline TEXT,
            severity VARCHAR(50),
            severity_level INT,
            urgency VARCHAR(50),
            certainty VARCHAR(50),
            sent DATETIME,
            effective DATETIME,
            expires DATETIME,
            is_active BOOLEAN,
            alert_duration_hours DOUBLE,
            area_desc TEXT,
            description TEXT,
            instruction TEXT,
            response VARCHAR(50),
            insert_time DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)

        print("✅ Tables created")

        # ------------------ GRANTS ------------------
        cursor.execute("GRANT ALL PRIVILEGES ON flink_db.* TO 'flink'@'%';")
        cursor.execute("FLUSH PRIVILEGES;")
        print("✅ Privileges granted to user 'flink'")

        # ------------------ CDC SETTINGS ------------------
        cursor.execute("SET GLOBAL binlog_format = 'ROW';")
        cursor.execute("SET GLOBAL binlog_row_image = 'FULL';")
        print("✅ Binlog configured for CDC")

        cursor.close()
        conn.close()
        print("🎉 MySQL initialization completed successfully!")

    except Error as e:
        print("❌ Error:", e)


if __name__ == "__main__":
    run()
