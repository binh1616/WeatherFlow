from confluent_kafka.admin import AdminClient, NewTopic
import sys

bootstrap_servers = 'kafka:9092'  # hoặc 'localhost:9092' nếu chạy local

def create_kafka_topics():
    admin_client = AdminClient({
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'admin-client-weather'
    })

    topics_to_create = [
        NewTopic(
            topic="weather-data",
            num_partitions=3,
            replication_factor=1,
            config={"retention.ms": "604800000"}  # 7 ngày
        ),
        NewTopic(
            topic="weather-alerts",
            num_partitions=3,
            replication_factor=1,
            config={"retention.ms": "604800000"}  # 7 ngày
        ),
        # 🆕 Topic changelog cho alerts
        NewTopic(
            topic="weather-alerts-changelog",
            num_partitions=3,
            replication_factor=1,
            config={
                "retention.ms": "604800000",   # giữ 7 ngày
                "cleanup.policy": "delete"     # có thể đổi sang compact nếu cần log compaction
            }
        )
    ]

    fs = admin_client.create_topics(topics_to_create)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "TopicAlreadyExists" in str(e):
                print(f"Topic '{topic}' already exists, skipping.")
            else:
                print(f"Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    create_kafka_topics()
