import json

from kafka import KafkaConsumer
from configs import kafka_config

prefix = "svitlana_mishuhli"

topics = [
    f"{prefix}_temperature_alerts",
    f"{prefix}_humidity_alerts",
]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{prefix}_alerts_reader_v1",
)

counts = {topic: 0 for topic in topics}

print("Читаю сповіщення з обох топіків.", flush=True)

try:
    for record in consumer:
        counts[record.topic] += 1

        print(f"\nТОПІК: {record.topic}", flush=True)
        print(
            f"partition={record.partition}, offset={record.offset}",
            flush=True,
        )
        print(
            json.dumps(record.value, ensure_ascii=False, indent=2),
            flush=True,
        )

except KeyboardInterrupt:
    print("\nЧитання зупинено.")
finally:
    consumer.close()

    print("\nКількість прочитаних сповіщень:")
    for topic, count in counts.items():
        print(f"{topic}: {count}")