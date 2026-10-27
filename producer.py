import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from configs import kafka_config

topic_name = "svitlana_mishuhli_building_sensors"

# Один запуск програми — один незмінний ID датчика.
sensor_id = str(uuid.uuid4())

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

print(f"Датчик запущено: {sensor_id}", flush=True)

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2),
        }

        result = producer.send(
            topic_name,
            key=sensor_id.encode("utf-8"),
            value=data,
        ).get(timeout=15)

        print(
            f"Надіслано: {json.dumps(data, ensure_ascii=False)} | "
            f"partition={result.partition}, offset={result.offset}",
            flush=True,
        )

        time.sleep(2)

except KeyboardInterrupt:
    print("\nДатчик зупинено.")
finally:
    producer.close()