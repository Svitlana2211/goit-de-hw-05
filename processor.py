import json

from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config

prefix = "svitlana_mishuhli"
source_topic = f"{prefix}_building_sensors"
temperature_topic = f"{prefix}_temperature_alerts"
humidity_topic = f"{prefix}_humidity_alerts"

connection = {
    "bootstrap_servers": kafka_config["bootstrap_servers"],
    "security_protocol": kafka_config["security_protocol"],
    "sasl_mechanism": kafka_config["sasl_mechanism"],
    "sasl_plain_username": kafka_config["username"],
    "sasl_plain_password": kafka_config["password"],
}

consumer = KafkaConsumer(
    source_topic,
    **connection,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=f"{prefix}_processor_v1",
)

producer = KafkaProducer(
    **connection,
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
    acks="all",
)


def send_alert(topic, data, text, source):
    alert = {
        **data,
        "message": text,
        "source_partition": source.partition,
        "source_offset": source.offset,
    }

    result = producer.send(
        topic,
        key=data["sensor_id"].encode("utf-8"),
        value=alert,
    ).get(timeout=15)

    print(
        f"ЗАПИСАНО В {topic}: "
        f"{json.dumps(alert, ensure_ascii=False)} | "
        f"partition={result.partition}, offset={result.offset}",
        flush=True,
    )


print(f"Очікую дані з {source_topic}", flush=True)

try:
    for record in consumer:
        data = record.value
        temperature = data["temperature"]
        humidity = data["humidity"]

        print(
            f"\nОТРИМАНО: {json.dumps(data, ensure_ascii=False)} | "
            f"partition={record.partition}, offset={record.offset}",
            flush=True,
        )

        if temperature > 40:
            send_alert(
                temperature_topic,
                data,
                "Температура перевищує 40 °C",
                record,
            )

        if humidity > 80 or humidity < 20:
            send_alert(
                humidity_topic,
                data,
                "Вологість поза допустимими межами 20–80 %",
                record,
            )

        if temperature <= 40 and 20 <= humidity <= 80:
            print("Показники в нормі — сповіщень немає.", flush=True)

        # Підтверджуємо обробку після запису потрібних сповіщень.
        consumer.commit()

except KeyboardInterrupt:
    print("\nОбробник зупинено.")
finally:
    consumer.close()
    producer.close()