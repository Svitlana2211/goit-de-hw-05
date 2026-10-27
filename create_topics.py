from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config

my_name = "svitlana_mishuhli"

topic_names = [
    f"{my_name}_building_sensors",
    f"{my_name}_temperature_alerts",
    f"{my_name}_humidity_alerts",
]

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

try:
    for topic_name in topic_names:
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=2,
            replication_factor=1,
        )

        try:
            admin_client.create_topics(new_topics=[new_topic])
            print(f"Створено: {topic_name}")
        except TopicAlreadyExistsError:
            print(f"Уже існує: {topic_name}")

    print("\nМої топіки:")
    [
        print(topic)
        for topic in admin_client.list_topics()
        if my_name in topic
    ]
finally:
    admin_client.close()