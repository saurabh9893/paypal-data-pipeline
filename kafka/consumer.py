# kafka/consumer.py
# Consumes PayPal events from Kafka → uploads to ADLS Bronze
# TODO: Will be implemented on Day 15

from kafka import KafkaConsumer
import json

KAFKA_TOPIC = "paypal-transactions"
KAFKA_BROKER = "localhost:9092"


def run_consumer():
    """Reads events from Kafka topic and lands them in ADLS."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="paypal-pipeline-group"
    )

    print(f"[CONSUMER] Listening on topic: {KAFKA_TOPIC}")
    for message in consumer:
        event = message.value
        print(f"[CONSUMER] Received: {event}")
        # TODO Day 15: upload event to ADLS


if __name__ == "__main__":
    run_consumer()
