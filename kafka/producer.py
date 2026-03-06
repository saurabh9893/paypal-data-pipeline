# kafka/producer.py
# Simulates PayPal webhook events → Kafka topic
# TODO: Will be implemented on Day 14

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

KAFKA_TOPIC = "paypal-transactions"
KAFKA_BROKER = "localhost:9092"


def simulate_transaction() -> dict:
    """Generates a fake PayPal transaction event."""
    return {
        "transaction_id": f"TXN{random.randint(100000, 999999)}",
        "amount": round(random.uniform(10, 500), 2),
        "currency": "USD",
        "status": random.choice(["COMPLETED", "PENDING", "REFUNDED"]),
        "timestamp": datetime.utcnow().isoformat()
    }


def run_producer(num_events: int = 10, delay: float = 1.0):
    """Sends simulated transactions to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for i in range(num_events):
        event = simulate_transaction()
        producer.send(KAFKA_TOPIC, event)
        print(f"[PRODUCER] Sent: {event}")
        time.sleep(delay)

    producer.flush()
    print(f"[PRODUCER] Done. Sent {num_events} events.")


if __name__ == "__main__":
    run_producer()
