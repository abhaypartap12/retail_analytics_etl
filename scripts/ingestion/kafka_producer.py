from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Kafka configuration
conf = {
    'bootstrap.servers': 'kafka:9093',
    'client.id': 'sales-producer'
}

producer = Producer(conf)

def generate_sale_event():
    event = {
        "sale_id": str(random.randint(100000, 999999)),
        "customer_id": random.randint(1,73),
        "product_id": random.randint(101,142),
        "quantity": random.randint(1, 5),
        "sale_timestamp": datetime.utcnow().isoformat()
    }
    return event

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Produce 10 events
if __name__ == "__main__":
    try:
        for _ in range(500):
            sale_event = generate_sale_event()
            producer.produce(
                topic="transaction-events",
                key=str(sale_event["sale_id"]),
                value=json.dumps(sale_event),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(1)  # simulate 1 event per second
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        producer.flush()
