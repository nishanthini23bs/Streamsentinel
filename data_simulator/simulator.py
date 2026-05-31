import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()
fake = Faker('en_IN')

# ─────────────────────────────────────────
# Connect to Kafka
# Like plugging your tap into the pipe
# ─────────────────────────────────────────
def create_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv(
            'KAFKA_BROKER', 'localhost:9092'
        ),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

# ─────────────────────────────────────────
# Normal order — everything correct
# ─────────────────────────────────────────
def generate_order():
    return {
        "order_id":      fake.uuid4(),
        "customer_name": fake.name(),
        "restaurant":    random.choice([
                            "Dominos", "KFC", "McDonald's",
                            "Subway", "Pizza Hut", "Burger King"
                         ]),
        "amount":        round(random.uniform(100, 2000), 2),
        "status":        random.choice([
                            "placed", "confirmed",
                            "preparing", "out_for_delivery",
                            "delivered"
                         ]),
        "city":          random.choice([
                            "Chennai", "Mumbai", "Delhi",
                            "Bangalore", "Hyderabad"
                         ]),
        "timestamp":     datetime.now().isoformat(),
        "is_anomaly":    False
    }

# ─────────────────────────────────────────
# Broken order — intentionally wrong
# Tests if our detector catches it
# ─────────────────────────────────────────
def generate_anomaly():
    order        = generate_order()
    anomaly_type = random.choice([
        "missing_fields",
        "huge_amount",
        "wrong_format"
    ])
    if anomaly_type == "missing_fields":
        del order["amount"]
        del order["customer_name"]
    elif anomaly_type == "huge_amount":
        order["amount"] = 999999
    elif anomaly_type == "wrong_format":
        order["timestamp"] = "not-a-real-date"

    order["is_anomaly"]   = True
    order["anomaly_type"] = anomaly_type
    return order

# ─────────────────────────────────────────
# Main — connects to Kafka and sends orders
# ─────────────────────────────────────────
def run_simulator():
    print("🚀 StreamSentinel Data Simulator Started!")
    print("📡 Connecting to Kafka...")

    try:
        producer = create_producer()
        print("✅ Connected to Kafka!\n")
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        print("💡 Run: docker-compose up -d")
        return

    topic = os.getenv('KAFKA_TOPIC_ORDERS', 'orders_stream')
    count = 0

    print(f"📦 Sending to Kafka topic: '{topic}'")
    print("Press Ctrl+C to stop\n")

    while True:
        if random.random() < 0.05:
            order = generate_anomaly()
            print(f"⚠️  ANOMALY  → "
                  f"{order.get('anomaly_type'):15} | "
                  f"{order['order_id'][:8]}")
        else:
            order = generate_order()
            print(f"✅ Order    → "
                  f"{order['restaurant']:12} | "
                  f"₹{order['amount']:8.2f} | "
                  f"{order['city']}")

        # ── This line sends order INTO Kafka ──
        producer.send(topic, value=order)
        producer.flush()

        count += 1
        if count % 10 == 0:
            print(f"\n📊 {count} orders sent to Kafka!\n")

        time.sleep(1)

if __name__ == "__main__":
    run_simulator()