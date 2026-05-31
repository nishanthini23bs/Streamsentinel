from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# Consumer = collects letters FROM the postbox
# ─────────────────────────────────────────
def get_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv(
            'KAFKA_BROKER', 'localhost:9092'
        ),
        value_deserializer=lambda x: json.loads(
            x.decode('utf-8')
        ),
        auto_offset_reset='earliest',
        group_id='streamsentinel-group'
    )

def start_consuming():
    topic    = os.getenv('KAFKA_TOPIC_ORDERS', 'orders_stream')
    consumer = get_consumer(topic)

    print("👂 Consumer listening to Kafka...")
    print(f"📬 Topic: '{topic}'")
    print("Waiting for orders...\n")

    for message in consumer:
        order = message.value

        if order.get('is_anomaly'):
            print(f"⚠️  ANOMALY received → "
                  f"{order.get('anomaly_type'):15} | "
                  f"{order['order_id'][:8]}")
        else:
            print(f"📥 Order received   → "
                  f"{order.get('restaurant','?'):12} | "
                  f"₹{order.get('amount', 0):8.2f} | "
                  f"{order.get('city','?')}")

if __name__ == "__main__":
    start_consuming()