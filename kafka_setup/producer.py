from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# Producer = puts letters INTO the postbox
# ─────────────────────────────────────────
def get_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv(
            'KAFKA_BROKER', 'localhost:9092'
        ),
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',   # wait for save confirmation
        retries=3     # retry 3 times if it fails
    )

def send_message(producer, topic, message):
    try:
        producer.send(topic, value=message)
        producer.flush()
        print(f"✅ Sent: {message.get('order_id','?')[:8]}")
        return True
    except KafkaError as e:
        print(f"❌ Send failed: {e}")
        return False