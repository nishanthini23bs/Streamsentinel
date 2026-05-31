import pickle
import json
import os
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from auto_remediation.fixer import remediate, get_producer
from explainer.reporter import generate_report

load_dotenv()

def load_model():
    model_path = "anomaly_detector/models/isolation_forest.pkl"
    if not os.path.exists(model_path):
        print("❌ Model not found! Run train_model.py first")
        return None
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    print("✅ Anomaly detection model loaded!")
    return model

def extract_features(order):
    try:
        amount = float(order.get("amount", -1))
        try:
            ts   = datetime.fromisoformat(order.get("timestamp", ""))
            hour = ts.hour
        except:
            hour = -1
        required = ["order_id", "customer_name",
                    "restaurant", "amount", "timestamp"]
        has_all  = all(k in order for k in required)
        return [amount, hour, 2.0, 1, 1.0 if has_all else 0.0]
    except:
        return [-1, -1, -1, -1, 0.0]

def analyze_order(model, order):
    features = extract_features(order)
    features_df = pd.DataFrame(
        [features],
        columns=["amount", "hour", "processing_time",
                 "item_count", "has_all_fields"]
    )
    prediction = model.predict(features_df)[0]
    score      = model.score_samples(features_df)[0]

    anomaly_reason = None
    if prediction == -1:
        amount  = features[0]
        hour    = features[1]
        has_all = features[4]

        if amount > 5000 or amount < 0:
            anomaly_reason = "suspicious_amount"
        elif has_all < 1.0:
            anomaly_reason = "missing_fields"
        elif hour < 0:
            anomaly_reason = "invalid_timestamp"
        else:
            anomaly_reason = "unusual_pattern"

    return {
        "order_id":       order.get("order_id", "unknown"),
        "is_anomaly":     prediction == -1,
        "anomaly_score":  round(float(score), 4),
        "anomaly_reason": anomaly_reason,
        "original_order": order,
        "detected_at":    datetime.now().isoformat()
    }

def run_detector():
    print("🚨 StreamSentinel Anomaly Detector + Auto-Fixer Started!")

    model = load_model()
    if not model:
        return

    producer = get_producer()

    consumer = KafkaConsumer(
        os.getenv('KAFKA_TOPIC_ORDERS', 'orders_stream'),
        bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='anomaly-detector-group'
    )

    print("👂 Listening to Kafka...")
    print("🔍 Detecting AND fixing anomalies in real-time!\n")

    normal_count  = 0
    anomaly_count = 0
    fixed_count   = 0

    for message in consumer:
        order = message.value

        if order.get("was_remediated"):
            continue

        result = analyze_order(model, order)

        if result["is_anomaly"]:
            anomaly_count += 1
            print(f"🚨 ANOMALY DETECTED!")
            print(f"   Order  : {result['order_id'][:8]}")
            print(f"   Reason : {result['anomaly_reason']}")
            print(f"   Score  : {result['anomaly_score']}")

            remediate(result, producer)
            fixed_count += 1

            generate_report(result)

            try:
                requests.post(
                    "http://localhost:8000/report-anomaly",
                    json={
                        "anomaly_reason": result["anomaly_reason"],
                        "order_id":       result["order_id"]
                    },
                    timeout=2
                )
            except:
                pass

        else:
            normal_count += 1
            restaurant = order.get('restaurant', '?')
            amount     = order.get('amount', 0)
            print(f"✅ Normal  → "
                  f"{restaurant:12} | "
                  f"₹{amount:8.2f} | "
                  f"Score: {result['anomaly_score']}")

            try:
                requests.post(
                    "http://localhost:8000/report-order",
                    timeout=2
                )
            except:
                pass

        total = normal_count + anomaly_count
        if total % 20 == 0:
            print(f"\n📊 Stats: {total} analyzed | "
                  f"{anomaly_count} anomalies | "
                  f"{fixed_count} auto-fixed ✅\n")

if __name__ == "__main__":
    run_detector()