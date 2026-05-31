import json
import os
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# Connect to Kafka so we can send
# fixed orders back into the pipeline
# ─────────────────────────────────────────
def get_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv(
            'KAFKA_BROKER', 'localhost:9092'
        ),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

# ─────────────────────────────────────────
# FIX 1: Missing fields
# Like filling in a form someone left blank
# ─────────────────────────────────────────
def fix_missing_fields(order):
    fixed = order.copy()

    if "amount" not in fixed:
        fixed["amount"] = 500.0
        print(f"   🔧 Fixed: added default amount ₹500")

    if "customer_name" not in fixed:
        fixed["customer_name"] = "Unknown Customer"
        print(f"   🔧 Fixed: added default customer name")

    if "restaurant" not in fixed:
        fixed["restaurant"] = "Unknown Restaurant"
        print(f"   🔧 Fixed: added default restaurant")

    if "status" not in fixed:
        fixed["status"] = "placed"
        print(f"   🔧 Fixed: added default status")

    if "city" not in fixed:
        fixed["city"] = "Unknown"
        print(f"   🔧 Fixed: added default city")

    fixed["remediation"] = "missing_fields_filled"
    return fixed

# ─────────────────────────────────────────
# FIX 2: Suspicious amount
# Like a bank refusing a ₹99,99,999 payment
# and replacing with a safe maximum
# ─────────────────────────────────────────
def fix_suspicious_amount(order):
    fixed      = order.copy()
    MAX_AMOUNT = 2000.0
    MIN_AMOUNT = 50.0

    original = fixed.get("amount", 0)

    if fixed.get("amount", 0) > MAX_AMOUNT:
        fixed["amount"] = MAX_AMOUNT
        print(f"   🔧 Fixed: amount ₹{original} → capped to ₹{MAX_AMOUNT}")

    elif fixed.get("amount", 0) < MIN_AMOUNT:
        fixed["amount"] = MIN_AMOUNT
        print(f"   🔧 Fixed: amount ₹{original} → raised to ₹{MIN_AMOUNT}")

    fixed["remediation"] = "amount_normalized"
    return fixed

# ─────────────────────────────────────────
# FIX 3: Wrong timestamp
# Like replacing a broken date with today
# ─────────────────────────────────────────
def fix_invalid_timestamp(order):
    fixed = order.copy()
    fixed["timestamp"]   = datetime.now().isoformat()
    fixed["remediation"] = "timestamp_corrected"
    print(f"   🔧 Fixed: replaced broken timestamp with current time")
    return fixed

# ─────────────────────────────────────────
# FIX 4: Unknown pattern
# We don't know what's wrong
# so we quarantine it for human review
# ─────────────────────────────────────────
def quarantine_order(order, producer):
    order["remediation"]    = "quarantined"
    order["quarantine_reason"] = "unusual_pattern"
    order["quarantined_at"] = datetime.now().isoformat()

    # Send to a separate "quarantine" topic
    producer.send("quarantine_stream", value=order)
    producer.flush()
    print(f"   🔧 Fixed: sent to quarantine topic for review")
    return order

# ─────────────────────────────────────────
# MAIN FIX FUNCTION
# Decides which fix to apply based on
# what the detector found wrong
# ─────────────────────────────────────────
def remediate(detection_result, producer):
    order  = detection_result["original_order"]
    reason = detection_result.get("anomaly_reason", "unknown")

    print(f"\n🔧 AUTO-REMEDIATING: {reason}")
    print(f"   Order ID: {order.get('order_id','?')[:8]}")

    # Choose the right fix
    if reason == "missing_fields":
        fixed = fix_missing_fields(order)

    elif reason == "suspicious_amount":
        fixed = fix_suspicious_amount(order)

    elif reason == "invalid_timestamp":
        fixed = fix_invalid_timestamp(order)

    else:
        fixed = quarantine_order(order, producer)
        print(f"   ✅ Quarantined successfully!\n")
        return fixed

    # Mark order as remediated
    fixed["is_anomaly"]     = False
    fixed["was_remediated"] = True
    fixed["remediated_at"]  = datetime.now().isoformat()

    # Send FIXED order back into the pipeline!
    producer.send(
        os.getenv('KAFKA_TOPIC_ORDERS', 'orders_stream'),
        value=fixed
    )
    producer.flush()

    print(f"   ✅ Fixed order sent back to pipeline!\n")
    return fixed