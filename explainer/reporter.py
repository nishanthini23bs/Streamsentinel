import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────
# This is our "spokesperson"
# Takes technical error → writes plain English
# Like a translator between engineers and managers
# ─────────────────────────────────────────

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"  # updated model!
# ─────────────────────────────────────────
# Build the prompt we send to the LLM
# We give it all the context it needs
# ─────────────────────────────────────────
def build_prompt(detection_result):
    order  = detection_result.get("original_order", {})
    reason = detection_result.get("anomaly_reason", "unknown")
    score  = detection_result.get("anomaly_score", 0)

    return f"""
You are a data pipeline monitoring assistant for StreamSentinel.
An anomaly was detected in a food delivery order pipeline.

ANOMALY DETAILS:
- Order ID      : {order.get('order_id', 'unknown')[:8]}
- Restaurant    : {order.get('restaurant', 'unknown')}
- Amount        : ₹{order.get('amount', 'missing')}
- City          : {order.get('city', 'unknown')}
- Timestamp     : {order.get('timestamp', 'missing')}
- Anomaly Type  : {reason}
- Anomaly Score : {score}
- Detected At   : {detection_result.get('detected_at', 'unknown')}

Write a SHORT incident report (3-4 sentences) in plain English that:
1. States what went wrong in simple terms
2. Explains the potential business impact
3. States what StreamSentinel did to fix it automatically
4. Gives a confidence level (High/Medium/Low)

Keep it simple enough for a non-technical business manager to understand.
Do NOT use technical jargon. Be direct and concise.
"""

# ─────────────────────────────────────────
# Call the Groq LLM API
# Like asking a smart assistant to explain
# ─────────────────────────────────────────
def call_llm(prompt):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "⚠️ No API key found. Add GROQ_API_KEY to .env file"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json"
    }

    payload = {
        "model":    GROQ_MODEL,
        "messages": [
            {
                "role":    "system",
                "content": "You are a helpful data pipeline monitoring assistant. Write clear, concise incident reports."
            },
            {
                "role":    "user",
                "content": prompt
            }
        ],
        "max_tokens":  200,
        "temperature": 0.3
    }

    try:
        response = requests.post(
            GROQ_API_URL,
            headers=headers,
            json=payload,
            timeout=10
        )
        data = response.json()
        print(f"🔍 Raw API response: {data}")
        if "choices" in data:
            return data["choices"][0]["message"]["content"]
        else:
            return f"⚠️ API Error: {data.get('error', data)}"
    except Exception as e:
        return f"⚠️ LLM call failed: {e}"

# ─────────────────────────────────────────
# Save report to a log file
# So we have history of all incidents
# ─────────────────────────────────────────
def save_report(report, detection_result):
    os.makedirs("logs", exist_ok=True)
    log_file = "logs/incident_reports.json"

    entry = {
        "timestamp":      datetime.now().isoformat(),
        "order_id":       detection_result.get("order_id", "?"),
        "anomaly_reason": detection_result.get("anomaly_reason"),
        "anomaly_score":  detection_result.get("anomaly_score"),
        "llm_report":     report
    }

    # Load existing logs
    existing = []
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            try:
                existing = json.load(f)
            except:
                existing = []

    # Add new entry
    existing.append(entry)

    # Save back
    with open(log_file, "w") as f:
        json.dump(existing, f, indent=2)

# ─────────────────────────────────────────
# MAIN FUNCTION
# Called by detector when anomaly is found
# ─────────────────────────────────────────
def generate_report(detection_result):
    print(f"\n📝 Generating incident report...")

    prompt = build_prompt(detection_result)
    report = call_llm(prompt)

    print(f"\n{'='*50}")
    print(f"📋 INCIDENT REPORT")
    print(f"{'='*50}")
    print(report)
    print(f"{'='*50}\n")

    save_report(report, detection_result)
    return report


# ─────────────────────────────────────────
# Test it standalone — run this file alone
# to verify LLM connection works
# ─────────────────────────────────────────
if __name__ == "__main__":
    test_detection = {
        "order_id":       "test-1234-abcd",
        "is_anomaly":     True,
        "anomaly_score":  -0.8123,
        "anomaly_reason": "suspicious_amount",
        "detected_at":    datetime.now().isoformat(),
        "original_order": {
            "order_id":      "test-1234-abcd",
            "restaurant":    "Dominos",
            "amount":        999999,
            "city":          "Chennai",
            "timestamp":     datetime.now().isoformat(),
            "customer_name": "Test User"
        }
    }

    print("🧪 Testing LLM Explainer...")
    report = generate_report(test_detection)
    print("✅ Test complete!")