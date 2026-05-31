from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="StreamSentinel API",
    description="Real-time pipeline monitoring API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────
# Load existing incident count from disk
# So cards don't reset to 0 on restart
# ─────────────────────────────────────────
def load_stats_from_logs():
    log_file = "logs/incident_reports.json"
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            try:
                data = json.load(f)
                return len(data)
            except:
                return 0
    return 0

def load_orders_from_logs():
    log_file = "logs/order_count.json"
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            try:
                data = json.load(f)
                return data.get("total_orders", 0)
            except:
                return 0
    return 0

def save_order_count(count):
    os.makedirs("logs", exist_ok=True)
    with open("logs/order_count.json", "w") as f:
        json.dump({"total_orders": count}, f)

# ─────────────────────────────────────────
# Initialize stats from disk on startup
# Cards now match table on every restart!
# ─────────────────────────────────────────
initial_anomalies = load_stats_from_logs()
initial_orders    = load_orders_from_logs()

stats = {
    "total_orders":    initial_orders,
    "total_anomalies": initial_anomalies,
    "total_fixed":     initial_anomalies,
    "pipeline_status": "healthy",
    "last_updated":    datetime.now().isoformat()
}

incidents = []

# ─────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────
@app.get("/")
def root():
    return {
        "message": "StreamSentinel API is running!",
        "status":  "healthy",
        "version": "1.0.0"
    }

@app.get("/stats")
def get_stats():
    return stats

@app.get("/incidents")
def get_incidents():
    log_file = "logs/incident_reports.json"
    if not os.path.exists(log_file):
        return {"incidents": [], "total": 0}
    with open(log_file, "r") as f:
        try:
            data = json.load(f)
        except:
            data = []
    return {
        "incidents": data[-50:],
        "total":     len(data)
    }

@app.get("/health")
def get_health():
    total     = stats["total_orders"]
    anomalies = stats["total_anomalies"]

    if total == 0:
        anomaly_rate = 0
        score        = 100
    else:
        anomaly_rate = anomalies / total
        score        = max(0, int(100 - (anomaly_rate * 100 * 5)))

    return {
        "health_score": score,
        "status":       "healthy" if score > 70 else "warning" if score > 40 else "critical",
        "total_orders": total,
        "anomaly_rate": f"{anomaly_rate*100:.1f}%" if total > 0 else "0%",
        "last_updated": stats["last_updated"]
    }

@app.post("/report-anomaly")
def report_anomaly(data: dict):
    global stats, incidents

    stats["total_anomalies"] += 1
    stats["total_fixed"]     += 1
    stats["last_updated"]     = datetime.now().isoformat()

    total     = stats["total_orders"]
    anomalies = stats["total_anomalies"]
    if total > 0 and anomalies / total > 0.2:
        stats["pipeline_status"] = "critical"
    else:
        stats["pipeline_status"] = "healthy"

    incidents.append({
        "timestamp":      datetime.now().isoformat(),
        "anomaly_reason": data.get("anomaly_reason"),
        "order_id":       data.get("order_id"),
        "was_fixed":      True
    })

    return {"message": "Anomaly recorded", "status": "ok"}

@app.post("/report-order")
def report_order():
    global stats
    stats["total_orders"] += 1
    stats["last_updated"]  = datetime.now().isoformat()

    # ── Save order count to disk so it persists ──
    save_order_count(stats["total_orders"])

    return {"message": "Order counted", "status": "ok"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("📡 Dashboard connected via WebSocket!")
    try:
        while True:
            await websocket.send_json({
                "type": "stats_update",
                "data": stats,
                "time": datetime.now().isoformat()
            })
            await asyncio.sleep(2)
    except:
        print("📡 Dashboard disconnected")