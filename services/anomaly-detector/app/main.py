from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import joblib
import threading
import os
import time
import requests

# =========================
# App setup
# =========================
app = FastAPI(title="Anomaly Detection Service")

# =========================
# Kafka config
# =========================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "logs"

# =========================
# Alert service config
# =========================
ALERT_SERVICE_URL = os.getenv(
    "ALERT_SERVICE_URL",
    "http://alert-service:8002/alert"
)

# =========================
# Load ML model (SAFE PATH)
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(BASE_DIR, "model", "model.pkl")

model = joblib.load(MODEL_PATH)
print("✅ ML model loaded successfully")

# =========================
# Kafka consumer logic
# =========================
def consume_logs():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="anomaly-detector"
            )

            print("🚀 Anomaly detector listening to Kafka...")

            for msg in consumer:
                log = msg.value

                # Simple numeric feature: message length
                value = [[len(log.get("message", ""))]]
                prediction = model.predict(value)

                if prediction[0] == -1:
                    # SEND TO ALERT SERVICE
                    requests.post(ALERT_SERVICE_URL, json=log)
                    print("🚨 ANOMALY SENT TO ALERT SERVICE:", log)
                else:
                    print("✅ Normal log:", log)

        except Exception as e:
            print("⚠️ Kafka error, retrying in 5s:", e)
            time.sleep(5)

# =========================
# Start consumer on startup
# =========================
@app.on_event("startup")
def start_consumer():
    thread = threading.Thread(target=consume_logs, daemon=True)
    thread.start()

# =========================
# Health check
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

