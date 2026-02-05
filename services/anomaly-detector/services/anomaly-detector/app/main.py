from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import joblib
import os
import threading
import time

app = FastAPI(title="Anomaly Detection Service")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "logs"

# Load trained ML model
model = joblib.load("model/model.pkl")

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

            print("Anomaly detector listening to Kafka...")

            for msg in consumer:
                log = msg.value
                value = [[len(log.get("message", ""))]]
                prediction = model.predict(value)

                if prediction[0] == -1:
                    print("ANOMALY DETECTED:", log)
                else:
                    print("Normal log:", log)

        except Exception as e:
            print("Kafka error, retrying in 5 seconds:", e)
            time.sleep(5)

@app.on_event("startup")
def start_consumer():
    thread = threading.Thread(target=consume_logs, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"status": "ok"}
