from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import os

# -------------------------
# App initialization
# -------------------------
app = FastAPI(title="Log Ingestor Service")

# -------------------------
# Kafka configuration
# -------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "logs"

producer = None

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Kafka producer initialized")
except Exception as e:
    print("Kafka not available. Running without Kafka.")
    print(str(e))

# -------------------------
# Request schema
# -------------------------
class LogEvent(BaseModel):
    service: str
    level: str
    message: str

# -------------------------
# Health check endpoint
# -------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -------------------------
# Log ingestion endpoint
# -------------------------
@app.post("/logs")
def ingest_log(log: LogEvent):
    log_data = log.dict()

    if producer:
        try:
            producer.send(TOPIC, log_data)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to send log to Kafka: {e}"
            )

    return {
        "status": "accepted",
        "log": log_data
    }
