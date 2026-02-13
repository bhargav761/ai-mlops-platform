from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest
import json
import os
import time
import logging

# --------------------------------------------------
# Logging setup (production style)
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("log-ingestor")

# --------------------------------------------------
# App initialization
# --------------------------------------------------
app = FastAPI(title="Log Ingestor Service")

# --------------------------------------------------
# Kafka configuration
# --------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "logs"

producer = None

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("Kafka producer initialized")
except Exception as e:
    logger.warning("Kafka not available. Running without Kafka.")
    logger.warning(str(e))

# --------------------------------------------------
# Prometheus Metrics (Production Grade)
# --------------------------------------------------

# Total logs received
LOGS_RECEIVED = Counter(
    "logs_received_total",
    "Total number of logs received"
)

# Logs successfully sent to Kafka
LOGS_PROCESSED = Counter(
    "logs_processed_total",
    "Total number of logs successfully processed"
)

# Failed log attempts
LOGS_FAILED = Counter(
    "logs_failed_total",
    "Total number of logs failed to process"
)

# Request processing time
REQUEST_LATENCY = Histogram(
    "log_ingest_latency_seconds",
    "Time taken to process log ingestion request"
)

# --------------------------------------------------
# Request schema
# --------------------------------------------------
class LogEvent(BaseModel):
    service: str
    level: str
    message: str

# --------------------------------------------------
# Health check endpoint
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# --------------------------------------------------
# Prometheus metrics endpoint
# --------------------------------------------------
@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")

# --------------------------------------------------
# Log ingestion endpoint
# --------------------------------------------------
@app.post("/logs")
def ingest_log(log: LogEvent):

    start_time = time.time()
    LOGS_RECEIVED.inc()

    log_data = log.dict()

    if producer:
        try:
            producer.send(TOPIC, log_data)
            LOGS_PROCESSED.inc()
            logger.info(f"Log sent to Kafka: {log_data}")
        except Exception as e:
            LOGS_FAILED.inc()
            logger.error(f"Kafka error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to send log to Kafka: {e}"
            )
    else:
        LOGS_FAILED.inc()
        logger.warning("Producer not available. Log not sent.")

    duration = time.time() - start_time
    REQUEST_LATENCY.observe(duration)

    return {
        "status": "accepted",
        "log": log_data
    }

