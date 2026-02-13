from fastapi import FastAPI
from pydantic import BaseModel
import datetime

app = FastAPI(title="Alert Service")

class Alert(BaseModel):
    service: str
    level: str
    message: str

@app.post("/alert")
def receive_alert(alert: Alert):
    timestamp = datetime.datetime.utcnow().isoformat()
    print("🚨 ALERT RECEIVED 🚨")
    print("Time:", timestamp)
    print("Service:", alert.service)
    print("Level:", alert.level)
    print("Message:", alert.message)
    return {"status": "alert received", "time": timestamp}

@app.get("/health")
def health():
    return {"status": "ok"}

