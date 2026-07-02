from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone
from aes_common.database import db
from aes_common.ids import new_id
from aes_common.events import publish_event, OBSERVATION_CREATED

app = FastAPI(title="AES Observation Service", version="0.1")

class ObservationIn(BaseModel):
    managedObjectId: str
    property: str
    value: float | int | str | bool
    unit: str | None = None
    timestamp: datetime | None = None
    source: str | None = None

@app.on_event("startup")
def startup():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            observation_id TEXT PRIMARY KEY,
            managed_object_id TEXT NOT NULL,
            property TEXT NOT NULL,
            value TEXT NOT NULL,
            unit TEXT,
            timestamp TIMESTAMPTZ NOT NULL,
            source TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        conn.commit()

@app.get("/health")
def health():
    return {"service": "AES Observation Service", "status": "Healthy"}

@app.post("/api/v1/observations", status_code=202)
def create_observation(payload: ObservationIn):
    observation_id = new_id("OBS")
    ts = payload.timestamp or datetime.now(timezone.utc)

    with db() as conn:
        conn.execute("""
            INSERT INTO observations (
                observation_id, managed_object_id, property, value, unit, timestamp, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            observation_id,
            payload.managedObjectId,
            payload.property,
            str(payload.value),
            payload.unit,
            ts,
            payload.source,
        ))
        conn.commit()

    event = {
        "eventType": OBSERVATION_CREATED,
        "eventVersion": "1.0",
        "observationId": observation_id,
        "managedObjectId": payload.managedObjectId,
        "property": payload.property,
        "timestamp": ts.isoformat(),
    }

    publish_event(event)

    return {
        "observationId": observation_id,
        "status": "Accepted",
        "event": OBSERVATION_CREATED,
    }
