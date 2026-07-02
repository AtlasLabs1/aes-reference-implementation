from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone
from uuid import uuid4
import os
import json
import time
import psycopg
import redis

app = FastAPI(title="AES Observation Service", version="0.1")

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

class ObservationIn(BaseModel):
    managedObjectId: str
    property: str
    value: float | int | str | bool
    unit: str | None = None
    timestamp: datetime | None = None
    source: str | None = None

def get_db_connection_with_retry(max_attempts=20, delay_seconds=2):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg.connect(DATABASE_URL)
        except Exception as ex:
            last_error = ex
            print(f"PostgreSQL not ready yet. Attempt {attempt}/{max_attempts}. Waiting...")
            time.sleep(delay_seconds)
    raise last_error

@app.on_event("startup")
def startup():
    with get_db_connection_with_retry() as conn:
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
    observation_id = "OBS-" + str(uuid4())
    ts = payload.timestamp or datetime.now(timezone.utc)

    with get_db_connection_with_retry() as conn:
        conn.execute(
            """
            INSERT INTO observations (
                observation_id, managed_object_id, property, value, unit, timestamp, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                observation_id,
                payload.managedObjectId,
                payload.property,
                str(payload.value),
                payload.unit,
                ts,
                payload.source,
            ),
        )
        conn.commit()

    r = redis.from_url(REDIS_URL, decode_responses=True)

    event = {
        "eventType": "ObservationCreated",
        "eventVersion": "1.0",
        "observationId": observation_id,
        "managedObjectId": payload.managedObjectId,
        "property": payload.property,
        "timestamp": ts.isoformat(),
    }

    r.xadd("aes.events", {"event": json.dumps(event)})

    return {
        "observationId": observation_id,
        "status": "Accepted",
        "event": "ObservationCreated",
    }
