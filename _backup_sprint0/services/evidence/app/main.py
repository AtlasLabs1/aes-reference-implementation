from fastapi import FastAPI
import os, json, time
from uuid import uuid4
from datetime import datetime, timezone
import psycopg
import redis
import threading

app = FastAPI(title="AES Evidence Service", version="0.1")

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

def db(max_attempts=20, delay=2):
    last = None
    for i in range(max_attempts):
        try:
            return psycopg.connect(DATABASE_URL)
        except Exception as ex:
            last = ex
            print(f"PostgreSQL not ready. Attempt {i+1}/{max_attempts}")
            time.sleep(delay)
    raise last

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS evidences (
            evidence_id TEXT PRIMARY KEY,
            observation_id TEXT NOT NULL,
            managed_object_id TEXT NOT NULL,
            evidence_type TEXT NOT NULL,
            property TEXT NOT NULL,
            value TEXT NOT NULL,
            unit TEXT,
            confidence DOUBLE PRECISION NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        conn.commit()

def load_observation(observation_id):
    with db() as conn:
        row = conn.execute("""
            SELECT observation_id, managed_object_id, property, value, unit, timestamp
            FROM observations
            WHERE observation_id = %s
        """, (observation_id,)).fetchone()
        return row

def create_evidence_from_observation(row):
    evidence_id = "EVD-" + str(uuid4())

    observation_id, managed_object_id, prop, value, unit, ts = row

    with db() as conn:
        conn.execute("""
            INSERT INTO evidences (
                evidence_id, observation_id, managed_object_id,
                evidence_type, property, value, unit, confidence, timestamp
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            evidence_id,
            observation_id,
            managed_object_id,
            "MeasurementEvidence",
            prop,
            str(value),
            unit,
            1.0,
            ts
        ))
        conn.commit()

    return {
        "eventType": "EvidenceCreated",
        "eventVersion": "1.0",
        "evidenceId": evidence_id,
        "observationId": observation_id,
        "managedObjectId": managed_object_id,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def event_worker():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    last_id = "0-0"

    print("Evidence worker started. Listening to aes.events")

    while True:
        events = r.xread({"aes.events": last_id}, block=5000, count=10)

        for stream, messages in events:
            for message_id, fields in messages:
                last_id = message_id
                raw = fields.get("event")
                if not raw:
                    continue

                event = json.loads(raw)

                if event.get("eventType") != "ObservationCreated":
                    continue

                observation_id = event.get("observationId")
                row = load_observation(observation_id)

                if not row:
                    print(f"Observation not found: {observation_id}")
                    continue

                evidence_event = create_evidence_from_observation(row)
                r.xadd("aes.events", {"event": json.dumps(evidence_event)})
                print(f"EvidenceCreated published: {evidence_event['evidenceId']}")

@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"service": "AES Evidence Service", "status": "Healthy"}
