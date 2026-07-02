from fastapi import FastAPI
from datetime import datetime, timezone
import json
import threading
import time

from aes_common.database import db
from aes_common.ids import new_id
from aes_common.events import (
    redis_client,
    publish_event,
    OBSERVATION_CREATED,
    EVIDENCE_CREATED,
)
from aes_common.config import AES_EVENT_STREAM

app = FastAPI(title="AES Evidence Service", version="0.1")


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
        return conn.execute("""
            SELECT observation_id, managed_object_id, property, value, unit, timestamp
            FROM observations
            WHERE observation_id = %s
        """, (observation_id,)).fetchone()


def create_evidence(row):
    evidence_id = new_id("EVD")
    observation_id, managed_object_id, prop, value, unit, ts = row

    with db() as conn:
        conn.execute("""
            INSERT INTO evidences (
                evidence_id,
                observation_id,
                managed_object_id,
                evidence_type,
                property,
                value,
                unit,
                confidence,
                timestamp
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
            ts,
        ))
        conn.commit()

    event = {
        "eventType": EVIDENCE_CREATED,
        "eventVersion": "1.0",
        "evidenceId": evidence_id,
        "observationId": observation_id,
        "managedObjectId": managed_object_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    publish_event(event)
    print(f"[Evidence Service] EvidenceCreated published: {evidence_id}")


def event_worker():
    r = redis_client()
    last_id = "0-0"

    print("[Evidence Service] Worker started. Listening to", AES_EVENT_STREAM)

    while True:
        try:
            events = r.xread({AES_EVENT_STREAM: last_id}, block=5000, count=10)

            for stream, messages in events:
                for message_id, fields in messages:
                    last_id = message_id
                    raw = fields.get("event")

                    if not raw:
                        continue

                    event = json.loads(raw)

                    if event.get("eventType") != OBSERVATION_CREATED:
                        continue

                    observation_id = event.get("observationId")
                    row = load_observation(observation_id)

                    if not row:
                        print(f"[Evidence Service] Observation not found: {observation_id}")
                        continue

                    create_evidence(row)

        except Exception as ex:
            print(f"[Evidence Service] Worker error: {ex}")
            time.sleep(2)


@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {"service": "AES Evidence Service", "status": "Healthy"}
