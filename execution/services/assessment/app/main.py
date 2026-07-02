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
    EVIDENCE_CREATED,
    ASSESSMENT_CREATED,
)
from aes_common.config import AES_EVENT_STREAM

app = FastAPI(title="AES Assessment Service", version="0.1")


def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS assessments (
            assessment_id TEXT PRIMARY KEY,
            evidence_id TEXT NOT NULL,
            managed_object_id TEXT NOT NULL,
            assessment_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            confidence DOUBLE PRECISION NOT NULL,
            summary TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        conn.commit()


def load_evidence(evidence_id):
    with db() as conn:
        return conn.execute("""
            SELECT evidence_id, managed_object_id, property, value, unit, confidence
            FROM evidences
            WHERE evidence_id = %s
        """, (evidence_id,)).fetchone()


def create_assessment(row):
    evidence_id, managed_object_id, prop, value, unit, evidence_confidence = row

    value_num = None
    try:
        value_num = float(value)
    except Exception:
        pass

    assessment_type = "NormalCondition"
    severity = "Info"
    confidence = float(evidence_confidence)
    summary = f"{prop} evidence is within normal assessment scope."

    if prop.lower() == "level" and unit == "%" and value_num is not None:
        if value_num <= 10:
            assessment_type = "CriticalLowTankLevel"
            severity = "Critical"
            summary = "Water tank level is critically low."
        elif value_num <= 30:
            assessment_type = "LowTankLevel"
            severity = "Warning"
            summary = "Water tank level is low."
        else:
            assessment_type = "NormalTankLevel"
            severity = "Info"
            summary = "Water tank level is normal."

    assessment_id = new_id("ASM")

    with db() as conn:
        conn.execute("""
            INSERT INTO assessments (
                assessment_id,
                evidence_id,
                managed_object_id,
                assessment_type,
                severity,
                confidence,
                summary
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            assessment_id,
            evidence_id,
            managed_object_id,
            assessment_type,
            severity,
            confidence,
            summary,
        ))
        conn.commit()

    event = {
        "eventType": ASSESSMENT_CREATED,
        "eventVersion": "1.0",
        "assessmentId": assessment_id,
        "evidenceId": evidence_id,
        "managedObjectId": managed_object_id,
        "assessmentType": assessment_type,
        "severity": severity,
        "confidence": confidence,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    publish_event(event)
    print(f"[Assessment Service] AssessmentCreated published: {assessment_id}")


def event_worker():
    r = redis_client()
    last_id = "0-0"

    print("[Assessment Service] Worker started. Listening to", AES_EVENT_STREAM)

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

                    if event.get("eventType") != EVIDENCE_CREATED:
                        continue

                    evidence_id = event.get("evidenceId")
                    row = load_evidence(evidence_id)

                    if not row:
                        print(f"[Assessment Service] Evidence not found: {evidence_id}")
                        continue

                    create_assessment(row)

        except Exception as ex:
            print(f"[Assessment Service] Worker error: {ex}")
            time.sleep(2)


@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {"service": "AES Assessment Service", "status": "Healthy"}
