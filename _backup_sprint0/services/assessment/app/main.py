from fastapi import FastAPI
import os, json, time, threading
from uuid import uuid4
from datetime import datetime, timezone
import psycopg
import redis

app = FastAPI(title="AES Assessment Service", version="0.1")

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
            SELECT evidence_id, observation_id, managed_object_id, property, value, unit, confidence, timestamp
            FROM evidences
            WHERE evidence_id = %s
        """, (evidence_id,)).fetchone()

def assess(row):
    evidence_id, observation_id, managed_object_id, prop, value, unit, evidence_confidence, ts = row
    value_num = None
    try:
        value_num = float(value)
    except:
        pass

    assessment_type = "NormalCondition"
    severity = "Info"
    confidence = float(evidence_confidence)
    summary = f"{prop} observation is within normal assessment scope."

    # First reference rule: water tank level
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

    assessment_id = "ASM-" + str(uuid4())

    with db() as conn:
        conn.execute("""
            INSERT INTO assessments (
                assessment_id, evidence_id, managed_object_id,
                assessment_type, severity, confidence, summary
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            assessment_id,
            evidence_id,
            managed_object_id,
            assessment_type,
            severity,
            confidence,
            summary
        ))
        conn.commit()

    return {
        "eventType": "AssessmentCreated",
        "eventVersion": "1.0",
        "assessmentId": assessment_id,
        "evidenceId": evidence_id,
        "managedObjectId": managed_object_id,
        "assessmentType": assessment_type,
        "severity": severity,
        "confidence": confidence,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def event_worker():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    last_id = "0-0"

    print("Assessment worker started. Listening to aes.events")

    while True:
        events = r.xread({"aes.events": last_id}, block=5000, count=10)

        for stream, messages in events:
            for message_id, fields in messages:
                last_id = message_id
                raw = fields.get("event")
                if not raw:
                    continue

                event = json.loads(raw)

                if event.get("eventType") != "EvidenceCreated":
                    continue

                evidence_id = event.get("evidenceId")
                row = load_evidence(evidence_id)

                if not row:
                    print(f"Evidence not found: {evidence_id}")
                    continue

                assessment_event = assess(row)
                r.xadd("aes.events", {"event": json.dumps(assessment_event)})
                print(f"AssessmentCreated published: {assessment_event['assessmentId']}")

@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()

@app.get("/health")
def health():
    return {"service": "AES Assessment Service", "status": "Healthy"}
