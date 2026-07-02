from fastapi import FastAPI
from datetime import datetime, timezone
import json
import threading
import time
import os
import yaml

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


def load_water_dkm():
    path = os.getenv("DKM_WATER_PATH", "/app/dkms/water/v0.1/water.dkm.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


DKM = None


def load_evidence(evidence_id):
    with db() as conn:
        return conn.execute("""
            SELECT evidence_id, managed_object_id, property, value, unit, confidence
            FROM evidences
            WHERE evidence_id = %s
        """, (evidence_id,)).fetchone()


def compare(operator, left, right):
    if operator == "less_or_equal":
        return left <= right
    if operator == "less_than":
        return left < right
    if operator == "greater_or_equal":
        return left >= right
    if operator == "greater_than":
        return left > right
    if operator == "equal":
        return left == right
    return False


def evaluate_dkm(evidence):
    evidence_id, managed_object_id, prop, value, unit, evidence_confidence = evidence

    try:
        value_num = float(value)
    except Exception:
        value_num = value

    for rule in DKM.get("rules", []):
        match = rule.get("match", {})

        if match.get("property") != prop:
            continue

        if match.get("unit") != unit:
            continue

        operator = match.get("operator")
        target = match.get("value")

        if compare(operator, value_num, target):
            assessment = rule["assessment"]

            return {
                "ruleId": rule["id"],
                "assessmentType": assessment["type"],
                "severity": assessment["severity"],
                "summary": assessment["summary"],
                "confidence": float(assessment.get("confidence", evidence_confidence)),
            }

    return {
        "ruleId": "NO_MATCH",
        "assessmentType": "NoAssessment",
        "severity": "Info",
        "summary": "No DKM rule matched this evidence.",
        "confidence": 0.0,
    }


def create_assessment(evidence):
    evidence_id, managed_object_id, prop, value, unit, evidence_confidence = evidence
    result = evaluate_dkm(evidence)

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
            result["assessmentType"],
            result["severity"],
            result["confidence"],
            result["summary"],
        ))
        conn.commit()

    event = {
        "eventType": ASSESSMENT_CREATED,
        "eventVersion": "1.0",
        "assessmentId": assessment_id,
        "evidenceId": evidence_id,
        "managedObjectId": managed_object_id,
        "assessmentType": result["assessmentType"],
        "severity": result["severity"],
        "confidence": result["confidence"],
        "ruleId": result["ruleId"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    publish_event(event)
    print(f"[Assessment Service] AssessmentCreated published: {assessment_id} using rule {result['ruleId']}")


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
                    evidence = load_evidence(evidence_id)

                    if not evidence:
                        print(f"[Assessment Service] Evidence not found: {evidence_id}")
                        continue

                    create_assessment(evidence)

        except Exception as ex:
            print(f"[Assessment Service] Worker error: {ex}")
            time.sleep(2)


@app.on_event("startup")
def startup():
    global DKM
    init_db()
    DKM = load_water_dkm()
    print(f"[Assessment Service] Loaded DKM: {DKM['dkm']['name']} v{DKM['dkm']['version']}")
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {"service": "AES Assessment Service", "status": "Healthy", "dkm": DKM["dkm"] if DKM else None}
