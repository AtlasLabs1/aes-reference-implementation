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

DKM_PATH = os.getenv("AES_DKM_PATH", "/app/dkms/water-system")
DKM_RULES = []


def load_dkm_rules():
    global DKM_RULES

    rules_file = os.path.join(DKM_PATH, "assessment_rules.yaml")

    with open(rules_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    DKM_RULES = data.get("assessment_rules", [])

    print(f"[Assessment Service] Loaded {len(DKM_RULES)} assessment rules from DKM: {DKM_PATH}")


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
            rule_id TEXT,
            dkm_id TEXT,
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


def condition_matches(operator, left, right):
    if operator == "<=":
        return left <= right
    if operator == "<":
        return left < right
    if operator == ">=":
        return left >= right
    if operator == ">":
        return left > right
    if operator == "==":
        return left == right
    return False


def find_matching_rule(prop, unit, value):
    try:
        value_num = float(value)
    except Exception:
        value_num = value

    for rule in DKM_RULES:
        applies = rule.get("applies_to", {})
        condition = rule.get("condition", {})

        rule_prop = str(applies.get("property", "")).lower()
        rule_unit = applies.get("unit")

        if rule_prop != str(prop).lower():
            continue

        if rule_unit != unit:
            continue

        operator = condition.get("operator")
        rule_value = condition.get("value")

        if isinstance(value_num, (float, int)):
            rule_value = float(rule_value)

        if condition_matches(operator, value_num, rule_value):
            return rule

    return None


def create_assessment(row):
    evidence_id, managed_object_id, prop, value, unit, evidence_confidence = row

    rule = find_matching_rule(prop, unit, value)

    if rule:
        output = rule.get("output", {})
        assessment_type = output.get("assessment_type", "UnknownAssessment")
        severity = output.get("severity", "Info")
        confidence = float(output.get("confidence", evidence_confidence))
        summary = output.get("summary", "Assessment generated from DKM rule.")
        rule_id = rule.get("id")
        dkm_id = "water-system"
    else:
        assessment_type = "NoMatchingAssessmentRule"
        severity = "Info"
        confidence = float(evidence_confidence)
        summary = "No matching assessment rule found in active DKM."
        rule_id = None
        dkm_id = "water-system"

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
                summary,
                rule_id,
                dkm_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            assessment_id,
            evidence_id,
            managed_object_id,
            assessment_type,
            severity,
            confidence,
            summary,
            rule_id,
            dkm_id,
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
        "ruleId": rule_id,
        "dkmId": dkm_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    publish_event(event)
    print(f"[Assessment Service] AssessmentCreated published: {assessment_id} using rule {rule_id}")


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
    load_dkm_rules()
    init_db()
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {
        "service": "AES Assessment Service",
        "status": "Healthy",
        "dkmPath": DKM_PATH,
        "rulesLoaded": len(DKM_RULES),
    }
