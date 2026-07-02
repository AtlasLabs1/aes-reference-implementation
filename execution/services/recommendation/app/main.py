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
    ASSESSMENT_CREATED,
    RECOMMENDATION_CREATED,
)
from aes_common.config import AES_EVENT_STREAM

app = FastAPI(title="AES Recommendation Service", version="0.1")


def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            recommendation_id TEXT PRIMARY KEY,
            assessment_id TEXT NOT NULL,
            managed_object_id TEXT NOT NULL,
            recommendation_type TEXT NOT NULL,
            priority TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        conn.commit()


def load_water_dkm():
    path = os.getenv("DKM_WATER_PATH", "/app/dkms/water/v0.1/water.dkm.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


DKM = None


def load_assessment(assessment_id):
    with db() as conn:
        return conn.execute("""
            SELECT assessment_id, managed_object_id, assessment_type, severity
            FROM assessments
            WHERE assessment_id = %s
        """, (assessment_id,)).fetchone()


def find_recommendation_for_assessment(assessment_type):
    for rule in DKM.get("rules", []):
        assessment = rule.get("assessment", {})

        if assessment.get("type") == assessment_type:
            recommendation = rule.get("recommendation")

            if recommendation:
                return {
                    "ruleId": rule["id"],
                    "recommendationType": recommendation["type"],
                    "priority": recommendation["priority"],
                    "message": recommendation["message"],
                }

    return {
        "ruleId": "NO_MATCH",
        "recommendationType": "NoRecommendation",
        "priority": "Info",
        "message": "No DKM recommendation matched this assessment.",
    }


def create_recommendation(assessment):
    assessment_id, managed_object_id, assessment_type, severity = assessment

    result = find_recommendation_for_assessment(assessment_type)

    recommendation_id = new_id("REC")

    with db() as conn:
        conn.execute("""
            INSERT INTO recommendations (
                recommendation_id,
                assessment_id,
                managed_object_id,
                recommendation_type,
                priority,
                message
            )
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            recommendation_id,
            assessment_id,
            managed_object_id,
            result["recommendationType"],
            result["priority"],
            result["message"],
        ))
        conn.commit()

    event = {
        "eventType": RECOMMENDATION_CREATED,
        "eventVersion": "1.0",
        "recommendationId": recommendation_id,
        "assessmentId": assessment_id,
        "managedObjectId": managed_object_id,
        "recommendationType": result["recommendationType"],
        "priority": result["priority"],
        "ruleId": result["ruleId"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    publish_event(event)
    print(f"[Recommendation Service] RecommendationCreated published: {recommendation_id} using rule {result['ruleId']}")


def event_worker():
    r = redis_client()
    last_id = "0-0"

    print("[Recommendation Service] Worker started. Listening to", AES_EVENT_STREAM)

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

                    if event.get("eventType") != ASSESSMENT_CREATED:
                        continue

                    assessment_id = event.get("assessmentId")
                    assessment = load_assessment(assessment_id)

                    if not assessment:
                        print(f"[Recommendation Service] Assessment not found: {assessment_id}")
                        continue

                    create_recommendation(assessment)

        except Exception as ex:
            print(f"[Recommendation Service] Worker error: {ex}")
            time.sleep(2)


@app.on_event("startup")
def startup():
    global DKM
    init_db()
    DKM = load_water_dkm()
    print(f"[Recommendation Service] Loaded DKM: {DKM['dkm']['name']} v{DKM['dkm']['version']}")
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {
        "service": "AES Recommendation Service",
        "status": "Healthy",
        "dkm": DKM["dkm"] if DKM else None
    }
