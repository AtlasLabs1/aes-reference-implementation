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
            trace_ref TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        conn.commit()


def load_assessment(assessment_id):
    with db() as conn:
        return conn.execute("""
            SELECT assessment_id, evidence_id, managed_object_id,
                   assessment_type, severity, confidence, summary
            FROM assessments
            WHERE assessment_id = %s
        """, (assessment_id,)).fetchone()


def create_recommendation(row):
    assessment_id, evidence_id, managed_object_id, assessment_type, severity, confidence, summary = row

    recommendation_type = "NoActionRequired"
    priority = "Info"
    message = "No action required. Continue monitoring."

    if assessment_type == "CriticalLowTankLevel":
        recommendation_type = "ImmediateWaterSupplyAction"
        priority = "Critical"
        message = "Water tank level is critically low. Refill immediately or start available pump if safe."
    elif assessment_type == "LowTankLevel":
        recommendation_type = "PlanWaterRefill"
        priority = "Warning"
        message = "Water tank level is low. Plan refill or verify water supply availability."
    elif assessment_type == "NormalTankLevel":
        recommendation_type = "ContinueMonitoring"
        priority = "Info"
        message = "Water tank level is normal. Continue monitoring."

    recommendation_id = new_id("REC")

    with db() as conn:
        conn.execute("""
            INSERT INTO recommendations (
                recommendation_id,
                assessment_id,
                managed_object_id,
                recommendation_type,
                priority,
                message,
                trace_ref
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            recommendation_id,
            assessment_id,
            managed_object_id,
            recommendation_type,
            priority,
            message,
            evidence_id
        ))
        conn.commit()

    event = {
        "eventType": RECOMMENDATION_CREATED,
        "eventVersion": "1.0",
        "recommendationId": recommendation_id,
        "assessmentId": assessment_id,
        "managedObjectId": managed_object_id,
        "recommendationType": recommendation_type,
        "priority": priority,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    publish_event(event)
    print(f"[Recommendation Service] RecommendationCreated published: {recommendation_id}")


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
                    row = load_assessment(assessment_id)

                    if not row:
                        print(f"[Recommendation Service] Assessment not found: {assessment_id}")
                        continue

                    create_recommendation(row)

        except Exception as ex:
            print(f"[Recommendation Service] Worker error: {ex}")
            time.sleep(2)


@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=event_worker, daemon=True)
    thread.start()


@app.get("/health")
def health():
    return {"service": "AES Recommendation Service", "status": "Healthy"}
