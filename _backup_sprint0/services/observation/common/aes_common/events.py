import json
import redis
from aes_common.config import REDIS_URL, AES_EVENT_STREAM

OBSERVATION_CREATED = 'ObservationCreated'
EVIDENCE_CREATED = 'EvidenceCreated'
ASSESSMENT_CREATED = 'AssessmentCreated'
RECOMMENDATION_CREATED = 'RecommendationCreated'

def redis_client():
    return redis.from_url(REDIS_URL, decode_responses=True)

def publish_event(event: dict):
    r = redis_client()
    r.xadd(AES_EVENT_STREAM, {'event': json.dumps(event)})
