import os

DATABASE_URL = os.getenv('DATABASE_URL')
REDIS_URL = os.getenv('REDIS_URL')
AES_EVENT_STREAM = os.getenv('AES_EVENT_STREAM', 'aes.events')
