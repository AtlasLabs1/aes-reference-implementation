import time
import psycopg
from aes_common.config import DATABASE_URL

def db(max_attempts=20, delay=2):
    last = None
    for i in range(max_attempts):
        try:
            return psycopg.connect(DATABASE_URL)
        except Exception as ex:
            last = ex
            print(f'PostgreSQL not ready. Attempt {i+1}/{max_attempts}')
            time.sleep(delay)
    raise last
