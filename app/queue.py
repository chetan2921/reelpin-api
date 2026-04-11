import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AgeLimit, Retries, TimeLimit
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
JOB_MAX_RETRIES = int(os.getenv("JOB_MAX_RETRIES", "3"))
JOB_MIN_BACKOFF_MS = int(os.getenv("JOB_MIN_BACKOFF_MS", "15000"))
JOB_MAX_BACKOFF_MS = int(os.getenv("JOB_MAX_BACKOFF_MS", "300000"))

broker = RedisBroker(url=REDIS_URL)
broker.add_middleware(AgeLimit())
broker.add_middleware(
    Retries(
        max_retries=JOB_MAX_RETRIES,
        min_backoff=JOB_MIN_BACKOFF_MS,
        max_backoff=JOB_MAX_BACKOFF_MS,
    )
)
broker.add_middleware(TimeLimit())

dramatiq.set_broker(broker)
