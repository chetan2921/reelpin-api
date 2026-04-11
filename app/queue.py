import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AgeLimit, Retries, TimeLimit
import os
import re

def _normalize_redis_url(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        return "redis://localhost:6379/0"

    # Accept a pasted Upstash CLI command and extract the actual connection URL.
    if value.startswith("redis-cli"):
        match = re.search(r"-u\s+(\S+)", value)
        if match:
            value = match.group(1)

    # Upstash connections require TLS; upgrade scheme automatically if needed.
    if value.startswith("redis://") and "upstash.io" in value:
        value = "rediss://" + value[len("redis://"):]

    return value


REDIS_URL = _normalize_redis_url(os.getenv("REDIS_URL"))
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
