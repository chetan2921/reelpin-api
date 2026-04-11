import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AgeLimit, Retries, TimeLimit

from app.config import get_settings

settings = get_settings()

broker = RedisBroker(url=settings.REDIS_URL)
broker.add_middleware(AgeLimit())
broker.add_middleware(
    Retries(
        max_retries=settings.JOB_MAX_RETRIES,
        min_backoff=settings.JOB_MIN_BACKOFF_MS,
        max_backoff=settings.JOB_MAX_BACKOFF_MS,
    )
)
broker.add_middleware(TimeLimit())

dramatiq.set_broker(broker)
