import asyncio
import logging
import time
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import urlparse

from app.config import get_settings
from app.models import ProcessingJobStatus
from app.pipeline import process_reel_pipeline_with_metrics
from app.services.database import (
    claim_next_processing_job,
    recover_stale_processing_jobs,
    update_processing_job,
)

logger = logging.getLogger(__name__)
settings = get_settings()


def _derive_platform(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if "instagram" in host:
        return "instagram"
    if "tiktok" in host:
        return "tiktok"
    if "youtube" in host or "youtu.be" in host:
        return "youtube"
    return "web"


def process_reel_job(job: dict) -> None:
    job_id = job["id"]
    start = perf_counter()

    try:
        reel, step_durations = asyncio.run(
            process_reel_pipeline_with_metrics(
                url=job["url"],
                user_id=job["user_id"],
                progress_callback=lambda step, progress: update_processing_job(
                    job_id,
                    {
                        "current_step": step,
                        "progress_percent": progress,
                    },
                ),
            )
        )

        total_duration = round(perf_counter() - start, 3)
        update_processing_job(
            job_id,
            {
                "status": ProcessingJobStatus.completed.value,
                "current_step": "completed",
                "progress_percent": 100,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "result_reel_id": reel.id,
                "source_platform": _derive_platform(job["url"]),
                "step_durations": {
                    **step_durations,
                    "total_seconds": total_duration,
                },
            },
        )
        logger.info("Processing job %s completed in %.2fs", job_id, total_duration)
    except Exception as e:
        update_processing_job(
            job_id,
            {
                "status": ProcessingJobStatus.failed.value,
                "current_step": "failed",
                "error_message": str(e),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "progress_percent": 100,
                "source_platform": _derive_platform(job["url"]),
            },
        )
        logger.exception("Processing job %s failed: %s", job_id, e)


def run_worker() -> None:
    logger.info(
        "Background worker started with %.1fs polling interval",
        settings.WORKER_POLL_INTERVAL_SECONDS,
    )
    last_recovery_at = 0.0

    while True:
        try:
            now = time.monotonic()
            if now - last_recovery_at >= settings.WORKER_RECOVERY_INTERVAL_SECONDS:
                recovered = recover_stale_processing_jobs(
                    stale_job_minutes=settings.WORKER_STALE_JOB_MINUTES,
                )
                if recovered:
                    logger.warning("Recovered %s stalled processing job(s)", recovered)
                last_recovery_at = now

            job = claim_next_processing_job()
            if not job:
                time.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)
                continue

            process_reel_job(job)
        except Exception as e:
            logger.exception("Worker loop iteration failed: %s", e)
            time.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)
