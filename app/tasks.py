import asyncio
import logging
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import urlparse

import dramatiq

from app.config import get_settings
from app.models import ProcessingJobStatus
from app.pipeline import process_reel_pipeline_with_metrics
import app.queue  # Ensures the broker is configured on import.
from app.services.database import get_processing_job, update_processing_job

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


@dramatiq.actor(
    queue_name="reel_processing",
    max_retries=settings.JOB_MAX_RETRIES,
    min_backoff=settings.JOB_MIN_BACKOFF_MS,
    max_backoff=settings.JOB_MAX_BACKOFF_MS,
)
def process_reel_job(job_id: str) -> None:
    job = get_processing_job(job_id)
    if not job:
        logger.warning("Processing job %s not found", job_id)
        return

    attempt_count = int(job.get("attempt_count", 0)) + 1
    update_processing_job(
        job_id,
        {
            "status": ProcessingJobStatus.processing.value,
            "current_step": "starting",
            "progress_percent": 5,
            "attempt_count": attempt_count,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "error_message": None,
            "source_platform": _derive_platform(job["url"]),
        },
    )

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
            },
        )
        logger.exception("Processing job %s failed: %s", job_id, e)
        raise
