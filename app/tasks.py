import asyncio
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import logging
import os
import socket
import time
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import urlparse

from app.config import get_settings
from app.models import ProcessingJobStatus
from app.pipeline import process_reel_pipeline_with_metrics
from app.services.database import (
    claim_available_processing_jobs,
    find_reel_by_user_and_url,
    find_reel_by_user_and_source_identity,
    recover_stale_processing_jobs,
    update_processing_job_if_claimed,
    upsert_service_health,
)
from app.services.completion_notifications import send_reel_ready_notification
from app.services.failures import classify_processing_failure
from app.services.observability import log_processing_event
from app.services.queue_control import job_source_key
from app.services.retry_policy import build_retry_decision
from app.services.ops_alerts import maybe_send_instagram_cookie_alert
from app.services.security import (
    build_secret_configuration_summary,
    configure_secure_logging,
    secret_configuration_warnings,
)
from app.services.source_identity import resolve_source_identity

configure_secure_logging()
logger = logging.getLogger(__name__)
settings = get_settings()
WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"


class JobClaimLostError(RuntimeError):
    pass


def _derive_platform(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    if "instagram" in host:
        return "instagram"
    if "tiktok" in host:
        return "tiktok"
    if "youtube" in host or "youtu.be" in host:
        return "youtube"
    return "web"


def process_reel_job(job: dict, *, worker_id: str) -> None:
    job_id = job["id"]
    start = perf_counter()
    progress_state = {
        "current_step": "starting",
        "job_id": job_id,
        "user_id": job["user_id"],
        "url": job["url"],
        "source": None,
        "cookie_slot_index": None,
        "attempt_count": int(job.get("attempt_count", 0) or 0),
        "max_attempts": int(job.get("max_attempts", 0) or 0),
        "worker_id": worker_id,
    }

    try:
        source = resolve_source_identity(job["url"])
        progress_state["source"] = source
        log_processing_event(
            logger,
            "worker.processing_job.started",
            job_id=job_id,
            user_id=job["user_id"],
            url=source.normalized_url,
            source=source,
            processing_step="starting",
            status=ProcessingJobStatus.processing.value,
            attempt_count=int(job.get("attempt_count", 0) or 0),
            max_attempts=int(job.get("max_attempts", 0) or 0),
        )

        existing_reel = find_reel_by_user_and_url(
            user_id=job["user_id"],
            url=source.normalized_url,
        )
        if not existing_reel and source.source_content_id:
            existing_reel = find_reel_by_user_and_source_identity(
                user_id=job["user_id"],
                source_platform=source.source_platform,
                source_content_id=source.source_content_id,
            )
        if existing_reel:
            total_duration = round(perf_counter() - start, 3)
            updated = update_processing_job_if_claimed(
                job_id=job_id,
                claimed_by=worker_id,
                updates={
                    "status": ProcessingJobStatus.completed.value,
                    "current_step": "completed",
                    "progress_percent": 100,
                    "failure_code": None,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "next_retry_at": datetime.now(timezone.utc).isoformat(),
                    "claimed_by": None,
                    "result_reel_id": existing_reel["id"],
                    "source_platform": source.source_platform,
                    "transcript_source": existing_reel.get("transcript_source"),
                    "step_durations": {
                        "dedupe_seconds": total_duration,
                        "total_seconds": total_duration,
                    },
                },
            )
            if not updated:
                raise JobClaimLostError(
                    f"Processing job {job_id} is no longer owned by worker {worker_id}."
                )
            log_processing_event(
                logger,
                "worker.processing_job.reused_existing_reel",
                job_id=job_id,
                user_id=job["user_id"],
                url=source.normalized_url,
                source=source,
                processing_step="completed",
                status=ProcessingJobStatus.completed.value,
                attempt_count=int(job.get("attempt_count", 0) or 0),
                max_attempts=int(job.get("max_attempts", 0) or 0),
                duration_seconds=total_duration,
                extra={"result_reel_id": existing_reel["id"]},
            )
            _notify_reel_ready(
                user_id=job["user_id"],
                reel_id=existing_reel["id"],
                job_id=job_id,
                reel_title=existing_reel.get("title"),
            )
            return

        reel, step_durations = asyncio.run(
            process_reel_pipeline_with_metrics(
                url=source.normalized_url,
                user_id=job["user_id"],
                progress_callback=lambda step, progress, extra: _persist_progress_update(
                    job_id=job_id,
                    step=step,
                    progress=progress,
                    extra=extra,
                    state=progress_state,
                ),
            )
        )

        total_duration = round(perf_counter() - start, 3)
        updated = update_processing_job_if_claimed(
            job_id=job_id,
            claimed_by=worker_id,
            updates={
                "status": ProcessingJobStatus.completed.value,
                "current_step": "completed",
                "progress_percent": 100,
                "failure_code": None,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "next_retry_at": datetime.now(timezone.utc).isoformat(),
                "claimed_by": None,
                "result_reel_id": reel.id,
                "source_platform": source.source_platform,
                "transcript_source": reel.transcript_source,
                "step_durations": {
                    **step_durations,
                    "total_seconds": total_duration,
                },
            },
        )
        if not updated:
            raise JobClaimLostError(
                f"Processing job {job_id} is no longer owned by worker {worker_id}."
            )
        log_processing_event(
            logger,
            "worker.processing_job.completed",
            job_id=job_id,
            user_id=job["user_id"],
            url=source.normalized_url,
            source=source,
            processing_step="completed",
            status=ProcessingJobStatus.completed.value,
            attempt_count=int(job.get("attempt_count", 0) or 0),
            max_attempts=int(job.get("max_attempts", 0) or 0),
            duration_seconds=total_duration,
            extra={"step_durations": step_durations, "result_reel_id": reel.id},
        )
        _notify_reel_ready(
            user_id=job["user_id"],
            reel_id=reel.id,
            job_id=job_id,
            reel_title=reel.title,
        )
    except JobClaimLostError as e:
        logger.warning(str(e))
        log_processing_event(
            logger,
            "worker.processing_job.claim_lost",
            job_id=job_id,
            user_id=job.get("user_id"),
            url=job.get("url"),
            processing_step=progress_state["current_step"],
            status="claim_lost",
            attempt_count=int(job.get("attempt_count", 0) or 0),
            max_attempts=int(job.get("max_attempts", 0) or 0),
        )
    except Exception as e:
        failure = classify_processing_failure(e, step=progress_state["current_step"])
        retry_decision = build_retry_decision(
            failure=failure,
            attempt_count=int(job.get("attempt_count", 0) or 0),
            max_attempts=int(job.get("max_attempts", 0) or 0),
            transient_retry_delay_seconds=settings.WORKER_TRANSIENT_RETRY_DELAY_SECONDS,
            rate_limit_retry_delay_seconds=settings.WORKER_RATE_LIMIT_RETRY_DELAY_SECONDS,
        )
        updated = update_processing_job_if_claimed(
            job_id=job_id,
            claimed_by=worker_id,
            updates={
                "status": retry_decision.status.value,
                "current_step": retry_decision.current_step,
                "failure_code": failure.code.value,
                "error_message": failure.message,
                "completed_at": retry_decision.completed_at,
                "next_retry_at": retry_decision.next_retry_at,
                "started_at": None,
                "claimed_by": None,
                "progress_percent": 0 if retry_decision.should_retry else 100,
                "source_platform": _derive_platform(job["url"]),
            },
        )
        if not updated:
            logger.warning(
                "Processing job %s failure update skipped because the claim is no longer owned by %s",
                job_id,
                worker_id,
            )
            return
        log_processing_event(
            logger,
            "worker.processing_job.ended",
            job_id=job_id,
            user_id=job["user_id"],
            url=job["url"],
            processing_step=progress_state["current_step"],
            failure_code=failure.code.value,
            status=retry_decision.status.value,
            attempt_count=int(job.get("attempt_count", 0) or 0),
            max_attempts=int(job.get("max_attempts", 0) or 0),
            extra={"next_retry_at": retry_decision.next_retry_at},
        )


def _persist_progress_update(
    *,
    job_id: str,
    step: str,
    progress: int,
    extra: dict,
    state: dict,
) -> None:
    updates = _progress_update(
        step=step,
        progress=progress,
        extra=extra,
        state=state,
    )
    updated = update_processing_job_if_claimed(
        job_id=job_id,
        claimed_by=state["worker_id"],
        updates=updates,
    )
    if not updated:
        raise JobClaimLostError(
            f"Processing job {job_id} progress update lost ownership for worker {state['worker_id']}."
        )


def _notify_reel_ready(
    *,
    user_id: str,
    reel_id: str,
    job_id: str,
    reel_title: str | None,
) -> None:
    try:
        delivered = send_reel_ready_notification(
            user_id=user_id,
            reel_id=reel_id,
            job_id=job_id,
            reel_title=reel_title,
        )
        log_processing_event(
            logger,
            "worker.processing_job.notification_sent",
            job_id=job_id,
            user_id=user_id,
            processing_step="completed",
            status="notification_sent",
            extra={"delivered_device_count": delivered, "result_reel_id": reel_id},
        )
    except Exception as e:
        logger.warning("Completion push skipped for job %s: %s", job_id, e)


def _progress_update(*, step: str, progress: int, extra: dict, state: dict) -> dict:
    state["current_step"] = step
    if "cookie_slot_index" in extra:
        state["cookie_slot_index"] = extra["cookie_slot_index"]
    log_processing_event(
        logger,
        "worker.processing_job.step",
        job_id=state.get("job_id"),
        user_id=state.get("user_id"),
        url=state.get("url"),
        source=state.get("source"),
        processing_step=step,
        status=ProcessingJobStatus.processing.value,
        cookie_slot_index=state.get("cookie_slot_index"),
        attempt_count=state.get("attempt_count"),
        max_attempts=state.get("max_attempts"),
        extra={"progress_percent": progress},
    )
    return {
        "current_step": step,
        "progress_percent": progress,
        "failure_code": None,
    }


def _platform_limits() -> dict[str, int]:
    return {
        "instagram": max(1, settings.WORKER_INSTAGRAM_CONCURRENCY),
        "tiktok": max(1, settings.WORKER_TIKTOK_CONCURRENCY),
        "youtube": max(1, settings.WORKER_YOUTUBE_CONCURRENCY),
        "web": max(1, settings.WORKER_WEB_CONCURRENCY),
    }


def _reap_completed_jobs(active_futures: dict[Future, dict]) -> None:
    completed = [future for future in active_futures if future.done()]
    for future in completed:
        job_meta = active_futures.pop(future, {})
        try:
            future.result()
        except Exception as e:
            logger.exception(
                "Worker future for job %s crashed outside job error handling: %s",
                job_meta.get("job_id"),
                e,
            )


def _wait_for_capacity(active_futures: dict[Future, dict]) -> None:
    if not active_futures:
        return

    done, _ = wait(
        list(active_futures.keys()),
        timeout=settings.WORKER_POLL_INTERVAL_SECONDS,
        return_when=FIRST_COMPLETED,
    )
    for future in done:
        job_meta = active_futures.pop(future, {})
        try:
            future.result()
        except Exception as e:
            logger.exception(
                "Worker future for job %s crashed outside job error handling: %s",
                job_meta.get("job_id"),
                e,
            )


def _active_platform_counts(active_futures: dict[Future, dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job_meta in active_futures.values():
        platform = str(job_meta.get("source_platform") or "web")
        counts[platform] = counts.get(platform, 0) + 1
    return counts


def _active_source_keys(active_futures: dict[Future, dict]) -> list[str]:
    keys: list[str] = []
    for job_meta in active_futures.values():
        source_key = job_meta.get("source_key")
        if source_key:
            keys.append(str(source_key))
    return keys


def _worker_heartbeat_details(active_futures: dict[Future, dict]) -> dict:
    return {
        "worker_id": WORKER_ID,
        "active_job_count": len(active_futures),
        "active_platform_counts": _active_platform_counts(active_futures),
        "active_source_keys": _active_source_keys(active_futures),
        "max_concurrency": max(1, settings.WORKER_CONCURRENCY),
    }


def run_worker() -> None:
    worker_concurrency = max(1, settings.WORKER_CONCURRENCY)
    logger.info(
        "Runtime secret configuration: %s",
        build_secret_configuration_summary(settings),
    )
    for warning in secret_configuration_warnings(settings):
        logger.warning(warning)
    logger.info(
        "Background worker started with %.1fs polling interval and concurrency %s",
        settings.WORKER_POLL_INTERVAL_SECONDS,
        worker_concurrency,
    )
    last_recovery_at = 0.0
    last_heartbeat_at = 0.0
    active_futures: dict[Future, dict] = {}
    platform_limits = _platform_limits()

    _heartbeat_worker(
        status="ok",
        details={"state": "started", **_worker_heartbeat_details(active_futures)},
    )

    with ThreadPoolExecutor(max_workers=worker_concurrency) as executor:
        while True:
            try:
                _reap_completed_jobs(active_futures)
                now = time.monotonic()

                if now - last_heartbeat_at >= settings.WORKER_HEARTBEAT_INTERVAL_SECONDS:
                    _heartbeat_worker(
                        status="ok",
                        details={
                            "state": "processing" if active_futures else "idle",
                            **_worker_heartbeat_details(active_futures),
                        },
                    )
                    maybe_send_instagram_cookie_alert(settings)
                    last_heartbeat_at = now

                if now - last_recovery_at >= settings.WORKER_RECOVERY_INTERVAL_SECONDS:
                    recovered = recover_stale_processing_jobs(
                        stale_job_minutes=settings.WORKER_STALE_JOB_MINUTES,
                    )
                    if recovered:
                        logger.warning("Recovered %s stalled processing job(s)", recovered)
                    last_recovery_at = now

                available_slots = max(0, worker_concurrency - len(active_futures))
                if available_slots > 0:
                    claimed_jobs = claim_available_processing_jobs(
                        worker_id=WORKER_ID,
                        max_jobs=available_slots,
                        platform_limits=platform_limits,
                    )
                    for job in claimed_jobs:
                        future = executor.submit(process_reel_job, job, worker_id=WORKER_ID)
                        active_futures[future] = {
                            "job_id": job["id"],
                            "source_platform": job.get("source_platform"),
                            "source_key": job_source_key(job),
                        }
                    if claimed_jobs:
                        continue

                if active_futures:
                    _wait_for_capacity(active_futures)
                    continue

                time.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)
            except Exception as e:
                _heartbeat_worker(
                    status="error",
                    details={
                        "state": "error",
                        "last_error": str(e),
                        **_worker_heartbeat_details(active_futures),
                    },
                )
                logger.exception("Worker loop iteration failed: %s", e)
                time.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)


def _heartbeat_worker(*, status: str, details: dict) -> None:
    try:
        upsert_service_health(
            service_name="worker",
            status=status,
            details={
                "poll_interval_seconds": settings.WORKER_POLL_INTERVAL_SECONDS,
                **details,
            },
        )
    except Exception as e:
        logger.warning("Worker heartbeat update failed: %s", e)
