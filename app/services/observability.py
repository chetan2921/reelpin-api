import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

from app.models import ProcessingJobStatus
from app.services.source_identity import SourceIdentity, resolve_source_identity


def log_processing_event(
    logger: logging.Logger,
    event: str,
    *,
    job_id: str | None = None,
    user_id: str | None = None,
    url: str | None = None,
    source: SourceIdentity | None = None,
    processing_step: str | None = None,
    failure_code: str | None = None,
    status: str | None = None,
    cookie_slot_index: int | None = None,
    attempt_count: int | None = None,
    max_attempts: int | None = None,
    duration_seconds: float | None = None,
    extra: dict | None = None,
) -> None:
    fields = {
        "event": event,
        "job_id": job_id,
        "user_id": user_id,
        "processing_step": processing_step,
        "failure_code": failure_code,
        "status": status,
        "cookie_slot_index": cookie_slot_index,
        "attempt_count": attempt_count,
        "max_attempts": max_attempts,
        "duration_seconds": duration_seconds,
    }

    if url:
        source = source or _safe_source_identity(url)
        fields["source_url_hash"] = _url_hash(url)
    else:
        source = source or None

    if source is not None:
        fields["source_platform"] = source.source_platform
        fields["source_content_type"] = source.source_content_type
        fields["source_content_id"] = source.source_content_id
        fields["normalized_url"] = source.normalized_url

    if extra:
        fields.update(extra)

    logger.info(
        json.dumps(
            {key: value for key, value in fields.items() if value is not None},
            sort_keys=True,
            default=str,
        )
    )


def build_processing_metrics(
    *,
    jobs: list[dict],
    queue_depth: dict[str, int],
) -> dict:
    platform_totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "success": 0,
            "failure": 0,
            "terminal": 0,
            "retry_count": 0,
            "retry_job_count": 0,
            "processing_seconds_total": 0.0,
            "processing_seconds_count": 0,
            "enqueue_to_start_total": 0.0,
            "enqueue_to_start_count": 0,
            "total_job_seconds_total": 0.0,
            "total_job_seconds_count": 0,
        }
    )
    step_totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {"total": 0.0, "count": 0}
    )
    total_enqueue_to_start_seconds = 0.0
    total_enqueue_to_start_count = 0
    total_job_seconds = 0.0
    total_job_count = 0
    total_processing_seconds = 0.0
    total_processing_count = 0
    total_retries = 0
    terminal_jobs = 0

    for job in jobs:
        platform = str(job.get("source_platform") or "unknown").strip() or "unknown"
        status = str(job.get("status") or "").strip().lower()
        attempt_count = int(job.get("attempt_count", 0) or 0)
        retries = max(attempt_count - 1, 0)
        step_durations = job.get("step_durations", {}) or {}

        if retries:
            platform_totals[platform]["retry_count"] += retries
            total_retries += retries

        enqueue_to_start_seconds = _elapsed_seconds(
            job.get("created_at"),
            job.get("started_at"),
        )
        if enqueue_to_start_seconds is not None:
            platform_totals[platform]["enqueue_to_start_total"] += enqueue_to_start_seconds
            platform_totals[platform]["enqueue_to_start_count"] += 1
            total_enqueue_to_start_seconds += enqueue_to_start_seconds
            total_enqueue_to_start_count += 1

        if status not in {
            ProcessingJobStatus.completed.value,
            ProcessingJobStatus.failed.value,
            ProcessingJobStatus.dead_lettered.value,
        }:
            continue

        terminal_jobs += 1
        platform_totals[platform]["terminal"] += 1
        if retries:
            platform_totals[platform]["retry_job_count"] += 1

        total_job_duration = _elapsed_seconds(
            job.get("created_at"),
            job.get("completed_at") or job.get("updated_at"),
        )
        if total_job_duration is not None:
            platform_totals[platform]["total_job_seconds_total"] += total_job_duration
            platform_totals[platform]["total_job_seconds_count"] += 1
            total_job_seconds += total_job_duration
            total_job_count += 1

        if status == ProcessingJobStatus.completed.value:
            platform_totals[platform]["success"] += 1
            total_seconds = _parse_duration(step_durations.get("total_seconds"))
            if total_seconds is not None:
                platform_totals[platform]["processing_seconds_total"] += total_seconds
                platform_totals[platform]["processing_seconds_count"] += 1
                total_processing_seconds += total_seconds
                total_processing_count += 1

            for step_name, value in step_durations.items():
                if not str(step_name).endswith("_seconds"):
                    continue
                parsed = _parse_duration(value)
                if parsed is None:
                    continue
                step_totals[str(step_name)]["total"] += parsed
                step_totals[str(step_name)]["count"] += 1
        else:
            platform_totals[platform]["failure"] += 1

    success_rate_by_platform = {}
    failure_rate_by_platform = {}
    retry_rate_by_platform = {}
    average_processing_seconds_by_platform = {}
    average_enqueue_to_start_seconds_by_platform = {}
    average_total_job_seconds_by_platform = {}
    retry_count_by_platform = {}

    for platform, totals in sorted(platform_totals.items()):
        terminal_total = totals["success"] + totals["failure"]
        success_rate_by_platform[platform] = round(
            (totals["success"] / terminal_total) if terminal_total else 0.0,
            4,
        )
        failure_rate_by_platform[platform] = round(
            (totals["failure"] / terminal_total) if terminal_total else 0.0,
            4,
        )
        retry_rate_by_platform[platform] = round(
            (totals["retry_job_count"] / totals["terminal"]) if totals["terminal"] else 0.0,
            4,
        )
        average_processing_seconds_by_platform[platform] = round(
            (
                totals["processing_seconds_total"] / totals["processing_seconds_count"]
            )
            if totals["processing_seconds_count"]
            else 0.0,
            3,
        )
        average_enqueue_to_start_seconds_by_platform[platform] = round(
            (
                totals["enqueue_to_start_total"] / totals["enqueue_to_start_count"]
            )
            if totals["enqueue_to_start_count"]
            else 0.0,
            3,
        )
        average_total_job_seconds_by_platform[platform] = round(
            (
                totals["total_job_seconds_total"] / totals["total_job_seconds_count"]
            )
            if totals["total_job_seconds_count"]
            else 0.0,
            3,
        )
        retry_count_by_platform[platform] = int(totals["retry_count"])

    average_step_seconds = {
        step_name: round(values["total"] / values["count"], 3)
        for step_name, values in sorted(step_totals.items())
        if values["count"]
    }

    return {
        "sample_size": terminal_jobs,
        "queue_depth": queue_depth,
        "total_retries": total_retries,
        "success_rate_by_platform": success_rate_by_platform,
        "failure_rate_by_platform": failure_rate_by_platform,
        "retry_rate_by_platform": retry_rate_by_platform,
        "average_enqueue_to_start_seconds": round(
            (total_enqueue_to_start_seconds / total_enqueue_to_start_count)
            if total_enqueue_to_start_count
            else 0.0,
            3,
        ),
        "average_enqueue_to_start_seconds_by_platform": average_enqueue_to_start_seconds_by_platform,
        "average_processing_seconds": round(
            (total_processing_seconds / total_processing_count)
            if total_processing_count
            else 0.0,
            3,
        ),
        "average_total_job_seconds": round(
            (total_job_seconds / total_job_count)
            if total_job_count
            else 0.0,
            3,
        ),
        "average_total_job_seconds_by_platform": average_total_job_seconds_by_platform,
        "average_processing_seconds_by_platform": average_processing_seconds_by_platform,
        "average_step_seconds": average_step_seconds,
        "retry_count_by_platform": retry_count_by_platform,
    }


def _parse_duration(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except ValueError:
        return None


def _parse_timestamp(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _elapsed_seconds(start_value, end_value) -> float | None:
    start = _parse_timestamp(start_value)
    end = _parse_timestamp(end_value)
    if start is None or end is None:
        return None
    elapsed = (end - start).total_seconds()
    if elapsed < 0:
        return None
    return elapsed


def _safe_source_identity(url: str) -> SourceIdentity | None:
    try:
        return resolve_source_identity(url)
    except Exception:
        return None


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()[:16]
