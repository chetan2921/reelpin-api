from datetime import datetime, timedelta, timezone

from app.models import DashboardOverviewResponse, ProcessingJobStatus
from app.services.cookie_health import inspect_instagram_cookie_slots
from app.services.observability import build_processing_metrics


def build_dashboard_overview() -> DashboardOverviewResponse:
    checked_at = datetime.now(timezone.utc).isoformat()
    since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    registered_profiles = _safe_count("profiles")
    registered_devices = _safe_count("device_push_tokens") or 0
    saved_reels = _safe_count("reels") or 0
    processing_jobs = _safe_count("processing_jobs") or 0
    device_users = _unique_count("device_push_tokens", "user_id")

    processing_counts = _get_processing_job_counts_by_status(
        [
            ProcessingJobStatus.queued.value,
            ProcessingJobStatus.processing.value,
            ProcessingJobStatus.completed.value,
            ProcessingJobStatus.failed.value,
            ProcessingJobStatus.dead_lettered.value,
        ]
    )
    active_jobs = processing_counts.get("queued", 0) + processing_counts.get("processing", 0)
    processing_metrics = build_processing_metrics(
        jobs=_list_processing_jobs_for_metrics(limit=500),
        queue_depth={
            "queued": processing_counts.get("queued", 0),
            "processing": processing_counts.get("processing", 0),
            "dead_lettered": processing_counts.get("dead_lettered", 0),
        },
    )
    health = _build_readiness_health_response().model_dump()

    return DashboardOverviewResponse(
        checked_at=checked_at,
        registered_profile_count=registered_profiles,
        registered_device_count=registered_devices,
        registered_device_user_count=device_users,
        saved_reel_count=saved_reels,
        processing_job_count=processing_jobs,
        active_job_count=active_jobs,
        completed_job_count=processing_counts.get("completed", 0),
        failed_job_count=processing_counts.get("failed", 0),
        dead_lettered_job_count=processing_counts.get("dead_lettered", 0),
        reels_saved_last_24h=_safe_count_since("reels", "created_at", since_24h),
        jobs_created_last_24h=_safe_count_since("processing_jobs", "created_at", since_24h),
        exact_download_count=None,
        exact_download_source="Use App Store Connect, Play Console, or Firebase Analytics for exact download counts. Backend device registrations are only a proxy.",
        processing_metrics=processing_metrics,
        health=health,
        instagram_cookie_health=_instagram_cookie_health(),
    )


def _unique_count(table_name: str, column_name: str) -> int:
    from app.services.database import list_column_values

    values = list_column_values(table_name=table_name, column_name=column_name, limit=5000)
    return len({value for value in values if value})


def _safe_count(table_name: str) -> int | None:
    try:
        from app.services.database import count_table_rows

        return count_table_rows(table_name)
    except Exception:
        return None


def _safe_count_since(table_name: str, timestamp_column: str, since_iso: str) -> int:
    try:
        from app.services.database import count_table_rows_since

        return count_table_rows_since(
            table_name=table_name,
            timestamp_column=timestamp_column,
            since_iso=since_iso,
        )
    except Exception:
        return 0


def _settings():
    from app.config import get_settings

    return get_settings()


def _instagram_cookie_health() -> list[dict]:
    return inspect_instagram_cookie_slots(_settings())


def _get_processing_job_counts_by_status(statuses: list[str]) -> dict[str, int]:
    from app.services.database import get_processing_job_counts_by_status

    return get_processing_job_counts_by_status(statuses)


def _list_processing_jobs_for_metrics(limit: int = 500) -> list[dict]:
    from app.services.database import list_processing_jobs_for_metrics

    return list_processing_jobs_for_metrics(limit=limit)


def _build_readiness_health_response():
    from app.services.health_checks import build_readiness_health_response

    return build_readiness_health_response()
