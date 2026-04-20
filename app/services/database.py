import logging
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from app.config import get_settings
from app.models import ProcessingJobStatus
from app.services.queue_control import (
    active_source_keys,
    can_claim_job,
    job_source_key,
)
from app.services.source_identity import normalize_source_url, resolve_source_identity

logger = logging.getLogger(__name__)

_supabase_client: Client | None = None

TABLE_NAME = "reels"
PROCESSING_JOBS_TABLE = "processing_jobs"
PROCESSING_CACHE_TABLE = "processing_cache"
SERVICE_HEALTH_TABLE = "service_health"
GEOCODE_CACHE_TABLE = "geocode_cache"
URL_MATCH_FALLBACK_LIMIT = 100


def _get_client() -> Client:
    """Get or create the Supabase client."""
    global _supabase_client
    if _supabase_client is None:
        settings = get_settings()
        _supabase_client = create_client(
            settings.SUPABASE_URL,
            settings.resolved_supabase_key(),
        )
        logger.info("Supabase client initialized")
    return _supabase_client


# ----- SQL to run in Supabase SQL Editor -----
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reels (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL DEFAULT 'default-user',
    url TEXT NOT NULL,
    normalized_url TEXT,
    source_platform TEXT,
    source_content_type TEXT,
    source_content_id TEXT,
    processing_version TEXT,
    ingestion_method TEXT,
    transcript_source TEXT,
    title TEXT NOT NULL DEFAULT 'Untitled',
    summary TEXT DEFAULT '',
    transcript TEXT DEFAULT '',
    category TEXT DEFAULT 'Other',
    subcategory TEXT DEFAULT 'Other',
    secondary_categories JSONB DEFAULT '[]'::jsonb,
    key_facts JSONB DEFAULT '[]'::jsonb,
    locations JSONB DEFAULT '[]'::jsonb,
    people_mentioned JSONB DEFAULT '[]'::jsonb,
    actionable_items JSONB DEFAULT '[]'::jsonb,
    pinecone_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast user lookups
CREATE INDEX IF NOT EXISTS idx_reels_user_id ON reels(user_id);

-- Index for category filtering
CREATE INDEX IF NOT EXISTS idx_reels_category ON reels(category);

-- Index for subcategory filtering
CREATE INDEX IF NOT EXISTS idx_reels_subcategory ON reels(subcategory);

CREATE INDEX IF NOT EXISTS idx_reels_normalized_url
ON reels(normalized_url);

CREATE INDEX IF NOT EXISTS idx_reels_source_identity
ON reels(user_id, source_platform, source_content_id);

CREATE TABLE IF NOT EXISTS processing_jobs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL,
    url TEXT NOT NULL,
    normalized_url TEXT,
    source_platform TEXT,
    source_content_type TEXT,
    source_content_id TEXT,
    processing_version TEXT,
    ingestion_method TEXT,
    transcript_source TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    current_step TEXT DEFAULT 'queued',
    progress_percent INTEGER NOT NULL DEFAULT 0,
    failure_code TEXT,
    error_message TEXT,
    result_reel_id UUID,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by TEXT,
    step_durations JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_retry_window
ON processing_jobs(status, next_retry_at);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_claimed_by
ON processing_jobs(claimed_by);

CREATE INDEX IF NOT EXISTS idx_processing_jobs_source_identity
ON processing_jobs(user_id, source_platform, source_content_id);

CREATE TABLE IF NOT EXISTS service_health (
    service_name TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'unknown',
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS geocode_cache (
    query_key TEXT PRIMARY KEY,
    query_text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processing_cache (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    source_platform TEXT NOT NULL,
    source_content_id TEXT NOT NULL,
    source_content_type TEXT DEFAULT '',
    normalized_url TEXT NOT NULL,
    processing_version TEXT DEFAULT '',
    ingestion_method TEXT DEFAULT '',
    transcript_source TEXT DEFAULT '',
    transcript TEXT DEFAULT '',
    caption TEXT DEFAULT '',
    extracted_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_platform, source_content_id)
);

CREATE INDEX IF NOT EXISTS idx_processing_cache_source_identity
ON processing_cache(source_platform, source_content_id);
"""


def save_reel(reel_data: dict) -> dict:
    """
    Insert a new reel record into Supabase.

    Args:
        reel_data: Dict with reel fields matching the table schema

    Returns:
        The inserted record as a dict
    """
    client = _get_client()
    try:
        # Convert Location objects to dicts if needed
        if "locations" in reel_data and reel_data["locations"]:
            reel_data["locations"] = [
                loc if isinstance(loc, dict) else loc.model_dump()
                for loc in reel_data["locations"]
            ]

        result = client.table(TABLE_NAME).insert(reel_data).execute()
        record = result.data[0]
        logger.info(f"Saved reel: {record['id']}")
        return record
    except Exception as e:
        logger.error(f"Failed to save reel: {e}")
        raise


def update_reel_fields(reel_id: str, updates: dict) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(TABLE_NAME)
            .update(updates)
            .eq("id", reel_id)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to update reel {reel_id}: {e}")
        raise


def create_processing_job(
    *,
    user_id: str,
    url: str,
    normalized_url: str,
    source_platform: str,
    source_content_type: str | None,
    source_content_id: str | None,
    processing_version: str,
    ingestion_method: str,
    transcript_source: str | None = None,
    max_attempts: int,
) -> dict:
    client = _get_client()
    try:
        result = client.table(PROCESSING_JOBS_TABLE).insert(
            {
                "user_id": user_id,
                "url": url,
                "normalized_url": normalized_url,
                "source_platform": source_platform,
                "source_content_type": source_content_type,
                "source_content_id": source_content_id,
                "processing_version": processing_version,
                "ingestion_method": ingestion_method,
                "transcript_source": transcript_source,
                "status": ProcessingJobStatus.queued.value,
                "current_step": "queued",
                "progress_percent": 0,
                "failure_code": None,
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "next_retry_at": datetime.now(timezone.utc).isoformat(),
                "claimed_by": None,
                "step_durations": {},
            }
        ).execute()
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to create processing job: {e}")
        raise


def create_completed_processing_job(
    *,
    user_id: str,
    url: str,
    normalized_url: str,
    source_platform: str,
    source_content_type: str | None,
    source_content_id: str | None,
    processing_version: str,
    ingestion_method: str,
    transcript_source: str | None,
    result_reel_id: str,
    max_attempts: int = 1,
) -> dict:
    client = _get_client()
    now = datetime.now(timezone.utc).isoformat()
    try:
        result = client.table(PROCESSING_JOBS_TABLE).insert(
            {
                "user_id": user_id,
                "url": url,
                "normalized_url": normalized_url,
                "source_platform": source_platform,
                "source_content_type": source_content_type,
                "source_content_id": source_content_id,
                "processing_version": processing_version,
                "ingestion_method": ingestion_method,
                "transcript_source": transcript_source,
                "status": ProcessingJobStatus.completed.value,
                "current_step": "completed",
                "progress_percent": 100,
                "failure_code": None,
                "result_reel_id": result_reel_id,
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "next_retry_at": now,
                "claimed_by": None,
                "step_durations": {},
                "started_at": now,
                "completed_at": now,
                "updated_at": now,
            }
        ).execute()
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to create completed processing job: {e}")
        raise


def get_processing_cache_entry(
    *,
    source_platform: str,
    source_content_id: str,
) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(PROCESSING_CACHE_TABLE)
            .select("*")
            .eq("source_platform", source_platform)
            .eq("source_content_id", source_content_id)
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(
            "Failed to fetch processing cache for %s/%s: %s",
            source_platform,
            source_content_id,
            e,
        )
        raise


def get_geocode_cache_entry(query_key: str) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(GEOCODE_CACHE_TABLE)
            .select("*")
            .eq("query_key", query_key)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch geocode cache for {query_key}: {e}")
        raise


def upsert_geocode_cache_entry(
    *,
    query_key: str,
    query_text: str,
    status: str,
    latitude: float | None,
    longitude: float | None,
) -> dict:
    client = _get_client()
    now = datetime.now(timezone.utc).isoformat()
    try:
        result = client.table(GEOCODE_CACHE_TABLE).upsert(
            {
                "query_key": query_key,
                "query_text": query_text,
                "status": status,
                "latitude": latitude,
                "longitude": longitude,
                "updated_at": now,
            },
            on_conflict="query_key",
        ).execute()
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to upsert geocode cache for {query_key}: {e}")
        raise


def upsert_service_health(
    *,
    service_name: str,
    status: str,
    details: dict | None = None,
    last_heartbeat_at: str | None = None,
) -> dict:
    client = _get_client()
    now = datetime.now(timezone.utc).isoformat()
    try:
        result = client.table(SERVICE_HEALTH_TABLE).upsert(
            {
                "service_name": service_name,
                "status": status,
                "details": details or {},
                "last_heartbeat_at": last_heartbeat_at or now,
                "updated_at": now,
            },
            on_conflict="service_name",
        ).execute()
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to upsert service health for {service_name}: {e}")
        raise


def get_service_health(service_name: str) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(SERVICE_HEALTH_TABLE)
            .select("*")
            .eq("service_name", service_name)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch service health for {service_name}: {e}")
        raise


def list_service_health(
    *,
    service_name_prefix: str | None = None,
    limit: int = 100,
) -> list[dict]:
    client = _get_client()
    try:
        query = client.table(SERVICE_HEALTH_TABLE).select("*")
        if service_name_prefix:
            query = query.like("service_name", f"{service_name_prefix}%")
        result = query.order("updated_at", desc=True).limit(limit).execute()
        return result.data
    except Exception as e:
        logger.error(
            "Failed to list service health for prefix %s: %s",
            service_name_prefix,
            e,
        )
        raise


def upsert_processing_cache_entry(cache_data: dict) -> dict:
    client = _get_client()
    now = datetime.now(timezone.utc).isoformat()
    try:
        payload = {
            **cache_data,
            "updated_at": now,
        }
        result = client.table(PROCESSING_CACHE_TABLE).upsert(
            payload,
            on_conflict="source_platform,source_content_id",
        ).execute()
        return result.data[0]
    except Exception as e:
        logger.error("Failed to upsert processing cache: %s", e)
        raise


def claim_available_processing_jobs(
    *,
    worker_id: str,
    max_jobs: int,
    platform_limits: dict[str, int],
    current_platform_counts: dict[str, int] | None = None,
    current_source_keys: set[str] | None = None,
) -> list[dict]:
    client = _get_client()
    settings = get_settings()

    try:
        queued_result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("status", ProcessingJobStatus.queued.value)
            .lte("next_retry_at", datetime.now(timezone.utc).isoformat())
            .order("next_retry_at")
            .order("created_at")
            .limit(max(settings.JOB_FETCH_LIMIT, max_jobs * 6))
            .execute()
        )

        processing_result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("status", ProcessingJobStatus.processing.value)
            .limit(200)
            .execute()
        )

        platform_counts = {
            str(platform): int(count)
            for platform, count in (current_platform_counts or {}).items()
        }
        source_keys = active_source_keys(processing_result.data)
        if current_source_keys:
            source_keys.update(current_source_keys)
        claimed_jobs: list[dict] = []

        for job in queued_result.data:
            if len(claimed_jobs) >= max_jobs:
                break

            if not can_claim_job(
                job,
                current_platform_counts=platform_counts,
                current_source_keys=source_keys,
                platform_limits=platform_limits,
            ):
                continue

            now = datetime.now(timezone.utc).isoformat()
            attempt_count = int(job.get("attempt_count", 0) or 0) + 1
            claimed = (
                client.table(PROCESSING_JOBS_TABLE)
                .update(
                    {
                        "status": ProcessingJobStatus.processing.value,
                        "current_step": "starting",
                        "progress_percent": 5,
                        "attempt_count": attempt_count,
                        "started_at": now,
                        "completed_at": None,
                        "failure_code": None,
                        "error_message": None,
                        "next_retry_at": now,
                        "claimed_by": worker_id,
                        "updated_at": now,
                    }
                )
                .eq("id", job["id"])
                .eq("status", ProcessingJobStatus.queued.value)
                .execute()
            )

            if claimed.data:
                claimed_job = claimed.data[0]
                claimed_jobs.append(claimed_job)
                platform = str(claimed_job.get("source_platform") or "web").strip() or "web"
                platform_counts[platform] = platform_counts.get(platform, 0) + 1
                source_key = job_source_key(claimed_job)
                if source_key:
                    source_keys.add(source_key)

        return claimed_jobs
    except Exception as e:
        logger.error(f"Failed to claim processing jobs: {e}")
        raise


def update_processing_job(job_id: str, updates: dict) -> dict:
    client = _get_client()
    try:
        updates = {
            **updates,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .update(updates)
            .eq("id", job_id)
            .execute()
        )
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to update processing job {job_id}: {e}")
        raise


def update_processing_job_if_claimed(
    *,
    job_id: str,
    claimed_by: str,
    updates: dict,
) -> dict | None:
    client = _get_client()
    try:
        payload = {
            **updates,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .update(payload)
            .eq("id", job_id)
            .eq("claimed_by", claimed_by)
            .eq("status", ProcessingJobStatus.processing.value)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(
            f"Failed to update claimed processing job {job_id} for {claimed_by}: {e}"
        )
        raise


def get_processing_job(job_id: str) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("id", job_id)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch processing job {job_id}: {e}")
        raise


def count_processing_jobs_since(
    *,
    user_id: str,
    since_iso: str,
) -> int:
    client = _get_client()
    try:
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("id", count="exact")
            .eq("user_id", user_id)
            .gte("created_at", since_iso)
            .limit(1)
            .execute()
        )
        return int(result.count or 0)
    except Exception as e:
        logger.error(f"Failed to count recent processing jobs for {user_id}: {e}")
        raise


def count_processing_jobs_by_status_for_user(
    *,
    user_id: str,
    statuses: list[str],
) -> int:
    client = _get_client()
    try:
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("id", count="exact")
            .eq("user_id", user_id)
            .in_("status", statuses)
            .limit(1)
            .execute()
        )
        return int(result.count or 0)
    except Exception as e:
        logger.error(f"Failed to count processing jobs by status for {user_id}: {e}")
        raise


def find_processing_job_by_user_and_url(
    *,
    user_id: str,
    url: str,
    statuses: list[str] | None = None,
) -> dict | None:
    client = _get_client()
    try:
        query = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("user_id", user_id)
            .eq("url", url)
            .order("created_at", desc=True)
            .limit(1)
        )
        if statuses:
            query = query.in_("status", statuses)

        result = query.execute()
        if result.data:
            return result.data[0]

        query = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(URL_MATCH_FALLBACK_LIMIT)
        )
        if statuses:
            query = query.in_("status", statuses)

        fallback_result = query.execute()
        return _find_normalized_url_match(fallback_result.data, url)
    except Exception as e:
        logger.error(f"Failed to find processing job for {user_id}: {e}")
        raise


def find_processing_job_by_user_and_source_identity(
    *,
    user_id: str,
    source_platform: str,
    source_content_id: str,
    statuses: list[str] | None = None,
) -> dict | None:
    client = _get_client()
    try:
        query = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(URL_MATCH_FALLBACK_LIMIT)
        )
        if statuses:
            query = query.in_("status", statuses)

        result = query.execute()
        return _find_source_identity_match(
            result.data,
            source_platform=source_platform,
            source_content_id=source_content_id,
        )
    except Exception as e:
        logger.error(
            "Failed to find processing job by source identity for %s: %s",
            user_id,
            e,
        )
        raise


def recover_stale_processing_jobs(*, stale_job_minutes: int) -> int:
    client = _get_client()
    recovered = 0
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_job_minutes)

    try:
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("status", ProcessingJobStatus.processing.value)
            .limit(25)
            .execute()
        )

        for job in result.data:
            updated_at = (
                job.get("updated_at")
                or job.get("started_at")
                or job.get("created_at")
            )
            if not updated_at:
                continue

            try:
                last_update = datetime.fromisoformat(
                    str(updated_at).replace("Z", "+00:00")
                )
            except ValueError:
                continue

            if last_update >= cutoff:
                continue

            reset = (
                client.table(PROCESSING_JOBS_TABLE)
                .update(
                    {
                        "status": ProcessingJobStatus.queued.value,
                        "current_step": "queued",
                        "progress_percent": 0,
                        "started_at": None,
                        "completed_at": None,
                        "failure_code": None,
                        "error_message": "Recovered after a worker interruption.",
                        "next_retry_at": datetime.now(timezone.utc).isoformat(),
                        "claimed_by": None,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                .eq("id", job["id"])
                .eq("status", ProcessingJobStatus.processing.value)
                .execute()
            )

            if reset.data:
                recovered += 1

        return recovered
    except Exception as e:
        logger.error(f"Failed to recover stale processing jobs: {e}")
        raise


def list_processing_jobs(
    *,
    user_id: str,
    active_only: bool = False,
    limit: int = 20,
) -> list[dict]:
    client = _get_client()
    try:
        query = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(limit)
        )

        if active_only:
            query = query.in_(
                "status",
                [
                    ProcessingJobStatus.queued.value,
                    ProcessingJobStatus.processing.value,
                ],
            )

        result = query.execute()
        return result.data
    except Exception as e:
        logger.error(f"Failed to list processing jobs for {user_id}: {e}")
        raise


def get_processing_job_counts_by_status(
    statuses: list[str],
) -> dict[str, int]:
    client = _get_client()
    counts: dict[str, int] = {}

    try:
        for status in statuses:
            result = (
                client.table(PROCESSING_JOBS_TABLE)
                .select("id", count="exact")
                .eq("status", status)
                .limit(1)
                .execute()
            )
            counts[status] = int(result.count or 0)

        return counts
    except Exception as e:
        logger.error(f"Failed to count processing jobs by status: {e}")
        raise


def list_processing_jobs_for_metrics(limit: int = 500) -> list[dict]:
    client = _get_client()
    try:
        result = (
            client.table(PROCESSING_JOBS_TABLE)
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return result.data
    except Exception as e:
        logger.error(f"Failed to list processing jobs for metrics: {e}")
        raise


def count_table_rows(table_name: str) -> int:
    client = _get_client()
    try:
        result = client.table(table_name).select("id", count="exact").limit(1).execute()
        return int(result.count or 0)
    except Exception as e:
        logger.error(f"Failed to count rows for table {table_name}: {e}")
        raise


def count_table_rows_since(
    *,
    table_name: str,
    timestamp_column: str,
    since_iso: str,
) -> int:
    client = _get_client()
    try:
        result = (
            client.table(table_name)
            .select("id", count="exact")
            .gte(timestamp_column, since_iso)
            .limit(1)
            .execute()
        )
        return int(result.count or 0)
    except Exception as e:
        logger.error(f"Failed to count rows for table {table_name} since {since_iso}: {e}")
        raise


def list_column_values(
    *,
    table_name: str,
    column_name: str,
    limit: int = 5000,
) -> list[str]:
    client = _get_client()
    try:
        result = (
            client.table(table_name)
            .select(column_name)
            .limit(limit)
            .execute()
        )
        values = []
        for row in result.data:
            value = row.get(column_name)
            if value is None:
                continue
            values.append(str(value))
        return values
    except Exception as e:
        logger.error(f"Failed to list column {column_name} from table {table_name}: {e}")
        raise


def get_reel(reel_id: str) -> dict | None:
    """Fetch a single reel by ID."""
    client = _get_client()
    try:
        result = client.table(TABLE_NAME).select("*").eq("id", reel_id).execute()
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch reel {reel_id}: {e}")
        raise


def find_reel_by_user_and_url(*, user_id: str, url: str) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(TABLE_NAME)
            .select("*")
            .eq("user_id", user_id)
            .eq("url", url)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]

        fallback_result = (
            client.table(TABLE_NAME)
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(URL_MATCH_FALLBACK_LIMIT)
            .execute()
        )
        return _find_normalized_url_match(fallback_result.data, url)
    except Exception as e:
        logger.error(f"Failed to fetch reel by url for {user_id}: {e}")
        raise


def find_reel_by_user_and_source_identity(
    *,
    user_id: str,
    source_platform: str,
    source_content_id: str,
) -> dict | None:
    client = _get_client()
    try:
        result = (
            client.table(TABLE_NAME)
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(URL_MATCH_FALLBACK_LIMIT)
            .execute()
        )
        return _find_source_identity_match(
            result.data,
            source_platform=source_platform,
            source_content_id=source_content_id,
        )
    except Exception as e:
        logger.error(
            "Failed to fetch reel by source identity for %s: %s",
            user_id,
            e,
        )
        raise


def list_user_category_pairs(user_id: str, limit: int = 5000) -> list[dict]:
    client = _get_client()
    try:
        result = (
            client.table(TABLE_NAME)
            .select("category,subcategory,secondary_categories,created_at")
            .eq("user_id", user_id)
            .order("created_at")
            .limit(limit)
            .execute()
        )
        return result.data
    except Exception as e:
        logger.error(f"Failed to list category pairs for {user_id}: {e}")
        raise


def list_user_reels_for_recategorization(user_id: str, limit: int = 200) -> list[dict]:
    client = _get_client()
    try:
        result = (
            client.table(TABLE_NAME)
            .select("*")
            .eq("user_id", user_id)
            .order("created_at")
            .limit(limit)
            .execute()
        )
        return result.data
    except Exception as e:
        logger.error(f"Failed to list reels for recategorization for {user_id}: {e}")
        raise


def get_reels(
    user_id: str | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    List reels with optional filters.

    Args:
        user_id: Filter by user
        category: Filter by category
        subcategory: Filter by subcategory
        limit: Max number of results

    Returns:
        List of reel dicts
    """
    client = _get_client()
    try:
        query = client.table(TABLE_NAME).select("*")

        if user_id:
            query = query.eq("user_id", user_id)
        if category:
            query = query.eq("category", category)
        if subcategory:
            query = query.eq("subcategory", subcategory)

        query = query.order("created_at", desc=True).limit(limit)
        result = query.execute()
        return result.data
    except Exception as e:
        logger.error(f"Failed to list reels: {e}")
        raise


def delete_reel(reel_id: str) -> bool:
    """Delete a reel by ID. Returns True if deleted."""
    client = _get_client()
    try:
        result = client.table(TABLE_NAME).delete().eq("id", reel_id).execute()
        deleted = len(result.data) > 0
        if deleted:
            logger.info(f"Deleted reel: {reel_id}")
        return deleted
    except Exception as e:
        logger.error(f"Failed to delete reel {reel_id}: {e}")
        raise


def get_reels_by_ids(reel_ids: list[str]) -> list[dict]:
    """Fetch multiple reels by their IDs."""
    client = _get_client()
    try:
        result = client.table(TABLE_NAME).select("*").in_("id", reel_ids).execute()
        return result.data
    except Exception as e:
        logger.error(f"Failed to fetch reels by IDs: {e}")
        raise


def upsert_device_push_token(user_id: str, token: str, platform: str) -> dict:
    client = _get_client()
    try:
        result = client.table("device_push_tokens").upsert(
            {
                "user_id": user_id,
                "fcm_token": token,
                "platform": platform,
                "last_seen_at": datetime.now(timezone.utc).isoformat(),
            },
            on_conflict="fcm_token",
        ).execute()
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to upsert device push token: {e}")
        raise


def get_device_push_tokens(user_id: str) -> list[str]:
    client = _get_client()
    try:
        result = (
            client.table("device_push_tokens")
            .select("fcm_token")
            .eq("user_id", user_id)
            .execute()
        )
        return [row["fcm_token"] for row in result.data]
    except Exception as e:
        logger.error(f"Failed to fetch device push tokens for {user_id}: {e}")
        raise


def delete_device_push_tokens(tokens: list[str]) -> int:
    if not tokens:
        return 0

    client = _get_client()
    try:
        result = (
            client.table("device_push_tokens")
            .delete()
            .in_("fcm_token", tokens)
            .execute()
        )
        return len(result.data or [])
    except Exception as e:
        logger.error("Failed to delete device push tokens: %s", e)
        raise


def _find_normalized_url_match(records: list[dict], url: str) -> dict | None:
    for record in records:
        try:
            candidate = normalize_source_url(record.get("url", ""))
        except Exception:
            candidate = str(record.get("url", "")).strip()

        if candidate == url:
            return record

    return None


def _find_source_identity_match(
    records: list[dict],
    *,
    source_platform: str,
    source_content_id: str,
) -> dict | None:
    for record in records:
        try:
            candidate = normalize_source_url(record.get("url", ""))
        except Exception:
            candidate = str(record.get("url", "")).strip()

        try:
            identity = resolve_source_identity(candidate)
        except Exception:
            continue

        if (
            identity.source_platform == source_platform
            and identity.source_content_id == source_content_id
        ):
            return record

    return None
