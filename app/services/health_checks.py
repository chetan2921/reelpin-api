from datetime import datetime, timedelta, timezone
from time import perf_counter

from app.models import HealthResponse, ServiceHealthCheck
from app.services.cookie_health import inspect_instagram_cookie_slots

def build_live_health_response() -> HealthResponse:
    checked_at = datetime.now(timezone.utc).isoformat()
    return HealthResponse(
        status="ok",
        ready=True,
        checked_at=checked_at,
        checks={
            "api": ServiceHealthCheck(
                healthy=True,
                status="ok",
                checked_at=checked_at,
                message="API process is reachable.",
            )
        },
    )


def build_readiness_health_response() -> HealthResponse:
    checked_at = datetime.now(timezone.utc).isoformat()
    checks = {
        "api": ServiceHealthCheck(
            healthy=True,
            status="ok",
            checked_at=checked_at,
            message="API process is reachable.",
        ),
        "supabase": _check_supabase(checked_at),
        "pinecone": _check_pinecone(checked_at),
        "groq": _check_groq(checked_at),
        "worker_loop": _check_worker_loop(checked_at),
        "instagram_cookies": _check_instagram_cookies(checked_at),
    }

    ready = all(check.healthy for check in checks.values())
    return HealthResponse(
        status="ok" if ready else "degraded",
        ready=ready,
        checked_at=checked_at,
        checks=checks,
    )


def _check_supabase(checked_at: str) -> ServiceHealthCheck:
    started = perf_counter()
    try:
        from app.services.database import _get_client

        result = (
            _get_client()
            .table("processing_jobs")
            .select("id", count="exact")
            .limit(1)
            .execute()
        )
        return ServiceHealthCheck(
            healthy=True,
            status="ok",
            latency_ms=_latency_ms(started),
            checked_at=checked_at,
            message="Supabase connection succeeded.",
            details={"count_sample": int(result.count or 0)},
        )
    except Exception as e:
        return _failed_check(
            checked_at=checked_at,
            started=started,
            message=f"Supabase check failed: {e}",
        )


def _check_pinecone(checked_at: str) -> ServiceHealthCheck:
    started = perf_counter()
    try:
        from app.services.embedder import _get_index

        stats = _get_index().describe_index_stats()
        return ServiceHealthCheck(
            healthy=True,
            status="ok",
            latency_ms=_latency_ms(started),
            checked_at=checked_at,
            message="Pinecone connection succeeded.",
            details={"namespaces": list((stats.get("namespaces") or {}).keys()) if isinstance(stats, dict) else []},
        )
    except Exception as e:
        return _failed_check(
            checked_at=checked_at,
            started=started,
            message=f"Pinecone check failed: {e}",
        )


def _check_groq(checked_at: str) -> ServiceHealthCheck:
    started = perf_counter()
    try:
        from app.services.transcriber import get_groq_client

        models = get_groq_client().models.list()
        model_count = len(getattr(models, "data", []) or [])
        return ServiceHealthCheck(
            healthy=True,
            status="ok",
            latency_ms=_latency_ms(started),
            checked_at=checked_at,
            message="Groq connection succeeded.",
            details={"model_count": model_count},
        )
    except Exception as e:
        return _failed_check(
            checked_at=checked_at,
            started=started,
            message=f"Groq check failed: {e}",
        )


def _check_worker_loop(checked_at: str) -> ServiceHealthCheck:
    from app.config import get_settings
    from app.services.database import get_service_health

    settings = get_settings()
    started = perf_counter()
    try:
        record = get_service_health("worker")
    except Exception as e:
        return _failed_check(
            checked_at=checked_at,
            started=started,
            message=f"Worker heartbeat check failed: {e}",
        )

    return evaluate_worker_health(
        record=record,
        checked_at=checked_at,
        stale_after_seconds=settings.HEALTH_WORKER_STALE_SECONDS,
        latency_ms=_latency_ms(started),
    )


def _check_instagram_cookies(checked_at: str) -> ServiceHealthCheck:
    from app.config import get_settings

    started = perf_counter()
    slots = inspect_instagram_cookie_slots(get_settings())
    any_configured = any(slot["configured"] for slot in slots)
    any_healthy = any(slot["healthy"] for slot in slots)

    if not any_configured:
        return ServiceHealthCheck(
            healthy=True,
            status="ok",
            latency_ms=_latency_ms(started),
            checked_at=checked_at,
            message="No Instagram cookie slots are configured. Public access will be used until cookies are needed.",
            details={"slots": slots},
        )

    if any_healthy:
        return ServiceHealthCheck(
            healthy=True,
            status="ok",
            latency_ms=_latency_ms(started),
            checked_at=checked_at,
            message="At least one Instagram cookie slot is healthy.",
            details={"slots": slots},
        )

    return ServiceHealthCheck(
        healthy=False,
        status="degraded",
        latency_ms=_latency_ms(started),
        checked_at=checked_at,
        message="All configured Instagram cookie slots are unhealthy.",
        details={"slots": slots},
    )


def evaluate_worker_health(
    *,
    record: dict | None,
    checked_at: str,
    stale_after_seconds: int,
    latency_ms: float | None = None,
    now: datetime | None = None,
) -> ServiceHealthCheck:
    current_time = now or datetime.now(timezone.utc)

    if not record:
        return ServiceHealthCheck(
            healthy=False,
            status="degraded",
            latency_ms=latency_ms,
            checked_at=checked_at,
            message="Worker heartbeat not found.",
        )

    raw_heartbeat = record.get("last_heartbeat_at")
    try:
        heartbeat = datetime.fromisoformat(str(raw_heartbeat).replace("Z", "+00:00"))
    except Exception:
        return ServiceHealthCheck(
            healthy=False,
            status="degraded",
            latency_ms=latency_ms,
            checked_at=checked_at,
            message="Worker heartbeat timestamp is invalid.",
            details={"last_heartbeat_at": raw_heartbeat},
        )

    is_fresh = heartbeat >= current_time - timedelta(seconds=stale_after_seconds)
    status = str(record.get("status") or "unknown").strip() or "unknown"
    details = record.get("details", {}) or {}

    return ServiceHealthCheck(
        healthy=is_fresh and status != "error",
        status="ok" if is_fresh and status != "error" else "degraded",
        latency_ms=latency_ms,
        checked_at=checked_at,
        message="Worker heartbeat is recent." if is_fresh else "Worker heartbeat is stale.",
        details={
            "worker_status": status,
            "last_heartbeat_at": heartbeat.isoformat(),
            **(details if isinstance(details, dict) else {}),
        },
    )


def _failed_check(
    *,
    checked_at: str,
    started: float,
    message: str,
) -> ServiceHealthCheck:
    return ServiceHealthCheck(
        healthy=False,
        status="degraded",
        latency_ms=_latency_ms(started),
        checked_at=checked_at,
        message=message,
    )


def _latency_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000, 2)
