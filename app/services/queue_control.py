from collections import Counter

from app.services.source_identity import resolve_source_identity


def active_platform_counts(jobs: list[dict]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for job in jobs:
        counts[_job_platform(job)] += 1
    return dict(counts)


def active_source_keys(jobs: list[dict]) -> set[str]:
    keys: set[str] = set()
    for job in jobs:
        source_key = job_source_key(job)
        if source_key:
            keys.add(source_key)
    return keys


def can_claim_job(
    job: dict,
    *,
    current_platform_counts: dict[str, int],
    current_source_keys: set[str],
    platform_limits: dict[str, int],
) -> bool:
    platform = _job_platform(job)
    if current_platform_counts.get(platform, 0) >= platform_limits.get(platform, 1):
        return False

    source_key = job_source_key(job)
    if source_key and source_key in current_source_keys:
        return False

    return True


def job_source_key(job: dict) -> str | None:
    try:
        source = resolve_source_identity(str(job.get("url") or ""))
    except Exception:
        return None

    if source.source_content_id:
        return f"{source.source_platform}:{source.source_content_id}"

    if source.normalized_url:
        return f"url:{source.normalized_url}"

    return None


def _job_platform(job: dict) -> str:
    try:
        return resolve_source_identity(str(job.get("url") or "")).source_platform
    except Exception:
        return str(job.get("source_platform") or "web").strip() or "web"
