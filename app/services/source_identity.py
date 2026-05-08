from dataclasses import dataclass
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


_TRACKING_QUERY_KEYS = {
    "fbclid",
    "feature",
    "igsh",
    "igshid",
    "pp",
    "si",
    "share_app_id",
    "share_item_id",
    "spm",
}


@dataclass(frozen=True)
class SourceIdentity:
    original_url: str
    normalized_url: str
    source_platform: str
    source_content_type: str
    source_content_id: str | None = None


def resolve_source_identity(url: str) -> SourceIdentity:
    raw_url = (url or "").strip()
    if not raw_url:
        raise ValueError("URL is required.")

    if "://" not in raw_url:
        raw_url = f"https://{raw_url.lstrip('/')}"

    parsed = urlparse(raw_url)
    host = _normalized_host(parsed.hostname)

    if not host:
        raise ValueError("URL is invalid.")

    if _matches_host(host, {"instagram.com", "instagr.am"}):
        return _resolve_instagram_identity(raw_url, parsed)

    raise ValueError(
        "Only Instagram URLs are supported."
    )


def normalize_source_url(url: str) -> str:
    return resolve_source_identity(url).normalized_url


def _resolve_instagram_identity(raw_url: str, parsed) -> SourceIdentity:
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) >= 2:
        kind = segments[0].lower()
        shortcode = segments[1]
        if kind in {"reel", "reels"}:
            return SourceIdentity(
                original_url=raw_url,
                normalized_url=f"https://www.instagram.com/reel/{shortcode}/",
                source_platform="instagram",
                source_content_type="reel",
                source_content_id=shortcode,
            )
        if kind == "p":
            return SourceIdentity(
                original_url=raw_url,
                normalized_url=f"https://www.instagram.com/p/{shortcode}/",
                source_platform="instagram",
                source_content_type="post",
                source_content_id=shortcode,
            )
        if kind == "tv":
            return SourceIdentity(
                original_url=raw_url,
                normalized_url=f"https://www.instagram.com/tv/{shortcode}/",
                source_platform="instagram",
                source_content_type="video",
                source_content_id=shortcode,
            )

    return SourceIdentity(
        original_url=raw_url,
        normalized_url=_normalize_generic_url(parsed, preferred_host="www.instagram.com"),
        source_platform="instagram",
        source_content_type="page",
        source_content_id=None,
    )


def _normalize_generic_url(parsed, *, preferred_host: str | None = None) -> str:
    host = preferred_host or parsed.hostname or ""
    host = host.lower()

    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    filtered_query = sorted(
        [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=False)
            if not _is_tracking_query_key(key)
        ]
    )
    query = urlencode(filtered_query, doseq=True)

    return urlunparse(("https", host, path or "/", "", query, ""))


def _normalized_host(host: str | None) -> str:
    if not host:
        return ""

    lowered = host.lower()
    for prefix in ("www.", "m.", "mobile."):
        if lowered.startswith(prefix):
            lowered = lowered[len(prefix):]

    return lowered


def _is_tracking_query_key(key: str) -> bool:
    lowered = key.lower()
    return lowered.startswith("utm_") or lowered in _TRACKING_QUERY_KEYS


def _matches_host(host: str, allowed_hosts: set[str]) -> bool:
    return host in allowed_hosts or any(host.endswith(f".{allowed}") for allowed in allowed_hosts)
