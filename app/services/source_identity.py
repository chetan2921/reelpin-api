from dataclasses import dataclass
import re
from urllib.parse import parse_qsl, parse_qs, urlencode, urlparse, urlunparse


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

    if _matches_host(host, {"youtube.com", "youtu.be"}):
        return _resolve_youtube_identity(raw_url, parsed)

    if _matches_host(host, {"tiktok.com"}):
        return _resolve_tiktok_identity(raw_url, parsed)

    normalized_url = _normalize_generic_url(parsed)
    return SourceIdentity(
        original_url=raw_url,
        normalized_url=normalized_url,
        source_platform="web",
        source_content_type="web",
        source_content_id=None,
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


def _resolve_youtube_identity(raw_url: str, parsed) -> SourceIdentity:
    video_id = _extract_youtube_video_id(raw_url)
    if video_id:
        path_segments = [segment for segment in parsed.path.split("/") if segment]
        content_type = "short" if path_segments[:1] == ["shorts"] else "video"
        return SourceIdentity(
            original_url=raw_url,
            normalized_url=f"https://www.youtube.com/watch?v={video_id}",
            source_platform="youtube",
            source_content_type=content_type,
            source_content_id=video_id,
        )

    return SourceIdentity(
        original_url=raw_url,
        normalized_url=_normalize_generic_url(parsed, preferred_host="www.youtube.com"),
        source_platform="youtube",
        source_content_type="video",
        source_content_id=None,
    )


def _resolve_tiktok_identity(raw_url: str, parsed) -> SourceIdentity:
    segments = [segment for segment in parsed.path.split("/") if segment]
    if any(segment.lower() == "video" for segment in segments):
        index = next(
            idx for idx, segment in enumerate(segments)
            if segment.lower() == "video"
        )
        if index + 1 < len(segments):
            video_id = segments[index + 1]
            if index > 0 and segments[index - 1].startswith("@"):
                normalized_path = f"/{segments[index - 1]}/video/{video_id}"
            else:
                normalized_path = f"/video/{video_id}"
            return SourceIdentity(
                original_url=raw_url,
                normalized_url=f"https://www.tiktok.com{normalized_path}",
                source_platform="tiktok",
                source_content_type="video",
                source_content_id=video_id,
            )

    if len(segments) >= 2 and segments[0].lower() == "t":
        share_id = segments[1]
        return SourceIdentity(
            original_url=raw_url,
            normalized_url=f"https://www.tiktok.com/t/{share_id}",
            source_platform="tiktok",
            source_content_type="share",
            source_content_id=share_id,
        )

    raw_host = (parsed.hostname or "").lower()
    if len(segments) == 1 and raw_host.startswith(("vm.", "vt.")):
        share_id = segments[0]
        return SourceIdentity(
            original_url=raw_url,
            normalized_url=f"https://www.tiktok.com/t/{share_id}",
            source_platform="tiktok",
            source_content_type="share",
            source_content_id=share_id,
        )

    return SourceIdentity(
        original_url=raw_url,
        normalized_url=_normalize_generic_url(parsed, preferred_host="www.tiktok.com"),
        source_platform="tiktok",
        source_content_type="video",
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


def _extract_youtube_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = _normalized_host(parsed.hostname)

    if host == "youtu.be":
        path_parts = [part for part in parsed.path.split("/") if part]
        return path_parts[0] if path_parts else None

    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
        return path_parts[1]

    query = parse_qs(parsed.query)
    video_ids = query.get("v")
    if video_ids:
        return video_ids[0]

    return None
