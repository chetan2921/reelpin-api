import base64
import gzip
import html
import json
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from uuid import uuid4

import yt_dlp

from app.config import get_settings

logger = logging.getLogger(__name__)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_INSTAGRAM_API_HEADERS = {
    "X-IG-App-ID": "936619743392459",
    "X-ASBD-ID": "198387",
    "X-IG-WWW-Claim": "0",
    "Origin": "https://www.instagram.com",
    "Accept": "*/*",
    "Referer": "https://www.instagram.com/",
    "User-Agent": _BROWSER_HEADERS["User-Agent"],
}


@dataclass
class DownloadedMedia:
    media_type: str
    media_paths: list[str]
    caption: str = ""
    cookie_slot_index: int | None = None


@dataclass(frozen=True)
class CookieSlot:
    index: int
    label: str
    file_path: str


def download_media(url: str) -> DownloadedMedia:
    """
    Download supported media from Instagram, YouTube, or TikTok.

    Returns either a video payload or an image-post payload.
    """
    settings = get_settings()
    download_dir = settings.TEMP_DOWNLOAD_DIR
    os.makedirs(download_dir, exist_ok=True)
    public_instagram_error = None
    cookie_slots, temp_cookie_files = _build_cookie_slots_from_env(url)
    candidate_slots: list[CookieSlot | None] = []
    if _is_instagram_url(url):
        candidate_slots.append(None)
        if settings.YTDLP_COOKIES_FROM_BROWSER:
            candidate_slots.append(None)
        candidate_slots.extend(cookie_slots)
    else:
        candidate_slots = list(cookie_slots)
    if not candidate_slots:
        candidate_slots = [None]
    elif not _is_instagram_url(url):
        candidate_slots.append(None)

    last_error: Exception | None = None

    try:
        for candidate_index, selected_cookie_slot in enumerate(candidate_slots):
            cookie_file = selected_cookie_slot.file_path if selected_cookie_slot else None
            instagram_cookie_header = (
                _build_cookie_header(cookie_file, "instagram.com")
                if _is_instagram_url(url) and cookie_file
                else None
            )
            cookie_slot_index = selected_cookie_slot.index if selected_cookie_slot else None

            output_path = os.path.join(download_dir, "%(id)s.%(ext)s")
            ydl_opts = {
                "outtmpl": output_path,
                "format": _preferred_download_format(url),
                "quiet": True,
                "no_warnings": True,
                "postprocessors": [],
                "http_headers": _BROWSER_HEADERS,
                "noplaylist": True,
            }

            if cookie_file:
                ydl_opts["cookiefile"] = cookie_file
                logger.info(
                    "Using %s cookie slot %s for download",
                    _platform_name(url),
                    selected_cookie_slot.label if selected_cookie_slot else "none",
                )

            use_browser_cookies = (
                settings.YTDLP_COOKIES_FROM_BROWSER
                if (not _is_instagram_url(url) and selected_cookie_slot is None)
                or (_is_instagram_url(url) and candidate_index == 1)
                else None
            )
            is_cookie_free_instagram_attempt = (
                _is_instagram_url(url)
                and selected_cookie_slot is None
            )
            if use_browser_cookies:
                ydl_opts["cookiesfrombrowser"] = (settings.YTDLP_COOKIES_FROM_BROWSER,)

            try:
                slot_public_instagram_error = None
                if _is_instagram_url(url):
                    if instagram_cookie_header:
                        try:
                            api_media = _download_authenticated_instagram_media(
                                url,
                                download_dir,
                                cookie_header=instagram_cookie_header,
                            )
                            if api_media is not None:
                                api_media.cookie_slot_index = cookie_slot_index
                                return api_media
                        except Exception as e:
                            if _should_try_next_cookie(str(e), has_more_slots=candidate_index < len(candidate_slots) - 1):
                                logger.warning(
                                    "Authenticated Instagram API fetch failed for cookie slot %s: %s",
                                    selected_cookie_slot.label if selected_cookie_slot else "none",
                                    e,
                                )
                                last_error = Exception(str(e))
                                continue
                            logger.warning("Authenticated Instagram API fetch failed: %s", e)
                    try:
                        public_media = _download_public_instagram_media(
                            url,
                            download_dir,
                            cookie_header=instagram_cookie_header,
                        )
                        public_media.cookie_slot_index = cookie_slot_index
                        return public_media
                    except Exception as e:
                        slot_public_instagram_error = str(e)
                        logger.warning("Public Instagram fetch failed: %s", e)
                        if is_cookie_free_instagram_attempt and candidate_index < len(candidate_slots) - 1:
                            last_error = Exception(slot_public_instagram_error)
                            continue
                        if _should_try_next_cookie(slot_public_instagram_error, has_more_slots=candidate_index < len(candidate_slots) - 1):
                            last_error = Exception(slot_public_instagram_error)
                            continue

                logger.info("Downloading media from: %s", url)
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    downloaded_file = ydl.prepare_filename(info)

                    if not os.path.exists(downloaded_file):
                        raise FileNotFoundError(
                            f"Download completed but file not found at {downloaded_file}"
                        )

                    caption = str(info.get("description") or "")
                    logger.info("Downloaded media to: %s", downloaded_file)
                    return DownloadedMedia(
                        media_type="video",
                        media_paths=[downloaded_file],
                        caption=caption,
                        cookie_slot_index=cookie_slot_index,
                    )

            except yt_dlp.utils.DownloadError as e:
                logger.error("yt-dlp download error: %s", e)
                friendly_error = _friendly_download_error(
                    url=url,
                    raw_message=str(e),
                    public_instagram_error=slot_public_instagram_error if _is_instagram_url(url) else None,
                )
                if is_cookie_free_instagram_attempt and candidate_index < len(candidate_slots) - 1:
                    last_error = Exception(friendly_error)
                    continue
                if _should_try_next_cookie(
                    friendly_error,
                    has_more_slots=candidate_index < len(candidate_slots) - 1,
                ):
                    logger.warning(
                        "Cookie slot %s failed for %s, trying next slot",
                        selected_cookie_slot.label if selected_cookie_slot else "anonymous",
                        _platform_name(url),
                    )
                    last_error = Exception(friendly_error)
                    continue
                raise Exception(friendly_error)
            except Exception as e:
                logger.error("Unexpected download error: %s", e)
                if is_cookie_free_instagram_attempt and candidate_index < len(candidate_slots) - 1:
                    last_error = e
                    continue
                if _should_try_next_cookie(
                    str(e),
                    has_more_slots=candidate_index < len(candidate_slots) - 1,
                ):
                    last_error = e
                    continue
                raise

        if last_error:
            raise last_error
        raise Exception("Download failed without a specific error.")
    finally:
        for temp_cookie_file in temp_cookie_files:
            cleanup_file(temp_cookie_file)


def download_reel(url: str) -> tuple[str, str]:
    """
    Backwards-compatible helper for callers that still expect a video tuple.
    """
    media = download_media(url)
    if media.media_type != "video" or not media.media_paths:
        raise Exception("This URL resolved to an image post instead of a video.")
    return media.media_paths[0], media.caption


def _download_public_instagram_media(
    url: str,
    download_dir: str,
    *,
    cookie_header: str | None = None,
) -> DownloadedMedia:
    logger.info("Trying public Instagram page fetch for: %s", url)
    request_headers = {
        **_BROWSER_HEADERS,
        "Referer": "https://www.instagram.com/",
    }
    if cookie_header:
        request_headers["Cookie"] = cookie_header
    request = urllib.request.Request(url, headers=request_headers)

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            page = response.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        raise Exception(f"Instagram page fetch returned HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise Exception("Instagram page fetch failed before yt-dlp could run") from e

    content_kind = _instagram_path_kind(url)
    caption = (
        _extract_meta_content(page, "og:description")
        or _extract_meta_content(page, "description", key="name")
        or ""
    )

    image_urls = _extract_instagram_image_urls(page)
    video_url = (
        _extract_meta_content(page, "og:video:secure_url")
        or _extract_meta_content(page, "og:video")
        or _extract_embedded_media_url(page, "video_url")
    )

    if video_url and content_kind != "post":
        destination = os.path.join(download_dir, f"instagram-{uuid4().hex}.mp4")
        _download_remote_file(video_url, destination)
        logger.info("Downloaded Instagram video via public page fetch: %s", destination)
        return DownloadedMedia(
            media_type="video",
            media_paths=[destination],
            caption=caption,
            cookie_slot_index=None,
        )

    if image_urls and (len(image_urls) > 1 or not video_url):
        image_paths = []
        for index, image_url in enumerate(image_urls, start=1):
            destination = os.path.join(
                download_dir,
                f"instagram-{uuid4().hex}-{index}.jpg",
            )
            _download_remote_file(image_url, destination)
            image_paths.append(destination)

        logger.info(
            "Downloaded %s Instagram image slide(s) via public page fetch",
            len(image_paths),
        )
        return DownloadedMedia(
            media_type="image",
            media_paths=image_paths,
            caption=caption,
            cookie_slot_index=None,
        )

    if video_url:
        destination = os.path.join(download_dir, f"instagram-{uuid4().hex}.mp4")
        _download_remote_file(video_url, destination)
        return DownloadedMedia(
            media_type="video",
            media_paths=[destination],
            caption=caption,
            cookie_slot_index=None,
        )

    raise Exception("Instagram did not expose a public media URL for this page.")


def _download_authenticated_instagram_media(
    url: str,
    download_dir: str,
    *,
    cookie_header: str,
) -> DownloadedMedia | None:
    shortcode = _instagram_shortcode(url)
    if not shortcode:
        return None

    media_pk = _instagram_shortcode_to_pk(shortcode)
    request = urllib.request.Request(
        f"https://i.instagram.com/api/v1/media/{media_pk}/info/",
        headers={
            **_INSTAGRAM_API_HEADERS,
            "Cookie": cookie_header,
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise Exception(f"Instagram API info returned HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise Exception("Instagram API info request failed") from e

    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return None

    item = items[0] or {}
    caption = str(((item.get("caption") or {}).get("text")) or "")

    carousel_media = item.get("carousel_media")
    if isinstance(carousel_media, list) and carousel_media:
        image_paths = _download_instagram_image_entries(
            carousel_media,
            download_dir,
            cookie_header=cookie_header,
        )
        if image_paths:
            return DownloadedMedia(
                media_type="image",
                media_paths=image_paths,
                caption=caption,
                cookie_slot_index=None,
            )

    image_paths = _download_instagram_image_entries(
        [item],
        download_dir,
        cookie_header=cookie_header,
    )
    if image_paths:
        return DownloadedMedia(
            media_type="image",
            media_paths=image_paths,
            caption=caption,
            cookie_slot_index=None,
        )

    video_versions = item.get("video_versions") or []
    if video_versions:
        best_video = max(
            [entry for entry in video_versions if isinstance(entry, dict) and entry.get("url")],
            key=lambda entry: int(entry.get("height") or 0) * int(entry.get("width") or 0),
            default=None,
        )
        if best_video:
            destination = os.path.join(download_dir, f"instagram-{uuid4().hex}.mp4")
            _download_remote_file(
                best_video["url"],
                destination,
                extra_headers={"Cookie": cookie_header, "Referer": "https://www.instagram.com/"},
            )
            return DownloadedMedia(
                media_type="video",
                media_paths=[destination],
                caption=caption,
                cookie_slot_index=None,
            )

    return None


def _download_instagram_image_entries(
    entries: list[dict],
    download_dir: str,
    *,
    cookie_header: str,
) -> list[str]:
    image_paths: list[str] = []
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            continue
        if entry.get("video_versions"):
            continue

        candidates = ((entry.get("image_versions2") or {}).get("candidates")) or []
        if not isinstance(candidates, list) or not candidates:
            continue

        best_image = max(
            [candidate for candidate in candidates if isinstance(candidate, dict) and candidate.get("url")],
            key=lambda candidate: int(candidate.get("height") or 0) * int(candidate.get("width") or 0),
            default=None,
        )
        if not best_image:
            continue

        destination = os.path.join(download_dir, f"instagram-{uuid4().hex}-{index}.jpg")
        _download_remote_file(
            best_image["url"],
            destination,
            extra_headers={"Cookie": cookie_header, "Referer": "https://www.instagram.com/"},
        )
        image_paths.append(destination)

    return image_paths


def _friendly_download_error(
    *,
    url: str,
    raw_message: str,
    public_instagram_error: str | None,
) -> str:
    lowered = raw_message.lower()
    platform = _platform_name(url)

    if _is_instagram_url(url) and public_instagram_error:
        logger.info("Previous public Instagram fetch result: %s", public_instagram_error)

    if "login required" in lowered or "cookies" in lowered:
        return (
            f"{platform} blocked anonymous download for this URL. "
            "Add authenticated cookies to the backend and try again."
        )

    if "rate-limit" in lowered or "rate limit" in lowered:
        return (
            f"{platform} rate limited the downloader. "
            "Try again later or add authenticated cookies to the backend."
        )

    if "private" in lowered:
        return f"This {platform} post is private and cannot be downloaded by the backend."

    if public_instagram_error:
        return public_instagram_error

    return (
        f"Failed to download this media from {platform}. "
        "It may be private, unavailable, or temporarily blocked."
    )


def _should_try_next_cookie(message: str, *, has_more_slots: bool) -> bool:
    if not has_more_slots:
        return False

    lowered = message.lower()
    retry_patterns = [
        "login required",
        "authenticated cookies",
        "blocked anonymous download",
        "fresh authenticated session",
        "cookie",
        "cookies",
        "unauthorized",
        "forbidden",
        "429",
        "rate limit",
        "rate-limit",
        "too many requests",
    ]
    return any(pattern in lowered for pattern in retry_patterns)


def _build_cookie_slots_from_env(url: str) -> tuple[list[CookieSlot], list[str]]:
    settings = get_settings()
    platform = _platform_key(url)
    temp_files: list[str] = []
    slots: list[CookieSlot] = []

    for index, label in enumerate(["active", "backup"], start=1):
        slot = _build_cookie_slot(
            settings,
            platform=platform,
            label=label,
            index=index,
            temp_files=temp_files,
        )
        if slot:
            slots.append(slot)

    if not slots:
        legacy_slot = _build_legacy_cookie_slot(
            settings,
            platform=platform,
            temp_files=temp_files,
        )
        if legacy_slot:
            slots.append(legacy_slot)

    return slots, temp_files


def _build_cookie_slot(
    settings,
    *,
    platform: str,
    label: str,
    index: int,
    temp_files: list[str],
) -> CookieSlot | None:
    file_value = _slot_cookie_file_value(settings, platform, label)
    if file_value:
        return CookieSlot(index=index, label=label, file_path=file_value)

    raw_cookies = _slot_cookie_blob_value(settings, platform, label)
    if not raw_cookies and platform != "ytdlp":
        raw_cookies = _slot_cookie_blob_value(settings, "ytdlp", label)

    if not raw_cookies:
        return None

    file_path = _write_cookie_blob_to_temp_file(raw_cookies)
    temp_files.append(file_path)
    return CookieSlot(index=index, label=label, file_path=file_path)


def _build_legacy_cookie_slot(settings, *, platform: str, temp_files: list[str]) -> CookieSlot | None:
    legacy_file = _legacy_cookie_file_value(settings, platform)
    if legacy_file:
        return CookieSlot(index=1, label="legacy", file_path=legacy_file)

    raw_cookies = _legacy_cookie_blob_value(settings, platform)
    if not raw_cookies and platform != "ytdlp":
        raw_cookies = _legacy_cookie_blob_value(settings, "ytdlp")

    if not raw_cookies:
        return None

    file_path = _write_cookie_blob_to_temp_file(raw_cookies)
    temp_files.append(file_path)
    return CookieSlot(index=1, label="legacy", file_path=file_path)


def _slot_cookie_file_value(settings, platform: str, label: str) -> str | None:
    attr_name = f"{platform.upper()}_{label.upper()}_COOKIES_FILE"
    return getattr(settings, attr_name, None)


def _slot_cookie_blob_value(settings, platform: str, label: str) -> str | None:
    attr_prefix = f"{platform.upper()}_{label.upper()}_COOKIE_DATA"
    try:
        return _decode_cookie_blob(
            encoded=getattr(settings, f"{attr_prefix}_BASE64", None),
            plain=getattr(settings, attr_prefix, None),
            encoded_label=f"{attr_prefix}_BASE64",
        )
    except Exception as e:
        logger.warning("%s %s cookie slot is invalid: %s", platform, label, e)
        return None


def _legacy_cookie_file_value(settings, platform: str) -> str | None:
    if platform == "instagram":
        return settings.INSTAGRAM_COOKIES_FILE
    return None


def _legacy_cookie_blob_value(settings, platform: str) -> str | None:
    try:
        return _decode_cookie_blob(
            encoded=getattr(settings, f"{platform.upper()}_COOKIE_DATA_BASE64", None),
            plain=getattr(settings, f"{platform.upper()}_COOKIE_DATA", None),
            encoded_label=f"{platform.upper()}_COOKIE_DATA_BASE64",
        )
    except Exception as e:
        logger.warning("Legacy %s cookie data is invalid: %s", platform, e)
        return None


def _write_cookie_blob_to_temp_file(raw_cookies: str) -> str:
    with tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".txt",
        prefix="yt-dlp-cookies-",
        encoding="utf-8",
    ) as tmp:
        tmp.write(raw_cookies)
        return tmp.name


def _decode_cookie_blob(*, encoded: str | None, plain: str | None, encoded_label: str) -> str | None:
    if encoded:
        try:
            decoded_bytes = base64.b64decode(encoded.encode("utf-8"))
            if decoded_bytes[:2] == b"\x1f\x8b":
                decoded_bytes = gzip.decompress(decoded_bytes)
            return decoded_bytes.decode("utf-8")
        except Exception as e:
            raise Exception(f"{encoded_label} could not be decoded.") from e
    if plain:
        return plain.replace("\\n", "\n")
    return None


def _platform_key(url: str) -> str:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if "instagram" in host:
        return "instagram"
    if "youtube" in host or "youtu.be" in host:
        return "youtube"
    if "tiktok" in host:
        return "tiktok"
    return "ytdlp"


def _preferred_download_format(url: str) -> str:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if "youtube" in host or "youtu.be" in host:
        return "bestaudio/best"
    if "instagram" in host or "tiktok" in host:
        return "bestaudio/best[height<=360]/best"
    return "bestaudio/best"


def _platform_name(url: str) -> str:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if "instagram" in host:
        return "Instagram"
    if "youtube" in host or "youtu.be" in host:
        return "YouTube"
    if "tiktok" in host:
        return "TikTok"
    return "the source platform"


def _is_instagram_url(url: str) -> bool:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    return "instagram.com" in host


def _instagram_shortcode(url: str) -> str | None:
    path_parts = [part for part in urllib.parse.urlparse(url).path.split("/") if part]
    if len(path_parts) < 2:
        return None
    if path_parts[0] not in {"reel", "p", "tv"}:
        return None
    return path_parts[1]


def _instagram_path_kind(url: str) -> str:
    path_parts = [part for part in urllib.parse.urlparse(url).path.split("/") if part]
    if not path_parts:
        return "unknown"
    if path_parts[0] in {"reel", "p", "tv"}:
        return path_parts[0]
    return "unknown"


def _instagram_shortcode_to_pk(shortcode: str) -> str:
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    value = 0
    for character in shortcode[:11]:
        value = value * len(alphabet) + alphabet.index(character)
    return str(value)


def _extract_instagram_image_urls(document: str) -> list[str]:
    urls: list[str] = []
    patterns = [
        r'"display_url":"([^"]+)"',
        r'"thumbnail_src":"([^"]+)"',
        r'"image_url":"([^"]+)"',
    ]

    for pattern in patterns:
        for match in re.findall(pattern, document):
            decoded = _decode_escaped_url(match)
            if decoded and decoded not in urls:
                urls.append(decoded)

    og_image = _extract_meta_content(document, "og:image")
    if og_image and og_image not in urls:
        urls.insert(0, og_image)

    return urls


def _extract_meta_content(
    document: str, attribute: str, *, key: str = "property"
) -> str | None:
    patterns = [
        rf'<meta[^>]+{key}=["\']{re.escape(attribute)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+{key}=["\']{re.escape(attribute)}["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, document, flags=re.IGNORECASE)
        if match:
            return html.unescape(match.group(1))

    return None


def _extract_embedded_media_url(document: str, field_name: str) -> str | None:
    match = re.search(rf'"{re.escape(field_name)}":"([^"]+)"', document)
    if not match:
        return None
    return _decode_escaped_url(match.group(1))


def _decode_escaped_url(value: str) -> str | None:
    try:
        return html.unescape(json.loads(f'"{value}"'))
    except json.JSONDecodeError:
        return html.unescape(value.replace("\\/", "/"))


def _download_remote_file(
    source_url: str,
    destination: str,
    *,
    extra_headers: dict[str, str] | None = None,
) -> None:
    request = urllib.request.Request(
        source_url,
        headers={**_BROWSER_HEADERS, **(extra_headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response, open(
            destination,
            "wb",
        ) as file_handle:
            shutil.copyfileobj(response, file_handle)
    except urllib.error.HTTPError as e:
        raise Exception(f"Remote media fetch returned HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise Exception("Remote media download failed") from e

    if not os.path.exists(destination):
        raise FileNotFoundError(
            f"Media download completed but file not found at {destination}"
        )


def _build_cookie_header(cookie_file: str | None, domain_suffix: str) -> str | None:
    if not cookie_file or not os.path.exists(cookie_file):
        return None

    cookies: list[str] = []
    try:
        with open(cookie_file, encoding="utf-8", errors="ignore") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = line.split("\t")
                if len(parts) < 7:
                    continue

                domain, _, path, secure_flag, expires_at, name, value = parts[:7]
                normalized_domain = domain.lstrip(".").lower()
                if not (
                    normalized_domain == domain_suffix
                    or normalized_domain.endswith(f".{domain_suffix}")
                ):
                    continue

                if expires_at.isdigit() and expires_at != "0":
                    try:
                        if int(expires_at) < int(time.time()):
                            continue
                    except ValueError:
                        pass

                if not name or not value:
                    continue

                cookies.append(f"{name}={value}")
    except OSError as e:
        logger.warning("Failed to parse configured cookie file for %s: %s", domain_suffix, e)
        return None

    if not cookies:
        return None

    return "; ".join(cookies)


def cleanup_file(file_path: str) -> None:
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info("Cleaned up temp file: %s", file_path)
    except OSError as e:
        logger.warning("Failed to clean up %s: %s", file_path, e)
