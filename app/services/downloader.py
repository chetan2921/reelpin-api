import base64
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


def download_media(url: str) -> DownloadedMedia:
    """
    Download supported media from Instagram, YouTube, or TikTok.

    Returns either a video payload or an image-post payload.
    """
    settings = get_settings()
    download_dir = settings.TEMP_DOWNLOAD_DIR
    os.makedirs(download_dir, exist_ok=True)
    public_instagram_error = None
    temp_cookie_file = _build_cookie_file_from_env(url)
    instagram_kind = _instagram_path_kind(url) if _is_instagram_url(url) else "unknown"
    instagram_cookie_file = temp_cookie_file or settings.INSTAGRAM_COOKIES_FILE
    instagram_cookie_header = (
        _build_cookie_header(instagram_cookie_file, "instagram.com")
        if _is_instagram_url(url)
        else None
    )

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

    cookie_file = instagram_cookie_file
    if cookie_file:
        ydl_opts["cookiefile"] = cookie_file

    if settings.YTDLP_COOKIES_FROM_BROWSER:
        ydl_opts["cookiesfrombrowser"] = (settings.YTDLP_COOKIES_FROM_BROWSER,)

    try:
        if _is_instagram_url(url):
            if instagram_cookie_header:
                try:
                    api_media = _download_authenticated_instagram_media(
                        url,
                        download_dir,
                        cookie_header=instagram_cookie_header,
                    )
                    if api_media is not None:
                        return api_media
                except Exception as e:
                    logger.warning("Authenticated Instagram API fetch failed: %s", e)
            try:
                public_media = _download_public_instagram_media(
                    url,
                    download_dir,
                    cookie_header=instagram_cookie_header,
                )
                if public_media.media_type == "image":
                    return public_media
                if instagram_kind != "post":
                    return public_media
            except Exception as e:
                public_instagram_error = str(e)
                logger.warning("Public Instagram fetch failed: %s", e)

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
            )

    except yt_dlp.utils.DownloadError as e:
        logger.error("yt-dlp download error: %s", e)
        raise Exception(
            _friendly_download_error(
                url=url,
                raw_message=str(e),
                public_instagram_error=public_instagram_error,
            )
        )
    except Exception as e:
        logger.error("Unexpected download error: %s", e)
        raise
    finally:
        if temp_cookie_file:
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
        )

    if video_url:
        destination = os.path.join(download_dir, f"instagram-{uuid4().hex}.mp4")
        _download_remote_file(video_url, destination)
        return DownloadedMedia(
            media_type="video",
            media_paths=[destination],
            caption=caption,
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


def _build_cookie_file_from_env(url: str) -> str | None:
    settings = get_settings()
    platform = _platform_key(url)
    raw_cookies = _decode_cookie_blob(
        encoded=_platform_cookie_value(settings, platform, encoded=True),
        plain=_platform_cookie_value(settings, platform, encoded=False),
        encoded_label=f"{platform.upper()}_COOKIE_DATA_BASE64",
    )

    if not raw_cookies:
        raw_cookies = _decode_cookie_blob(
            encoded=settings.YTDLP_COOKIE_DATA_BASE64,
            plain=settings.YTDLP_COOKIE_DATA,
            encoded_label="YTDLP_COOKIE_DATA_BASE64",
        )

    if not raw_cookies:
        return None

    with tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".txt",
        prefix="yt-dlp-cookies-",
        encoding="utf-8",
    ) as tmp:
        tmp.write(raw_cookies)
        return tmp.name


def _platform_cookie_value(settings, platform: str, *, encoded: bool) -> str | None:
    attr_name = f"{platform.upper()}_COOKIE_DATA_BASE64" if encoded else f"{platform.upper()}_COOKIE_DATA"
    return getattr(settings, attr_name, None)


def _decode_cookie_blob(*, encoded: str | None, plain: str | None, encoded_label: str) -> str | None:
    if encoded:
        try:
            return base64.b64decode(encoded.encode("utf-8")).decode("utf-8")
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
        return "bestaudio[ext=m4a]/bestaudio/best[height<=360]/best"
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
        logger.warning("Failed to parse cookie file %s: %s", cookie_file, e)
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
