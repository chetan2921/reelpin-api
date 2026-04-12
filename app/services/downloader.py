import html
import json
import logging
import os
import re
import shutil
import urllib.error
import urllib.parse
import urllib.request
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


def download_reel(url: str) -> tuple[str, str]:
    """
    Download a short-form video from a supported URL.

    Args:
        url: Shared Instagram, TikTok, or YouTube Shorts URL

    Returns:
        Tuple of (Path to the downloaded video file, Caption/Description text)

    Raises:
        Exception: If download fails
    """
    settings = get_settings()
    download_dir = settings.TEMP_DOWNLOAD_DIR
    os.makedirs(download_dir, exist_ok=True)
    public_instagram_error = None

    # Create a unique temp file path
    output_path = os.path.join(download_dir, f"%(id)s.%(ext)s")

    ydl_opts = {
        "outtmpl": output_path,
        "format": "best",
        "quiet": True,
        "no_warnings": True,
        # Extract audio-compatible format
        "postprocessors": [],
    }

    if settings.INSTAGRAM_COOKIES_FILE:
        ydl_opts["cookiefile"] = settings.INSTAGRAM_COOKIES_FILE

    if settings.YTDLP_COOKIES_FROM_BROWSER:
        ydl_opts["cookiesfrombrowser"] = (settings.YTDLP_COOKIES_FROM_BROWSER,)

    try:
        if _is_instagram_url(url):
            try:
                return _download_public_instagram_media(url, download_dir)
            except Exception as e:
                public_instagram_error = str(e)
                logger.warning("Public Instagram fetch failed: %s", e)

        logger.info(f"Downloading reel from: {url}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            # Construct the actual downloaded file path
            downloaded_file = ydl.prepare_filename(info)

            if not os.path.exists(downloaded_file):
                raise FileNotFoundError(
                    f"Download completed but file not found at {downloaded_file}"
                )

            caption = info.get("description", "")
            logger.info(f"Downloaded reel to: {downloaded_file}")
            return downloaded_file, caption

    except yt_dlp.utils.DownloadError as e:
        logger.error(f"yt-dlp download error: {e}")
        message = str(e)
        lowered = message.lower()

        if public_instagram_error and "image-only" in public_instagram_error.lower():
            raise Exception(public_instagram_error)

        if "login required" in lowered or "cookies" in lowered:
            raise Exception(
                "Instagram blocked this reel for anonymous download. "
                "Add Instagram cookies to the backend or try again later."
            )

        if "rate-limit" in lowered or "rate limit" in lowered:
            raise Exception(
                "Instagram rate limited the downloader. "
                "Try again in a few minutes or use authenticated cookies."
            )

        if "private" in lowered:
            raise Exception(
                "This Instagram post is private and cannot be downloaded by the backend."
            )

        raise Exception(
            "Failed to download this reel. It may be private, unavailable, "
            "or temporarily blocked by Instagram."
        )
    except Exception as e:
        logger.error(f"Unexpected download error: {e}")
        raise


def _is_instagram_url(url: str) -> bool:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    return "instagram.com" in host


def _download_public_instagram_media(
    url: str, download_dir: str
) -> tuple[str, str]:
    logger.info("Trying public Instagram page fetch for: %s", url)
    request = urllib.request.Request(url, headers=_BROWSER_HEADERS)

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            page = response.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        raise Exception(f"Instagram page fetch returned HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise Exception("Instagram page fetch failed before yt-dlp could run") from e

    video_url = (
        _extract_meta_content(page, "og:video:secure_url")
        or _extract_meta_content(page, "og:video")
        or _extract_embedded_video_url(page)
    )

    if not video_url:
        if _extract_meta_content(page, "og:image"):
            raise Exception(
                "This Instagram post appears to be image-only. ReelPin "
                "currently supports video reels, shorts, and video posts."
            )
        raise Exception("Instagram did not expose a public video URL for this page.")

    caption = (
        _extract_meta_content(page, "og:description")
        or _extract_meta_content(page, "description", key="name")
        or ""
    )

    destination = os.path.join(download_dir, f"instagram-{uuid4().hex}.mp4")
    media_request = urllib.request.Request(video_url, headers=_BROWSER_HEADERS)

    try:
        with urllib.request.urlopen(media_request, timeout=30) as response, open(
            destination, "wb"
        ) as file_handle:
            shutil.copyfileobj(response, file_handle)
    except urllib.error.HTTPError as e:
        raise Exception(f"Instagram media fetch returned HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise Exception("Instagram media download failed") from e

    if not os.path.exists(destination):
        raise FileNotFoundError(
            f"Instagram media download completed but file not found at {destination}"
        )

    logger.info("Downloaded Instagram media via public page fetch: %s", destination)
    return destination, caption


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


def _extract_embedded_video_url(document: str) -> str | None:
    match = re.search(r'"video_url":"([^"]+)"', document)
    if not match:
        return None

    try:
        return html.unescape(json.loads(f'"{match.group(1)}"'))
    except json.JSONDecodeError:
        return html.unescape(match.group(1).replace("\\/", "/"))


def cleanup_file(file_path: str) -> None:
    """Remove a temporary file after processing."""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up temp file: {file_path}")
    except OSError as e:
        logger.warning(f"Failed to clean up {file_path}: {e}")
