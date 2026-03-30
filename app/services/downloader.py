import os
import tempfile
import logging
import yt_dlp
from app.config import get_settings

logger = logging.getLogger(__name__)


def download_reel(url: str) -> tuple[str, str]:
    """
    Download an Instagram reel video from URL using yt-dlp.

    Args:
        url: Instagram reel URL

    Returns:
        Tuple of (Path to the downloaded video file, Caption/Description text)

    Raises:
        Exception: If download fails
    """
    settings = get_settings()
    download_dir = settings.TEMP_DOWNLOAD_DIR
    os.makedirs(download_dir, exist_ok=True)

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

    try:
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
        raise Exception(
            f"Failed to download reel. It may be private or the URL is invalid. Error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected download error: {e}")
        raise


def cleanup_file(file_path: str) -> None:
    """Remove a temporary file after processing."""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up temp file: {file_path}")
    except OSError as e:
        logger.warning(f"Failed to clean up {file_path}: {e}")
