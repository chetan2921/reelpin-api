import json
import logging
import urllib.parse
import urllib.request

from youtube_transcript_api import YouTubeTranscriptApi

logger = logging.getLogger(__name__)

_PREFERRED_LANGUAGES = ["en", "en-US", "hi", "hi-IN"]


def is_youtube_url(url: str) -> bool:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    return "youtube.com" in host or "youtu.be" in host


def fetch_youtube_transcript(url: str) -> tuple[str, str]:
    video_id = extract_youtube_video_id(url)
    if not video_id:
        raise Exception("Could not determine the YouTube video id from this URL.")

    logger.info("Fetching YouTube transcript for video id: %s", video_id)
    api = YouTubeTranscriptApi()
    transcript_list = api.list(video_id)

    transcript = _pick_transcript(transcript_list)
    fetched = transcript.fetch()
    transcript_text = " ".join(
        snippet.text.strip()
        for snippet in fetched
        if getattr(snippet, "text", "").strip()
    ).strip()

    if not transcript_text:
        raise Exception("YouTube transcript was empty for this video.")

    caption = _fetch_youtube_context(url)
    logger.info("Fetched YouTube transcript: %s chars", len(transcript_text))
    return transcript_text, caption


def extract_youtube_video_id(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    host = (parsed.hostname or "").lower()

    if host == "youtu.be":
        path_parts = [part for part in parsed.path.split("/") if part]
        return path_parts[0] if path_parts else None

    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
        return path_parts[1]

    query = urllib.parse.parse_qs(parsed.query)
    video_ids = query.get("v")
    if video_ids:
        return video_ids[0]

    return None


def _pick_transcript(transcript_list):
    try:
        return transcript_list.find_transcript(_PREFERRED_LANGUAGES)
    except Exception:
        pass

    try:
        return transcript_list.find_generated_transcript(_PREFERRED_LANGUAGES)
    except Exception:
        pass

    for transcript in transcript_list:
        if getattr(transcript, "is_translatable", False):
            try:
                return transcript.translate("en")
            except Exception:
                continue

    for transcript in transcript_list:
        return transcript

    raise Exception("No usable YouTube transcript was available for this video.")


def _fetch_youtube_context(url: str) -> str:
    oembed_url = "https://www.youtube.com/oembed?" + urllib.parse.urlencode(
        {
            "url": url,
            "format": "json",
        }
    )

    try:
        request = urllib.request.Request(
            oembed_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
            },
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        title = str(payload.get("title") or "").strip()
        author_name = str(payload.get("author_name") or "").strip()
        if title and author_name:
            return f"YouTube title: {title}. Channel: {author_name}."
        if title:
            return f"YouTube title: {title}."
    except Exception as e:
        logger.info("YouTube oEmbed context lookup skipped: %s", e)

    return ""
