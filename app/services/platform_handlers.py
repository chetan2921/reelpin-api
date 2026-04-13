from dataclasses import dataclass
import logging
from time import perf_counter

from app.services.source_identity import SourceIdentity

logger = logging.getLogger(__name__)


@dataclass
class PreparedPlatformContent:
    transcript_text: str
    caption: str
    media_paths: list[str]
    ingestion_method: str
    transcript_source: str
    step_durations: dict[str, float]
    cookie_slot_index: int | None = None


def prepare_content_for_source(
    source: SourceIdentity,
    *,
    mark,
) -> PreparedPlatformContent:
    handler_key = platform_handler_key(source)
    if handler_key == "instagram_reel":
        return _prepare_instagram_reel(source, mark=mark)
    if handler_key == "instagram_post":
        return _prepare_instagram_post(source, mark=mark)
    if handler_key == "youtube_short":
        return _prepare_youtube_short(source, mark=mark)
    if handler_key == "youtube_video":
        return _prepare_youtube_video(source, mark=mark)
    if handler_key == "tiktok":
        return _prepare_tiktok(source, mark=mark)
    return _prepare_generic_web(source, mark=mark)


def platform_handler_key(source: SourceIdentity) -> str:
    if source.source_platform == "instagram" and source.source_content_type == "reel":
        return "instagram_reel"
    if source.source_platform == "instagram" and source.source_content_type in {"post", "page"}:
        return "instagram_post"
    if source.source_platform == "youtube" and source.source_content_type == "short":
        return "youtube_short"
    if source.source_platform == "youtube":
        return "youtube_video"
    if source.source_platform == "tiktok":
        return "tiktok"
    return "web"


def _prepare_instagram_reel(source: SourceIdentity, *, mark) -> PreparedPlatformContent:
    downloaded, download_seconds = _download_platform_media(source, mark=mark)
    transcript_text, transcribe_seconds = _prepare_downloaded_video(downloaded, mark=mark)
    return PreparedPlatformContent(
        transcript_text=transcript_text,
        caption=downloaded.caption,
        media_paths=downloaded.media_paths,
        ingestion_method="instagram_reel_pipeline",
        transcript_source="groq_whisper",
        step_durations={
            "download_seconds": download_seconds,
            "transcribe_seconds": transcribe_seconds,
        },
        cookie_slot_index=downloaded.cookie_slot_index,
    )


def _prepare_instagram_post(source: SourceIdentity, *, mark) -> PreparedPlatformContent:
    downloaded, download_seconds = _download_platform_media(source, mark=mark)
    if downloaded.media_type == "image":
        transcript_text, second_step_seconds = _prepare_downloaded_images(downloaded, mark=mark)
        transcript_source = "groq_vision_ocr"
    else:
        transcript_text, second_step_seconds = _prepare_downloaded_video(downloaded, mark=mark)
        transcript_source = "groq_whisper"
    return PreparedPlatformContent(
        transcript_text=transcript_text,
        caption=downloaded.caption,
        media_paths=downloaded.media_paths,
        ingestion_method="instagram_post_pipeline",
        transcript_source=transcript_source,
        step_durations={
            "download_seconds": download_seconds,
            "transcribe_seconds": second_step_seconds,
        },
        cookie_slot_index=downloaded.cookie_slot_index,
    )


def _prepare_youtube_short(source: SourceIdentity, *, mark) -> PreparedPlatformContent:
    from app.services.youtube_transcripts import fetch_youtube_transcript

    mark("downloading", 12)
    started = perf_counter()
    transcript_text, caption = fetch_youtube_transcript(source.normalized_url)
    download_seconds = round(perf_counter() - started, 3)
    mark("transcribing", 36)
    return PreparedPlatformContent(
        transcript_text=transcript_text,
        caption=caption,
        media_paths=[],
        ingestion_method="youtube_short_pipeline",
        transcript_source="youtube_transcript_api",
        step_durations={
            "download_seconds": download_seconds,
            "transcribe_seconds": 0.0,
        },
    )


def _prepare_youtube_video(source: SourceIdentity, *, mark) -> PreparedPlatformContent:
    from app.services.youtube_transcripts import fetch_youtube_transcript

    mark("downloading", 12)
    started = perf_counter()
    transcript_text, caption = fetch_youtube_transcript(source.normalized_url)
    download_seconds = round(perf_counter() - started, 3)
    mark("transcribing", 36)
    return PreparedPlatformContent(
        transcript_text=transcript_text,
        caption=caption,
        media_paths=[],
        ingestion_method="youtube_video_pipeline",
        transcript_source="youtube_transcript_api",
        step_durations={
            "download_seconds": download_seconds,
            "transcribe_seconds": 0.0,
        },
    )


def _prepare_tiktok(source: SourceIdentity, *, mark) -> PreparedPlatformContent:
    downloaded, download_seconds = _download_platform_media(source, mark=mark)
    if downloaded.media_type == "image":
        transcript_text, second_step_seconds = _prepare_downloaded_images(downloaded, mark=mark)
        transcript_source = "groq_vision_ocr"
    else:
        transcript_text, second_step_seconds = _prepare_downloaded_video(downloaded, mark=mark)
        transcript_source = "groq_whisper"
    return PreparedPlatformContent(
        transcript_text=transcript_text,
        caption=downloaded.caption,
        media_paths=downloaded.media_paths,
        ingestion_method="tiktok_pipeline",
        transcript_source=transcript_source,
        step_durations={
            "download_seconds": download_seconds,
            "transcribe_seconds": second_step_seconds,
        },
        cookie_slot_index=downloaded.cookie_slot_index,
    )


def _prepare_generic_web(source: SourceIdentity, *, mark) -> PreparedPlatformContent:
    downloaded, download_seconds = _download_platform_media(source, mark=mark)
    if downloaded.media_type == "image":
        transcript_text, second_step_seconds = _prepare_downloaded_images(downloaded, mark=mark)
        transcript_source = "groq_vision_ocr"
    else:
        transcript_text, second_step_seconds = _prepare_downloaded_video(downloaded, mark=mark)
        transcript_source = "groq_whisper"
    return PreparedPlatformContent(
        transcript_text=transcript_text,
        caption=downloaded.caption,
        media_paths=downloaded.media_paths,
        ingestion_method="web_pipeline",
        transcript_source=transcript_source,
        step_durations={
            "download_seconds": download_seconds,
            "transcribe_seconds": second_step_seconds,
        },
        cookie_slot_index=downloaded.cookie_slot_index,
    )


def _download_platform_media(source: SourceIdentity, *, mark):
    from app.services.downloader import download_media

    mark("downloading", 12)
    started = perf_counter()
    downloaded = download_media(source.normalized_url)
    if downloaded.cookie_slot_index is not None:
        mark("downloading", 12, {"cookie_slot_index": downloaded.cookie_slot_index})
    return downloaded, round(perf_counter() - started, 3)


def _prepare_downloaded_video(downloaded, *, mark) -> tuple[str, float]:
    from app.services.transcriber import transcribe_audio

    mark("transcribing", 36)
    started = perf_counter()
    transcript_result = transcribe_audio(downloaded.media_paths[0])
    transcript_text = transcript_result["text"]
    return transcript_text, round(perf_counter() - started, 3)


def _prepare_downloaded_images(downloaded, *, mark) -> tuple[str, float]:
    from app.services.image_text import extract_text_from_images

    mark("ocr", 36)
    started = perf_counter()
    transcript_text = extract_text_from_images(downloaded.media_paths)
    if not transcript_text.strip():
        transcript_text = "(No readable text extracted from the image post.)"
    return transcript_text, round(perf_counter() - started, 3)
