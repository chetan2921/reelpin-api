from app.services.platform_handlers import platform_handler_key
from app.services.source_identity import SourceIdentity, resolve_source_identity

PROCESSING_VERSION = "reelpin-v1"


def build_url_processing_metadata(
    source: SourceIdentity,
    *,
    ingestion_method: str,
    transcript_source: str,
) -> dict:
    return {
        "normalized_url": source.normalized_url,
        "source_platform": source.source_platform,
        "source_content_type": source.source_content_type,
        "source_content_id": source.source_content_id,
        "processing_version": PROCESSING_VERSION,
        "ingestion_method": ingestion_method,
        "transcript_source": transcript_source,
    }


def build_direct_upload_metadata(url: str) -> dict:
    raw_url = (url or "").strip()
    if raw_url:
        try:
            source = resolve_source_identity(raw_url)
            return {
                "normalized_url": source.normalized_url,
                "source_platform": source.source_platform,
                "source_content_type": source.source_content_type,
                "source_content_id": source.source_content_id,
                "processing_version": PROCESSING_VERSION,
                "ingestion_method": "direct_upload",
                "transcript_source": "groq_whisper",
            }
        except Exception:
            pass

    return {
        "normalized_url": "direct-upload",
        "source_platform": "upload",
        "source_content_type": "video",
        "source_content_id": None,
        "processing_version": PROCESSING_VERSION,
        "ingestion_method": "direct_upload",
        "transcript_source": "groq_whisper",
    }


def default_url_processing_metadata(source: SourceIdentity) -> dict:
    handler_key = platform_handler_key(source)
    defaults = {
        "instagram_reel": ("instagram_reel_pipeline", "groq_whisper"),
        "instagram_post": ("instagram_post_pipeline", "groq_vision_ocr"),
        "youtube_short": ("youtube_short_pipeline", "youtube_transcript_api"),
        "youtube_video": ("youtube_video_pipeline", "youtube_transcript_api"),
        "tiktok": ("tiktok_pipeline", "groq_whisper"),
        "web": ("web_pipeline", "groq_whisper"),
    }
    ingestion_method, transcript_source = defaults.get(
        handler_key,
        ("web_pipeline", "groq_whisper"),
    )
    return build_url_processing_metadata(
        source,
        ingestion_method=ingestion_method,
        transcript_source=transcript_source,
    )
