import logging
from time import perf_counter
from app.services.downloader import cleanup_file, download_media
from app.services.image_text import extract_text_from_images
from app.services.transcriber import transcribe_audio
from app.services.extractor import extract_structured_data
from app.services.embedder import embed_and_store
from app.services.database import save_reel
from app.models import ReelResponse
from app.services.youtube_transcripts import (
    fetch_youtube_transcript,
    is_youtube_url,
)

logger = logging.getLogger(__name__)


async def process_reel_pipeline_with_metrics(
    url: str,
    user_id: str = "default-user",
    progress_callback=None,
) -> tuple[ReelResponse, dict[str, float]]:
    media_paths: list[str] = []
    step_durations: dict[str, float] = {}

    def _mark(step: str, progress: int) -> None:
        if progress_callback is not None:
            progress_callback(step, progress)

    try:
        _mark("downloading", 12)
        started = perf_counter()
        if is_youtube_url(url):
            downloaded = None
            transcript_text, caption = fetch_youtube_transcript(url)
        else:
            downloaded = download_media(url)
            media_paths = downloaded.media_paths
            caption = downloaded.caption
        step_durations["download_seconds"] = round(perf_counter() - started, 3)
        logger.info(f"[Pipeline] Step 1/5 complete in {step_durations['download_seconds']}s")

        _mark("transcribing", 36)
        started = perf_counter()
        if downloaded is None:
            step_durations["transcribe_seconds"] = 0.0
        elif downloaded.media_type == "video":
            transcript_result = transcribe_audio(downloaded.media_paths[0])
            transcript_text = transcript_result["text"]
            step_durations["transcribe_seconds"] = round(perf_counter() - started, 3)
        else:
            transcript_text = extract_text_from_images(downloaded.media_paths)
            if not transcript_text.strip():
                transcript_text = "(No readable text extracted from the image post.)"
            step_durations["transcribe_seconds"] = round(perf_counter() - started, 3)
        logger.info(f"[Pipeline] Step 2/5 complete in {step_durations['transcribe_seconds']}s")

        _mark("extracting", 58)
        started = perf_counter()
        extracted = extract_structured_data(transcript=transcript_text, caption=caption)
        step_durations["extract_seconds"] = round(perf_counter() - started, 3)
        logger.info(f"[Pipeline] Step 3/5 complete in {step_durations['extract_seconds']}s")

        _mark("saving", 76)
        started = perf_counter()
        reel_data = {
            "user_id": user_id,
            "url": url,
            "title": extracted.title,
            "summary": extracted.summary,
            "transcript": transcript_text,
            "category": extracted.category,
            "subcategory": extracted.subcategory,
            "secondary_categories": extracted.secondary_categories,
            "key_facts": extracted.key_facts,
            "locations": [loc.model_dump() for loc in extracted.locations],
            "people_mentioned": extracted.people_mentioned,
            "actionable_items": extracted.actionable_items,
        }
        saved_record = save_reel(reel_data)
        reel_id = saved_record["id"]
        step_durations["save_seconds"] = round(perf_counter() - started, 3)
        logger.info(f"[Pipeline] Step 4/5 complete in {step_durations['save_seconds']}s")

        _mark("embedding", 92)
        started = perf_counter()
        sec_cats = ", ".join(extracted.secondary_categories)
        search_text = (
            f"{extracted.title}. {extracted.summary}. "
            f"Primary Category: {extracted.category}. "
            f"Subcategory: {extracted.subcategory}. "
            f"Secondary Categories: {sec_cats}. "
            f"Caption: {caption}. Transcript: {transcript_text}"
        )
        embed_and_store(
            reel_id=reel_id,
            text=search_text,
            metadata={
                "user_id": user_id,
                "title": extracted.title,
                "category": extracted.category,
                "subcategory": extracted.subcategory,
                "summary": extracted.summary,
            },
        )
        step_durations["embed_seconds"] = round(perf_counter() - started, 3)
        logger.info(f"[Pipeline] Step 5/5 complete in {step_durations['embed_seconds']}s")

        _mark("completed", 100)
        return (
            ReelResponse(
                id=reel_id,
                user_id=user_id,
                url=url,
                title=extracted.title,
                summary=extracted.summary,
                transcript=transcript_text,
                category=extracted.category,
                subcategory=extracted.subcategory,
                secondary_categories=extracted.secondary_categories,
                key_facts=extracted.key_facts,
                locations=extracted.locations,
                people_mentioned=extracted.people_mentioned,
                actionable_items=extracted.actionable_items,
                created_at=saved_record.get("created_at"),
            ),
            step_durations,
        )
    except Exception as e:
        logger.error(f"[Pipeline] ❌ Failed: {e}")
        raise
    finally:
        for media_path in media_paths:
            cleanup_file(media_path)


async def process_reel_pipeline(url: str, user_id: str = "default-user") -> ReelResponse:
    """
    Full processing pipeline: download → transcribe → extract → embed → store.

    Args:
        url: Instagram reel URL
        user_id: User identifier

    Returns:
        ReelResponse with all processed data
    """
    result, _ = await process_reel_pipeline_with_metrics(url=url, user_id=user_id)
    return result


async def process_video_pipeline(
    video_path: str, url: str, user_id: str = "default-user"
) -> ReelResponse:
    """
    Process a directly uploaded video file (skips download step).

    Args:
        video_path: Path to the uploaded video file
        url: Original URL (can be empty for direct uploads)
        user_id: User identifier

    Returns:
        ReelResponse with all processed data
    """
    try:
        # Step 1: Transcribe audio
        logger.info("[Pipeline] Step 1/4: Transcribing audio from uploaded video...")
        transcript_result = transcribe_audio(video_path)
        transcript_text = transcript_result["text"]

        # Step 2: Extract structured data (no caption extracted from direct uploads currently)
        logger.info("[Pipeline] Step 2/4: Extracting structured data with AI...")
        extracted = extract_structured_data(transcript=transcript_text, caption="(direct upload)")

        # Step 3: Store in database
        logger.info("[Pipeline] Step 3/4: Saving to database...")
        reel_data = {
            "user_id": user_id,
            "url": url or "direct-upload",
            "title": extracted.title,
            "summary": extracted.summary,
            "transcript": transcript_text,
            "category": extracted.category,
            "subcategory": extracted.subcategory,
            "secondary_categories": extracted.secondary_categories,
            "key_facts": extracted.key_facts,
            "locations": [loc.model_dump() for loc in extracted.locations],
            "people_mentioned": extracted.people_mentioned,
            "actionable_items": extracted.actionable_items,
        }

        saved_record = save_reel(reel_data)
        reel_id = saved_record["id"]

        # Step 4: Index in Pinecone
        logger.info("[Pipeline] Step 4/4: Indexing for semantic search...")
        
        sec_cats = ", ".join(extracted.secondary_categories)
        search_text = f"{extracted.title}. {extracted.summary}. Primary Category: {extracted.category}. Subcategory: {extracted.subcategory}. Secondary Categories: {sec_cats}. Transcript: {transcript_text}"
        embed_and_store(
            reel_id=reel_id,
            text=search_text,
            metadata={
                "user_id": user_id,
                "title": extracted.title,
                "category": extracted.category,
                "subcategory": extracted.subcategory,
                "summary": extracted.summary,
            },
        )

        logger.info(f"[Pipeline] ✅ Complete! Reel saved with ID: {reel_id}")

        return ReelResponse(
            id=reel_id,
            user_id=user_id,
            url=url or "direct-upload",
            title=extracted.title,
            summary=extracted.summary,
            transcript=transcript_text,
            category=extracted.category,
            subcategory=extracted.subcategory,
            secondary_categories=extracted.secondary_categories,
            key_facts=extracted.key_facts,
            locations=extracted.locations,
            people_mentioned=extracted.people_mentioned,
            actionable_items=extracted.actionable_items,
            created_at=saved_record.get("created_at"),
        )

    except Exception as e:
        logger.error(f"[Pipeline] ❌ Failed: {e}")
        raise

    finally:
        cleanup_file(video_path)
