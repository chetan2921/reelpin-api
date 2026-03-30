import logging
import uuid
from app.services.downloader import download_reel, cleanup_file
from app.services.transcriber import transcribe_audio
from app.services.extractor import extract_structured_data
from app.services.embedder import embed_and_store
from app.services.database import save_reel
from app.models import ReelResponse

logger = logging.getLogger(__name__)


async def process_reel_pipeline(url: str, user_id: str = "default-user") -> ReelResponse:
    """
    Full processing pipeline: download → transcribe → extract → embed → store.

    Args:
        url: Instagram reel URL
        user_id: User identifier

    Returns:
        ReelResponse with all processed data
    """
    video_path = None

    try:
        # Step 1: Download the reel and extract caption
        logger.info(f"[Pipeline] Step 1/5: Downloading reel from {url}")
        video_path, caption = download_reel(url)

        # Step 2: Transcribe audio
        logger.info("[Pipeline] Step 2/5: Transcribing audio...")
        transcript_result = transcribe_audio(video_path)
        transcript_text = transcript_result["text"]

        # Step 3: Extract structured data
        logger.info("[Pipeline] Step 3/5: Extracting structured data with AI...")
        extracted = extract_structured_data(transcript=transcript_text, caption=caption)

        # Step 4: Store in database
        logger.info("[Pipeline] Step 4/5: Saving to database...")
        reel_data = {
            "user_id": user_id,
            "url": url,
            "title": extracted.title,
            "summary": extracted.summary,
            "transcript": transcript_text,
            "category": extracted.category,
            "secondary_categories": extracted.secondary_categories,
            "key_facts": extracted.key_facts,
            "locations": [loc.model_dump() for loc in extracted.locations],
            "people_mentioned": extracted.people_mentioned,
            "actionable_items": extracted.actionable_items,
        }

        saved_record = save_reel(reel_data)
        reel_id = saved_record["id"]

        # Step 5: Index in Pinecone for RAG search
        logger.info("[Pipeline] Step 5/5: Indexing for semantic search...")
        
        sec_cats = ", ".join(extracted.secondary_categories)
        search_text = f"{extracted.title}. {extracted.summary}. Primary Category: {extracted.category}. Secondary Categories: {sec_cats}. Caption: {caption}. Transcript: {transcript_text}"
        embed_and_store(
            reel_id=reel_id,
            text=search_text,
            metadata={
                "user_id": user_id,
                "title": extracted.title,
                "category": extracted.category,
                "summary": extracted.summary,
            },
        )

        # Update the record with the pinecone ID
        # (pinecone_id == reel_id in our case)

        logger.info(f"[Pipeline] ✅ Complete! Reel saved with ID: {reel_id}")

        return ReelResponse(
            id=reel_id,
            user_id=user_id,
            url=url,
            title=extracted.title,
            summary=extracted.summary,
            transcript=transcript_text,
            category=extracted.category,
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
        # Always clean up the downloaded video
        if video_path:
            cleanup_file(video_path)


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
        search_text = f"{extracted.title}. {extracted.summary}. Primary Category: {extracted.category}. Secondary Categories: {sec_cats}. Transcript: {transcript_text}"
        embed_and_store(
            reel_id=reel_id,
            text=search_text,
            metadata={
                "user_id": user_id,
                "title": extracted.title,
                "category": extracted.category,
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
