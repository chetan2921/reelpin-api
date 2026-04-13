import logging
from time import perf_counter
from app.services.downloader import cleanup_file
from app.services.extractor import extract_structured_data
from app.services.embedder import embed_and_store
from app.services.database import (
    get_processing_cache_entry,
    save_reel,
    update_reel_fields,
    upsert_processing_cache_entry,
)
from app.services.processing_cache import (
    build_processing_cache_payload,
    cache_record_to_result,
)
from app.services.processing_metadata import (
    build_direct_upload_metadata,
    build_url_processing_metadata,
    default_url_processing_metadata,
)
from app.services.platform_handlers import (
    PreparedPlatformContent,
    prepare_content_for_source,
)
from app.services.source_identity import resolve_source_identity
from app.models import ReelResponse

logger = logging.getLogger(__name__)


async def process_reel_pipeline_with_metrics(
    url: str,
    user_id: str = "default-user",
    progress_callback=None,
) -> tuple[ReelResponse, dict[str, float]]:
    media_paths: list[str] = []
    step_durations: dict[str, float] = {}
    source = resolve_source_identity(url)
    normalized_url = source.normalized_url
    cached_result = None
    prepared_content: PreparedPlatformContent | None = None
    processing_metadata = None

    def _mark(step: str, progress: int, extra: dict | None = None) -> None:
        if progress_callback is not None:
            progress_callback(step, progress, extra or {})

    try:
        if source.source_content_id:
            _mark("checking_cache", 8)
            started = perf_counter()
            try:
                cached_record = get_processing_cache_entry(
                    source_platform=source.source_platform,
                    source_content_id=source.source_content_id,
                )
                cached_result = cache_record_to_result(cached_record)
            except Exception as e:
                logger.warning(
                    "Processing cache lookup skipped for %s/%s: %s",
                    source.source_platform,
                    source.source_content_id,
                    e,
                )
            step_durations["cache_lookup_seconds"] = round(perf_counter() - started, 3)

        if cached_result:
            default_processing_metadata = default_url_processing_metadata(source)
            transcript_text = cached_result.transcript
            caption = cached_result.caption
            extracted = cached_result.extracted
            processing_metadata = {
                "normalized_url": cached_result.normalized_url or default_processing_metadata["normalized_url"],
                "source_platform": cached_result.source_platform or source.source_platform,
                "source_content_type": cached_result.source_content_type or source.source_content_type,
                "source_content_id": cached_result.source_content_id or source.source_content_id,
                "processing_version": cached_result.processing_version or default_processing_metadata["processing_version"],
                "ingestion_method": cached_result.ingestion_method or default_processing_metadata["ingestion_method"],
                "transcript_source": cached_result.transcript_source or default_processing_metadata["transcript_source"],
            }
            step_durations["download_seconds"] = 0.0
            step_durations["transcribe_seconds"] = 0.0
            step_durations["extract_seconds"] = 0.0
            logger.info(
                "[Pipeline] Cache hit for %s/%s",
                source.source_platform,
                source.source_content_id,
            )
        else:
            prepared_content = prepare_content_for_source(
                source,
                mark=_mark,
            )
            media_paths = prepared_content.media_paths
            transcript_text = prepared_content.transcript_text
            caption = prepared_content.caption
            step_durations["download_seconds"] = prepared_content.step_durations["download_seconds"]
            logger.info(f"[Pipeline] Step 1/5 complete in {step_durations['download_seconds']}s")
            step_durations["transcribe_seconds"] = prepared_content.step_durations["transcribe_seconds"]
            logger.info(f"[Pipeline] Step 2/5 complete in {step_durations['transcribe_seconds']}s")

            _mark("extracting", 58)
            started = perf_counter()
            extracted = extract_structured_data(transcript=transcript_text, caption=caption)
            step_durations["extract_seconds"] = round(perf_counter() - started, 3)
            logger.info(f"[Pipeline] Step 3/5 complete in {step_durations['extract_seconds']}s")

            processing_metadata = build_url_processing_metadata(
                source,
                ingestion_method=prepared_content.ingestion_method,
                transcript_source=prepared_content.transcript_source,
            )

            if source.source_content_id:
                started = perf_counter()
                try:
                    upsert_processing_cache_entry(
                        build_processing_cache_payload(
                            source_platform=source.source_platform,
                            source_content_id=source.source_content_id,
                            source_content_type=source.source_content_type,
                            normalized_url=normalized_url,
                            processing_version=processing_metadata["processing_version"],
                            ingestion_method=processing_metadata["ingestion_method"],
                            transcript_source=processing_metadata["transcript_source"],
                            transcript=transcript_text,
                            caption=caption,
                            extracted=extracted,
                        )
                    )
                    logger.info(
                        "[Pipeline] Stored processing cache for %s/%s",
                        source.source_platform,
                        source.source_content_id,
                    )
                except Exception as e:
                    logger.warning(
                        "Processing cache write skipped for %s/%s: %s",
                        source.source_platform,
                        source.source_content_id,
                        e,
                    )
                step_durations["cache_write_seconds"] = round(perf_counter() - started, 3)

        _mark("saving", 76)
        started = perf_counter()
        reel_data = {
            "user_id": user_id,
            "url": normalized_url,
            "normalized_url": processing_metadata["normalized_url"] if processing_metadata else normalized_url,
            "source_platform": processing_metadata["source_platform"] if processing_metadata else source.source_platform,
            "source_content_type": processing_metadata["source_content_type"] if processing_metadata else source.source_content_type,
            "source_content_id": processing_metadata["source_content_id"] if processing_metadata else source.source_content_id,
            "processing_version": processing_metadata["processing_version"] if processing_metadata else None,
            "ingestion_method": processing_metadata["ingestion_method"] if processing_metadata else None,
            "transcript_source": processing_metadata["transcript_source"] if processing_metadata else None,
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
        if saved_record.get("pinecone_id"):
            step_durations["embed_seconds"] = 0.0
            logger.info("[Pipeline] Skipped embedding because reel %s is already indexed", reel_id)
        else:
            pinecone_id = embed_and_store(
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
            update_reel_fields(reel_id, {"pinecone_id": pinecone_id})
            step_durations["embed_seconds"] = round(perf_counter() - started, 3)
        logger.info(f"[Pipeline] Step 5/5 complete in {step_durations['embed_seconds']}s")

        _mark("completed", 100)
        return (
            ReelResponse(
                id=reel_id,
                user_id=user_id,
                url=normalized_url,
                normalized_url=saved_record.get("normalized_url"),
                source_platform=saved_record.get("source_platform"),
                source_content_type=saved_record.get("source_content_type"),
                source_content_id=saved_record.get("source_content_id"),
                processing_version=saved_record.get("processing_version"),
                ingestion_method=saved_record.get("ingestion_method"),
                transcript_source=saved_record.get("transcript_source"),
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
        metadata = build_direct_upload_metadata(url)
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
            "url": metadata["normalized_url"],
            "normalized_url": metadata["normalized_url"],
            "source_platform": metadata["source_platform"],
            "source_content_type": metadata["source_content_type"],
            "source_content_id": metadata["source_content_id"],
            "processing_version": metadata["processing_version"],
            "ingestion_method": metadata["ingestion_method"],
            "transcript_source": metadata["transcript_source"],
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
        if saved_record.get("pinecone_id"):
            logger.info("[Pipeline] Skipped embedding because reel %s is already indexed", reel_id)
        else:
            pinecone_id = embed_and_store(
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
            update_reel_fields(reel_id, {"pinecone_id": pinecone_id})

        logger.info(f"[Pipeline] ✅ Complete! Reel saved with ID: {reel_id}")

        return ReelResponse(
            id=reel_id,
            user_id=user_id,
            url=metadata["normalized_url"],
            normalized_url=saved_record.get("normalized_url"),
            source_platform=saved_record.get("source_platform"),
            source_content_type=saved_record.get("source_content_type"),
            source_content_id=saved_record.get("source_content_id"),
            processing_version=saved_record.get("processing_version"),
            ingestion_method=saved_record.get("ingestion_method"),
            transcript_source=saved_record.get("transcript_source"),
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
