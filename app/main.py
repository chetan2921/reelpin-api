import os
import shutil
import logging
import tempfile
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware

from app.models import (
    DevicePushTokenInput,
    EnqueueReelJobInput,
    GenericSuccessResponse,
    ProcessingJobResponse,
    ProcessingJobStatus,
    ProactiveRecallPushRequest,
    ReelInput,
    ReelResponse,
    SearchQuery,
    SearchResponse,
    SearchResult,
    HealthResponse,
)
from app.pipeline import process_reel_pipeline, process_video_pipeline
from app.services.embedder import init_pinecone, search_similar
from app.services.database import (
    create_processing_job,
    delete_reel,
    get_device_push_tokens,
    get_processing_job,
    get_reel,
    get_reels,
    get_reels_by_ids,
    list_processing_jobs,
    upsert_device_push_token,
)
from app.services.notifications import send_push_notification

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

SEARCH_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "at",
    "best",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "my",
    "of",
    "on",
    "or",
    "show",
    "that",
    "the",
    "this",
    "to",
    "top",
    "with",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup."""
    logger.info("🚀 Starting ReelMind API...")
    try:
        init_pinecone()
        logger.info("✅ All services initialized")
    except Exception as e:
        logger.warning(f"⚠️  Service init warning: {e}")
    yield
    logger.info("👋 Shutting down ReelMind API")


app = FastAPI(
    title="ReelMind API",
    description="AI-powered Instagram reel analysis, categorization, and search",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow Flutter app to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
#                          ENDPOINTS
# ============================================================


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse()


@app.post("/api/v1/process-reel", response_model=ReelResponse)
async def process_reel(input_data: ReelInput):
    """
    Process an Instagram reel from URL.

    Full pipeline: download → transcribe → extract → embed → store.
    """
    try:
        logger.info(f"Processing reel: {input_data.url} for user: {input_data.user_id}")
        result = await process_reel_pipeline(
            url=input_data.url,
            user_id=input_data.user_id,
        )
        return result
    except Exception as e:
        logger.error(f"Process reel failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/processing-jobs/reels", response_model=ProcessingJobResponse)
async def enqueue_reel_processing(payload: EnqueueReelJobInput):
    try:
        from app.tasks import process_reel_job

        source_platform = _derive_source_platform(payload.url)
        job = create_processing_job(
            user_id=payload.user_id,
            url=payload.url,
            source_platform=source_platform,
            max_attempts=4,
        )
        process_reel_job.send(job["id"])
        return _db_job_to_response(job)
    except Exception as e:
        logger.error(f"Enqueue reel processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/processing-jobs/{job_id}", response_model=ProcessingJobResponse)
async def get_processing_job_detail(job_id: str):
    try:
        job = get_processing_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Processing job not found")
        return _db_job_to_response(job)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get processing job failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/processing-jobs", response_model=list[ProcessingJobResponse])
async def get_processing_job_list(
    user_id: str = Query(..., description="Filter by user ID"),
    active_only: bool = Query(default=False, description="Only queued/processing jobs"),
    limit: int = Query(default=20, ge=1, le=100, description="Max results"),
):
    try:
        jobs = list_processing_jobs(user_id=user_id, active_only=active_only, limit=limit)
        return [_db_job_to_response(job) for job in jobs]
    except Exception as e:
        logger.error(f"List processing jobs failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/process-video", response_model=ReelResponse)
async def process_video(
    video: UploadFile = File(...),
    url: str = Form(default=""),
    user_id: str = Form(default="default-user"),
):
    """
    Process a directly uploaded video file.

    Use this when URL download fails — user shares the video file instead.
    """
    # Save uploaded file to temp location
    temp_path = None
    try:
        suffix = os.path.splitext(video.filename or ".mp4")[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            shutil.copyfileobj(video.file, tmp)
            temp_path = tmp.name

        logger.info(f"Processing uploaded video: {video.filename} for user: {user_id}")
        result = await process_video_pipeline(
            video_path=temp_path,
            url=url,
            user_id=user_id,
        )
        return result
    except Exception as e:
        logger.error(f"Process video failed: {e}")
        # Clean up on error
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/reels", response_model=list[ReelResponse])
async def list_reels(
    user_id: str = Query(default=None, description="Filter by user ID"),
    category: str = Query(default=None, description="Filter by category"),
    subcategory: str = Query(default=None, description="Filter by subcategory"),
    limit: int = Query(default=50, ge=1, le=100, description="Max results"),
):
    """List saved reels with optional filters."""
    try:
        reels = get_reels(user_id=user_id, category=category, subcategory=subcategory, limit=limit)
        return [_db_record_to_response(r) for r in reels]
    except Exception as e:
        logger.error(f"List reels failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/reels/{reel_id}", response_model=ReelResponse)
async def get_reel_detail(reel_id: str):
    """Get a single reel's full details."""
    try:
        record = get_reel(reel_id)
        if not record:
            raise HTTPException(status_code=404, detail="Reel not found")
        return _db_record_to_response(record)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get reel failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/reels/{reel_id}")
async def remove_reel(reel_id: str):
    """Delete a saved reel."""
    try:
        deleted = delete_reel(reel_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Reel not found")
        return {"message": "Reel deleted", "id": reel_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete reel failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/search", response_model=SearchResponse)
async def search_reels(query: SearchQuery):
    """
    RAG-powered semantic search across saved reels.

    Uses Pinecone vector similarity to find relevant reels.
    """
    try:
        # Search Pinecone for similar vectors
        matches = search_similar(
            query=query.query,
            user_id=query.user_id,
            category=query.category,
            top_k=max(query.limit * 4, 12),
            subcategory=query.subcategory
        )

        if not matches:
            return SearchResponse(query=query.query, results=[], total=0)

        # Fetch full reel data from Supabase
        reel_ids = [m["reel_id"] for m in matches]
        reels = get_reels_by_ids(reel_ids)

        # Map reels by ID for easy lookup
        reel_map = {r["id"]: r for r in reels}

        # Build results in relevance order with a stricter hybrid relevance filter.
        query_tokens = _search_tokens(query.query)
        ranked_results = []
        for match in matches:
            reel_record = reel_map.get(match["reel_id"])
            if reel_record:
                semantic_score = float(match["score"])
                lexical_score = _lexical_score(
                    reel_record,
                    query.query,
                    query_tokens,
                )
                combined_score = round(
                    (semantic_score * 0.72) + (lexical_score * 0.28),
                    4,
                )

                if not _is_relevant_match(
                    semantic_score=semantic_score,
                    lexical_score=lexical_score,
                    normalized_query=query.query.strip().lower(),
                    query_tokens=query_tokens,
                ):
                    continue

                ranked_results.append(
                    (
                        combined_score,
                        SearchResult(
                            reel=_db_record_to_response(reel_record),
                            relevance_score=combined_score,
                        ),
                    )
                )

        ranked_results.sort(key=lambda item: item[0], reverse=True)
        results = [item[1] for item in ranked_results[: query.limit]]

        return SearchResponse(
            query=query.query,
            results=results,
            total=len(results),
        )

    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/device-push-tokens", response_model=GenericSuccessResponse)
async def register_device_push_token(payload: DevicePushTokenInput):
    try:
        upsert_device_push_token(
            user_id=payload.user_id,
            token=payload.token,
            platform=payload.platform,
        )
        return GenericSuccessResponse(message="device token stored")
    except Exception as e:
        logger.error(f"Register device push token failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/proactive-recall/push", response_model=GenericSuccessResponse)
async def send_proactive_recall_push(payload: ProactiveRecallPushRequest):
    try:
        tokens = get_device_push_tokens(payload.user_id)
        delivered = send_push_notification(
            tokens=tokens,
            title=payload.title,
            body=payload.body,
            data=payload.data,
        )
        return GenericSuccessResponse(
            message=f"push sent to {delivered} device(s)",
        )
    except Exception as e:
        logger.error(f"Send proactive recall push failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
#                         HELPERS
# ============================================================


def _db_record_to_response(record: dict) -> ReelResponse:
    """Convert a Supabase DB record dict to a ReelResponse model."""
    return ReelResponse(
        id=record["id"],
        user_id=record.get("user_id", ""),
        url=record.get("url", ""),
        title=record.get("title", ""),
        summary=record.get("summary", ""),
        transcript=record.get("transcript", ""),
        category=record.get("category", "Other"),
        subcategory=record.get("subcategory", "Other"),
        secondary_categories=record.get("secondary_categories", []),
        key_facts=record.get("key_facts", []),
        locations=record.get("locations", []),
        people_mentioned=record.get("people_mentioned", []),
        actionable_items=record.get("actionable_items", []),
        created_at=record.get("created_at"),
    )


def _db_job_to_response(record: dict) -> ProcessingJobResponse:
    reel_record = None
    result_reel_id = record.get("result_reel_id")
    if result_reel_id:
        try:
            reel_record = get_reel(result_reel_id)
        except Exception:
            reel_record = None

    return ProcessingJobResponse(
        id=record["id"],
        user_id=record.get("user_id", ""),
        url=record.get("url", ""),
        source_platform=record.get("source_platform"),
        status=ProcessingJobStatus(record.get("status", "queued")),
        current_step=record.get("current_step"),
        progress_percent=int(record.get("progress_percent", 0) or 0),
        error_message=record.get("error_message"),
        attempt_count=int(record.get("attempt_count", 0) or 0),
        max_attempts=int(record.get("max_attempts", 0) or 0),
        result_reel_id=result_reel_id,
        step_durations=record.get("step_durations", {}) or {},
        created_at=record.get("created_at"),
        updated_at=record.get("updated_at"),
        started_at=record.get("started_at"),
        completed_at=record.get("completed_at"),
        reel=_db_record_to_response(reel_record) if reel_record else None,
    )


def _derive_source_platform(url: str) -> str:
    lowered = url.lower()
    if "instagram.com" in lowered:
        return "instagram"
    if "tiktok.com" in lowered:
        return "tiktok"
    if "youtube.com" in lowered or "youtu.be" in lowered:
        return "youtube"
    return "web"


def _search_tokens(query: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", query.lower())
    return [
        token for token in tokens
        if len(token) > 1 and token not in SEARCH_STOP_WORDS
    ]


def _lexical_score(record: dict, query: str, query_tokens: list[str]) -> float:
    normalized_query = query.strip().lower()
    title = str(record.get("title", "")).lower()
    summary = str(record.get("summary", "")).lower()
    category = str(record.get("category", "")).lower()
    subcategory = str(record.get("subcategory", "")).lower()
    facts = " ".join(record.get("key_facts", [])).lower()
    people = " ".join(record.get("people_mentioned", [])).lower()
    actions = " ".join(record.get("actionable_items", [])).lower()
    locations = " ".join(
        [
            " ".join(
                [
                    str(location.get("name", "")),
                    str(location.get("address", "")),
                    str(location.get("city", "")),
                    str(location.get("state", "")),
                    str(location.get("country", "")),
                ]
            )
            for location in record.get("locations", [])
            if isinstance(location, dict)
        ]
    ).lower()

    score = 0.0
    primary_haystack = " ".join(
        [title, summary, category, subcategory, facts, people, actions, locations]
    )

    if normalized_query and normalized_query in primary_haystack:
        score += 0.55

    for token in query_tokens:
        token_hits = 0
        for haystack in [title, summary, category, subcategory, facts, people, actions, locations]:
            if token in haystack:
                token_hits += 1
        if token_hits:
            score += 0.10 + min(token_hits, 4) * 0.04

    return min(score, 1.0)


def _is_relevant_match(
    *,
    semantic_score: float,
    lexical_score: float,
    normalized_query: str,
    query_tokens: list[str],
) -> bool:
    if len(normalized_query) < 2:
        return False

    if lexical_score >= 0.62:
        return True

    if semantic_score >= 0.76:
        return True

    if semantic_score >= 0.58 and lexical_score >= 0.20:
        return True

    if len(query_tokens) >= 2 and lexical_score >= 0.32:
        return True

    return False
