import os
import shutil
import logging
import tempfile
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.models import (
    ReelInput,
    ReelResponse,
    SearchQuery,
    SearchResponse,
    SearchResult,
    HealthResponse,
)
from app.pipeline import process_reel_pipeline, process_video_pipeline
from app.services.embedder import init_pinecone, search_similar
from app.services.database import get_reel, get_reels, delete_reel, get_reels_by_ids

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


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
            # Pinecone specific subcategory filtering can be mapped if search_similar is updated
            # For now passing it as keyword kwargs if available
            top_k=query.limit,
            subcategory=query.subcategory
        )

        if not matches:
            return SearchResponse(query=query.query, results=[], total=0)

        # Fetch full reel data from Supabase
        reel_ids = [m["reel_id"] for m in matches]
        reels = get_reels_by_ids(reel_ids)

        # Map reels by ID for easy lookup
        reel_map = {r["id"]: r for r in reels}

        # Build results in relevance order
        results = []
        for match in matches:
            reel_record = reel_map.get(match["reel_id"])
            if reel_record:
                results.append(
                    SearchResult(
                        reel=_db_record_to_response(reel_record),
                        relevance_score=round(match["score"], 4),
                    )
                )

        return SearchResponse(
            query=query.query,
            results=results,
            total=len(results),
        )

    except Exception as e:
        logger.error(f"Search failed: {e}")
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
