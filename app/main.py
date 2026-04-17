import os
import shutil
import logging
import tempfile
import re
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Request, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from app.models import (
    ApiErrorResponse,
    DashboardOverviewResponse,
    DevicePushTokenInput,
    EnqueueReelJobInput,
    FailureCode,
    GenericSuccessResponse,
    ObservabilityMetricsResponse,
    ProcessingJobResponse,
    ProcessingJobStatus,
    ProactiveRecallPushRequest,
    ReclassifyCategoriesInput,
    ReclassifyCategoriesResponse,
    ReelInput,
    ReelCategoryFiltersResponse,
    ReelCategoryGroup,
    ReelResponse,
    SearchQuery,
    SearchResponse,
    SearchResult,
    HealthResponse,
)
from app.pipeline import process_reel_pipeline, process_video_pipeline
from app.services.embedder import init_pinecone, search_similar
from app.services.database import (
    create_completed_processing_job,
    create_processing_job,
    count_processing_jobs_by_status_for_user,
    count_processing_jobs_since,
    delete_reel,
    find_processing_job_by_user_and_url,
    find_processing_job_by_user_and_source_identity,
    find_reel_by_user_and_url,
    find_reel_by_user_and_source_identity,
    get_device_push_tokens,
    get_processing_job_counts_by_status,
    get_processing_job,
    get_reel,
    get_reels,
    get_reels_by_ids,
    list_user_category_pairs,
    list_processing_jobs_for_metrics,
    list_processing_jobs,
    upsert_device_push_token,
)
from app.services.notifications import send_push_notification
from app.services.completion_notifications import send_reel_ready_notification
from app.services.observability import build_processing_metrics, log_processing_event
from app.services.health_checks import (
    build_live_health_response,
    build_readiness_health_response,
)
from app.services.dashboard import build_dashboard_overview
from app.services.failures import classify_processing_failure
from app.services.cost_controls import evaluate_submission_limits
from app.services.api_responses import (
    ApiResponseError,
    failure_http_status,
    failure_user_message,
    is_retryable_failure_code,
    processing_job_progress_percent,
    processing_job_recommended_poll_after_seconds,
    processing_job_retry_scheduled,
    processing_job_retryable,
    processing_job_status_message,
    processing_job_terminal,
)
from app.services.security import (
    build_secret_configuration_summary,
    configure_secure_logging,
    secret_configuration_warnings,
)
from app.services.processing_metadata import PROCESSING_VERSION
from app.services.user_categories import (
    build_user_category_filters,
    recategorize_user_reels,
)
from app.config import get_settings
from app.services.source_identity import resolve_source_identity

configure_secure_logging()
logger = logging.getLogger(__name__)
settings = get_settings()

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
        logger.info(
            "Runtime secret configuration: %s",
            build_secret_configuration_summary(settings),
        )
        for warning in secret_configuration_warnings(settings):
            logger.warning(warning)
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


@app.exception_handler(ApiResponseError)
async def api_response_error_handler(_: Request, exc: ApiResponseError):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.to_response_body().model_dump(),
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError):
    detail = exc.errors()[0].get("msg", "Request validation failed.") if exc.errors() else "Request validation failed."
    response = ApiResponseError(
        status_code=422,
        error_code="validation_error",
        message="The request is missing required fields or contains invalid values.",
        detail=detail,
        retryable=False,
    )
    return JSONResponse(
        status_code=response.status_code,
        content=response.to_response_body().model_dump(),
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, str) else "Request failed."
    response = ApiResponseError(
        status_code=exc.status_code,
        error_code="http_error",
        message=detail,
        detail=detail,
        retryable=False,
    )
    return JSONResponse(
        status_code=response.status_code,
        content=response.to_response_body().model_dump(),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception):
    logger.exception("Unhandled API exception: %s", exc)
    response = ApiResponseError(
        status_code=500,
        error_code="internal_error",
        message="The server could not finish this request.",
        detail=str(exc) or "Unhandled server error",
        retryable=True,
    )
    return JSONResponse(
        status_code=response.status_code,
        content=response.to_response_body().model_dump(),
    )


# ============================================================
#                          ENDPOINTS
# ============================================================


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Detailed health endpoint that keeps backward-compatible 200 responses."""
    return build_readiness_health_response()


@app.get("/api/v1/health/live", response_model=HealthResponse)
async def health_live():
    """Liveness probe for the API process itself."""
    return build_live_health_response()


@app.get("/api/v1/health/ready", response_model=HealthResponse)
async def health_ready():
    """Readiness probe that returns 503 when dependencies are degraded."""
    payload = build_readiness_health_response()
    status_code = 200 if payload.ready else 503
    return JSONResponse(
        status_code=status_code,
        content=payload.model_dump(),
    )


@app.post("/api/v1/process-reel", response_model=ReelResponse)
async def process_reel(input_data: ReelInput):
    """
    Process a supported short-form URL or Instagram post from URL.

    Full pipeline: download → read/transcribe → extract → embed → store.
    """
    try:
        source = resolve_source_identity(input_data.url)
        log_processing_event(
            logger,
            "api.process_reel.requested",
            user_id=input_data.user_id,
            url=source.normalized_url,
            source=source,
            processing_step="requested",
        )
        existing_reel = find_reel_by_user_and_url(
            user_id=input_data.user_id,
            url=source.normalized_url,
        )
        if not existing_reel and source.source_content_id:
            existing_reel = find_reel_by_user_and_source_identity(
                user_id=input_data.user_id,
                source_platform=source.source_platform,
                source_content_id=source.source_content_id,
            )
        if existing_reel:
            log_processing_event(
                logger,
                "api.process_reel.reused_existing_reel",
                user_id=input_data.user_id,
                url=source.normalized_url,
                source=source,
                processing_step="reused_existing_reel",
                status="completed",
                extra={"result_reel_id": existing_reel["id"]},
            )
            return _db_record_to_response(existing_reel)

        _enforce_submission_limits(input_data.user_id)

        result = await process_reel_pipeline(
            url=source.normalized_url,
            user_id=input_data.user_id,
        )
        return result
    except ValueError as e:
        raise ApiResponseError(
            status_code=400,
            error_code="invalid_request",
            message="The shared URL is invalid.",
            detail=str(e),
            retryable=False,
        )
    except Exception as e:
        logger.error(f"Process reel failed: {e}")
        failure = classify_processing_failure(e)
        raise ApiResponseError(
            status_code=failure_http_status(failure.code),
            error_code=failure.code.value,
            message=failure_user_message(failure.code, fallback="The reel could not be processed."),
            detail=failure.message,
            retryable=is_retryable_failure_code(failure.code),
        )


@app.post("/api/v1/processing-jobs/reels", response_model=ProcessingJobResponse)
async def enqueue_reel_processing(payload: EnqueueReelJobInput):
    try:
        source = resolve_source_identity(payload.url)
        log_processing_event(
            logger,
            "api.processing_job.enqueue_requested",
            user_id=payload.user_id,
            url=source.normalized_url,
            source=source,
            processing_step="queued",
        )
        existing_job = find_processing_job_by_user_and_url(
            user_id=payload.user_id,
            url=source.normalized_url,
            statuses=[
                ProcessingJobStatus.queued.value,
                ProcessingJobStatus.processing.value,
                ProcessingJobStatus.completed.value,
            ],
        )
        if not existing_job and source.source_content_id:
            existing_job = find_processing_job_by_user_and_source_identity(
                user_id=payload.user_id,
                source_platform=source.source_platform,
                source_content_id=source.source_content_id,
                statuses=[
                    ProcessingJobStatus.queued.value,
                    ProcessingJobStatus.processing.value,
                    ProcessingJobStatus.completed.value,
                ],
            )
        if existing_job and (
            existing_job.get("status") != ProcessingJobStatus.completed.value
            or existing_job.get("result_reel_id")
        ):
            log_processing_event(
                logger,
                "api.processing_job.reused_existing_job",
                job_id=existing_job.get("id"),
                user_id=payload.user_id,
                url=source.normalized_url,
                source=source,
                processing_step=str(existing_job.get("current_step") or "queued"),
                status=str(existing_job.get("status") or "queued"),
                attempt_count=int(existing_job.get("attempt_count", 0) or 0),
                max_attempts=int(existing_job.get("max_attempts", 0) or 0),
            )
            response = _db_job_to_response(existing_job)
            if (
                response.status == ProcessingJobStatus.completed
                and response.result_reel_id
            ):
                _notify_reel_ready(
                    user_id=payload.user_id,
                    reel_id=response.result_reel_id,
                    job_id=response.id,
                    reel_title=response.reel.title if response.reel else None,
                )
            return response

        existing_reel = find_reel_by_user_and_url(
            user_id=payload.user_id,
            url=source.normalized_url,
        )
        if not existing_reel and source.source_content_id:
            existing_reel = find_reel_by_user_and_source_identity(
                user_id=payload.user_id,
                source_platform=source.source_platform,
                source_content_id=source.source_content_id,
            )
        if existing_reel:
            job = create_completed_processing_job(
                user_id=payload.user_id,
                url=source.normalized_url,
                normalized_url=source.normalized_url,
                source_platform=source.source_platform,
                source_content_type=source.source_content_type,
                source_content_id=source.source_content_id,
                processing_version=PROCESSING_VERSION,
                ingestion_method="url_share",
                transcript_source=existing_reel.get("transcript_source"),
                result_reel_id=existing_reel["id"],
                max_attempts=settings.PROCESSING_JOB_DEFAULT_MAX_ATTEMPTS,
            )
            log_processing_event(
                logger,
                "api.processing_job.completed_from_cache",
                job_id=job.get("id"),
                user_id=payload.user_id,
                url=source.normalized_url,
                source=source,
                processing_step="completed",
                status="completed",
                extra={"result_reel_id": existing_reel["id"]},
            )
            _notify_reel_ready(
                user_id=payload.user_id,
                reel_id=existing_reel["id"],
                job_id=job["id"],
                reel_title=existing_reel.get("title"),
            )
            return _db_job_to_response(job)

        _enforce_submission_limits(payload.user_id)

        job = create_processing_job(
            user_id=payload.user_id,
            url=source.normalized_url,
            normalized_url=source.normalized_url,
            source_platform=source.source_platform,
            source_content_type=source.source_content_type,
            source_content_id=source.source_content_id,
            processing_version=PROCESSING_VERSION,
            ingestion_method="url_share",
            max_attempts=settings.PROCESSING_JOB_DEFAULT_MAX_ATTEMPTS,
        )
        log_processing_event(
            logger,
            "api.processing_job.created",
            job_id=job.get("id"),
            user_id=payload.user_id,
            url=source.normalized_url,
            source=source,
            processing_step="queued",
            status="queued",
            attempt_count=int(job.get("attempt_count", 0) or 0),
            max_attempts=int(job.get("max_attempts", 0) or 0),
        )
        return _db_job_to_response(job)
    except ValueError as e:
        raise ApiResponseError(
            status_code=400,
            error_code="invalid_request",
            message="The shared URL is invalid.",
            detail=str(e),
            retryable=False,
        )
    except Exception as e:
        logger.error(f"Enqueue reel processing failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="processing_job_enqueue_failed",
            message="Could not create a processing job right now.",
            detail=str(e),
            retryable=True,
        )


@app.get("/api/v1/processing-jobs/{job_id}", response_model=ProcessingJobResponse)
async def get_processing_job_detail(job_id: str):
    try:
        job = get_processing_job(job_id)
        if not job:
            raise ApiResponseError(
                status_code=404,
                error_code="processing_job_not_found",
                message="The processing job was not found.",
                detail="Processing job not found",
                retryable=False,
            )
        return _db_job_to_response(job)
    except ApiResponseError:
        raise
    except Exception as e:
        logger.error(f"Get processing job failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="processing_job_lookup_failed",
            message="Could not load the processing job right now.",
            detail=str(e),
            retryable=True,
        )


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
        raise ApiResponseError(
            status_code=500,
            error_code="processing_job_list_failed",
            message="Could not load processing jobs right now.",
            detail=str(e),
            retryable=True,
        )


@app.get("/api/v1/metrics", response_model=ObservabilityMetricsResponse)
async def get_metrics():
    try:
        metrics = build_processing_metrics(
            jobs=list_processing_jobs_for_metrics(limit=500),
            queue_depth=get_processing_job_counts_by_status(
                [
                    ProcessingJobStatus.queued.value,
                    ProcessingJobStatus.processing.value,
                    ProcessingJobStatus.dead_lettered.value,
                ]
            ),
        )
        return ObservabilityMetricsResponse(**metrics)
    except Exception as e:
        logger.error(f"Get metrics failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="metrics_unavailable",
            message="Metrics are not available right now.",
            detail=str(e),
            retryable=True,
        )


@app.get("/api/v1/dashboard/overview", response_model=DashboardOverviewResponse)
async def get_dashboard_overview(
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
):
    _require_admin_key(x_admin_key)
    try:
        return build_dashboard_overview()
    except Exception as e:
        logger.error(f"Get dashboard overview failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="dashboard_unavailable",
            message="The dashboard is not available right now.",
            detail=str(e),
            retryable=True,
        )


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
        _enforce_submission_limits(user_id)
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
        failure = classify_processing_failure(e)
        raise ApiResponseError(
            status_code=failure_http_status(failure.code),
            error_code=failure.code.value,
            message=failure_user_message(failure.code, fallback="The uploaded video could not be processed."),
            detail=failure.message,
            retryable=is_retryable_failure_code(failure.code),
        )


@app.get("/api/v1/reels/category-filters", response_model=ReelCategoryFiltersResponse)
async def get_reel_category_filters(
    user_id: str = Query(..., description="Filter by user ID"),
):
    """List the user's dynamic category tree for reel filters."""
    try:
        categories = [
            ReelCategoryGroup(**group)
            for group in build_user_category_filters(
                list_user_category_pairs(user_id=user_id)
            )
        ]
        return ReelCategoryFiltersResponse(
            user_id=user_id,
            categories=categories,
            total_categories=len(categories),
        )
    except Exception as e:
        logger.error(f"List reel category filters failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="reel_category_filters_failed",
            message="Could not load category filters right now.",
            detail=str(e),
            retryable=True,
        )


@app.post("/api/v1/reels/reclassify-categories", response_model=ReclassifyCategoriesResponse)
async def reclassify_saved_reel_categories(payload: ReclassifyCategoriesInput):
    """Rebuild dynamic categories for a user's existing saved reels."""
    try:
        result = recategorize_user_reels(
            user_id=payload.user_id,
            limit=payload.limit,
        )
        categories = [ReelCategoryGroup(**group) for group in result["categories"]]
        return ReclassifyCategoriesResponse(
            user_id=payload.user_id,
            reviewed=result["reviewed"],
            updated=result["updated"],
            categories=categories,
        )
    except Exception as e:
        logger.error(f"Reclassify saved reel categories failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="reel_recategorization_failed",
            message="Could not rebuild reel categories right now.",
            detail=str(e),
            retryable=True,
        )


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
        raise ApiResponseError(
            status_code=500,
            error_code="reel_list_failed",
            message="Could not load reels right now.",
            detail=str(e),
            retryable=True,
        )


@app.get("/api/v1/reels/{reel_id}", response_model=ReelResponse)
async def get_reel_detail(reel_id: str):
    """Get a single reel's full details."""
    try:
        record = get_reel(reel_id)
        if not record:
            raise ApiResponseError(
                status_code=404,
                error_code="reel_not_found",
                message="The reel was not found.",
                detail="Reel not found",
                retryable=False,
            )
        return _db_record_to_response(record)
    except ApiResponseError:
        raise
    except Exception as e:
        logger.error(f"Get reel failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="reel_lookup_failed",
            message="Could not load the reel right now.",
            detail=str(e),
            retryable=True,
        )


@app.delete("/api/v1/reels/{reel_id}")
async def remove_reel(reel_id: str):
    """Delete a saved reel."""
    try:
        deleted = delete_reel(reel_id)
        if not deleted:
            raise ApiResponseError(
                status_code=404,
                error_code="reel_not_found",
                message="The reel was not found.",
                detail="Reel not found",
                retryable=False,
            )
        return {"message": "Reel deleted", "id": reel_id}
    except ApiResponseError:
        raise
    except Exception as e:
        logger.error(f"Delete reel failed: {e}")
        raise ApiResponseError(
            status_code=500,
            error_code="reel_delete_failed",
            message="Could not delete the reel right now.",
            detail=str(e),
            retryable=True,
        )


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
        raise ApiResponseError(
            status_code=500,
            error_code="search_failed",
            message="Search is not available right now.",
            detail=str(e),
            retryable=True,
        )


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
        raise ApiResponseError(
            status_code=500,
            error_code="device_token_registration_failed",
            message="Could not register the device token right now.",
            detail=str(e),
            retryable=True,
        )


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
        raise ApiResponseError(
            status_code=500,
            error_code="push_send_failed",
            message="Could not send the push notification right now.",
            detail=str(e),
            retryable=True,
        )


# ============================================================
#                         HELPERS
# ============================================================


def _db_record_to_response(record: dict) -> ReelResponse:
    """Convert a Supabase DB record dict to a ReelResponse model."""
    return ReelResponse(
        id=record["id"],
        user_id=record.get("user_id", ""),
        url=record.get("url", ""),
        normalized_url=record.get("normalized_url"),
        source_platform=record.get("source_platform"),
        source_content_type=record.get("source_content_type"),
        source_content_id=record.get("source_content_id"),
        processing_version=record.get("processing_version"),
        ingestion_method=record.get("ingestion_method"),
        transcript_source=record.get("transcript_source"),
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


def _notify_reel_ready(
    *,
    user_id: str,
    reel_id: str,
    job_id: str,
    reel_title: str | None,
) -> None:
    try:
        delivered = send_reel_ready_notification(
            user_id=user_id,
            reel_id=reel_id,
            job_id=job_id,
            reel_title=reel_title,
        )
        log_processing_event(
            logger,
            "api.processing_job.notification_sent",
            job_id=job_id,
            user_id=user_id,
            processing_step="completed",
            status="notification_sent",
            extra={"delivered_device_count": delivered, "result_reel_id": reel_id},
        )
    except Exception as e:
        logger.warning("Completion push skipped for job %s: %s", job_id, e)


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
        normalized_url=record.get("normalized_url"),
        source_platform=record.get("source_platform"),
        source_content_type=record.get("source_content_type"),
        source_content_id=record.get("source_content_id"),
        processing_version=record.get("processing_version"),
        ingestion_method=record.get("ingestion_method"),
        transcript_source=record.get("transcript_source"),
        status=ProcessingJobStatus(record.get("status", "queued")),
        current_step=record.get("current_step"),
        progress_percent=processing_job_progress_percent(record),
        failure_code=_parse_failure_code(record.get("failure_code")),
        error_message=record.get("error_message"),
        attempt_count=int(record.get("attempt_count", 0) or 0),
        max_attempts=int(record.get("max_attempts", 0) or 0),
        next_retry_at=record.get("next_retry_at"),
        terminal=processing_job_terminal(record),
        retry_scheduled=processing_job_retry_scheduled(record),
        retryable=processing_job_retryable(record),
        status_message=processing_job_status_message(record),
        recommended_poll_after_seconds=processing_job_recommended_poll_after_seconds(record),
        result_reel_id=result_reel_id,
        step_durations=record.get("step_durations", {}) or {},
        created_at=record.get("created_at"),
        updated_at=record.get("updated_at"),
        started_at=record.get("started_at"),
        completed_at=record.get("completed_at"),
        reel=_db_record_to_response(reel_record) if reel_record else None,
    )


def _derive_source_platform(url: str) -> str:
    return resolve_source_identity(url).source_platform


def _parse_failure_code(value: str | None) -> FailureCode | None:
    if not value:
        return None

    try:
        return FailureCode(value)
    except ValueError:
        return None


def _enforce_submission_limits(user_id: str) -> None:
    now = datetime.now(timezone.utc)
    decision = evaluate_submission_limits(
        recent_submission_count=count_processing_jobs_since(
            user_id=user_id,
            since_iso=(now - timedelta(hours=1)).isoformat(),
        ),
        active_job_count=count_processing_jobs_by_status_for_user(
            user_id=user_id,
            statuses=[
                ProcessingJobStatus.queued.value,
                ProcessingJobStatus.processing.value,
            ],
        ),
        max_submissions_per_hour=settings.USER_SUBMISSION_LIMIT_PER_HOUR,
        max_active_jobs=settings.USER_ACTIVE_JOB_LIMIT,
    )
    if decision.allowed:
        return

    raise ApiResponseError(
        status_code=429,
        error_code=decision.error_code or "submission_rate_limited",
        message=decision.message or "Submission limit reached.",
        detail=decision.detail or "Submission limit reached.",
        retryable=True,
    )


def _require_admin_key(candidate: str | None) -> None:
    configured = (settings.ADMIN_DASHBOARD_KEY or "").strip()
    if not configured:
        raise ApiResponseError(
            status_code=503,
            error_code="dashboard_not_configured",
            message="The admin dashboard is not configured yet.",
            detail="ADMIN_DASHBOARD_KEY is not set on the backend.",
            retryable=False,
        )

    if candidate != configured:
        raise ApiResponseError(
            status_code=401,
            error_code="unauthorized_dashboard_access",
            message="Admin dashboard authentication failed.",
            detail="The X-Admin-Key header is missing or invalid.",
            retryable=False,
        )


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
