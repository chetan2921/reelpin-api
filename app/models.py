from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime
from enum import Enum


# --- Categories ---
# Dynamically mapped from AI across 46 taxonomy classes


# --- Location ---

class Location(BaseModel):
    name: str
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


# --- Request Models ---

class ReelInput(BaseModel):
    url: str = Field(..., description="Instagram post/reel, TikTok, or YouTube Shorts URL")
    user_id: str = Field(default="default-user", description="User identifier")


class SearchQuery(BaseModel):
    query: str = Field(..., description="Natural language search query")
    user_id: str = Field(default="default-user", description="Filter by user")
    category: Optional[str] = Field(None, description="Filter by category")
    subcategory: Optional[str] = Field(None, description="Filter by subcategory")
    limit: int = Field(default=5, ge=1, le=20, description="Max results")


class ProcessingJobStatus(str, Enum):
    queued = "queued"
    processing = "processing"
    completed = "completed"
    failed = "failed"
    dead_lettered = "dead_lettered"


class FailureCode(str, Enum):
    auth_failure = "auth_failure"
    rate_limit = "rate_limit"
    no_audio = "no_audio"
    transcript_unavailable = "transcript_unavailable"
    unsupported_post_type = "unsupported_post_type"
    ocr_failure = "ocr_failure"
    provider_timeout = "provider_timeout"
    request_too_large = "request_too_large"
    internal_error = "internal_error"


class EnqueueReelJobInput(BaseModel):
    url: str = Field(..., description="Instagram post/reel, TikTok, or YouTube Shorts URL")
    user_id: str = Field(..., description="Authenticated user identifier")


class DevicePushTokenInput(BaseModel):
    user_id: str = Field(..., description="Authenticated user identifier")
    token: str = Field(..., description="Firebase Cloud Messaging token")
    platform: str = Field(..., description="ios, android, or web")


class ProactiveRecallPushRequest(BaseModel):
    user_id: str = Field(..., description="Target user identifier")
    title: str = Field(..., description="Notification title")
    body: str = Field(..., description="Notification body")
    data: dict[str, str] = Field(default_factory=dict, description="Optional string data payload")


# --- Extracted Data ---

class ExtractedData(BaseModel):
    title: str = ""
    summary: str = ""
    category: str = "Other"
    subcategory: str = "Other"
    secondary_categories: list[str] = Field(default_factory=list)
    key_facts: list[str] = Field(default_factory=list)
    locations: list[Location] = Field(default_factory=list)
    people_mentioned: list[str] = Field(default_factory=list)
    actionable_items: list[str] = Field(default_factory=list)


# --- Response Models ---

class ReelResponse(BaseModel):
    id: str
    user_id: str
    url: str
    normalized_url: Optional[str] = None
    source_platform: Optional[str] = None
    source_content_type: Optional[str] = None
    source_content_id: Optional[str] = None
    processing_version: Optional[str] = None
    ingestion_method: Optional[str] = None
    transcript_source: Optional[str] = None
    title: str
    summary: str
    transcript: str
    category: str
    subcategory: str = "Other"
    secondary_categories: list[str] = Field(default_factory=list)
    key_facts: list[str] = Field(default_factory=list)
    locations: list[Location] = Field(default_factory=list)
    people_mentioned: list[str] = Field(default_factory=list)
    actionable_items: list[str] = Field(default_factory=list)
    created_at: Optional[str] = None


class ProcessingJobResponse(BaseModel):
    id: str
    user_id: str
    url: str
    normalized_url: Optional[str] = None
    source_platform: Optional[str] = None
    source_content_type: Optional[str] = None
    source_content_id: Optional[str] = None
    processing_version: Optional[str] = None
    ingestion_method: Optional[str] = None
    transcript_source: Optional[str] = None
    status: ProcessingJobStatus
    current_step: Optional[str] = None
    progress_percent: int = 0
    failure_code: Optional[FailureCode] = None
    error_message: Optional[str] = None
    attempt_count: int = 0
    max_attempts: int = 0
    next_retry_at: Optional[str] = None
    terminal: bool = False
    retry_scheduled: bool = False
    retryable: bool = False
    status_message: str = ""
    recommended_poll_after_seconds: Optional[int] = None
    result_reel_id: Optional[str] = None
    step_durations: dict[str, float] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    reel: Optional[ReelResponse] = None


class SearchResult(BaseModel):
    reel: ReelResponse
    relevance_score: float


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResult]
    total: int


class ServiceHealthCheck(BaseModel):
    healthy: bool
    status: str
    latency_ms: Optional[float] = None
    message: Optional[str] = None
    checked_at: Optional[str] = None
    details: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str = "ok"
    ready: bool = True
    version: str = "1.0.0"
    service: str = "ReelMind API"
    checked_at: Optional[str] = None
    checks: dict[str, ServiceHealthCheck] = Field(default_factory=dict)


class ObservabilityMetricsResponse(BaseModel):
    sample_size: int = 0
    queue_depth: dict[str, int] = Field(default_factory=dict)
    total_retries: int = 0
    success_rate_by_platform: dict[str, float] = Field(default_factory=dict)
    failure_rate_by_platform: dict[str, float] = Field(default_factory=dict)
    average_processing_seconds: float = 0.0
    average_processing_seconds_by_platform: dict[str, float] = Field(default_factory=dict)
    average_step_seconds: dict[str, float] = Field(default_factory=dict)
    retry_count_by_platform: dict[str, int] = Field(default_factory=dict)


class GenericSuccessResponse(BaseModel):
    success: bool = True
    message: str = "ok"


class ApiErrorResponse(BaseModel):
    success: bool = False
    error_code: str
    message: str
    detail: str
    retryable: bool = False


class DashboardOverviewResponse(BaseModel):
    checked_at: str
    registered_profile_count: Optional[int] = None
    registered_device_count: int = 0
    registered_device_user_count: int = 0
    saved_reel_count: int = 0
    processing_job_count: int = 0
    active_job_count: int = 0
    completed_job_count: int = 0
    failed_job_count: int = 0
    dead_lettered_job_count: int = 0
    reels_saved_last_24h: int = 0
    jobs_created_last_24h: int = 0
    exact_download_count: Optional[int] = None
    exact_download_source: str = ""
    processing_metrics: dict[str, Any] = Field(default_factory=dict)
    health: dict[str, Any] = Field(default_factory=dict)
    instagram_cookie_health: list[dict[str, Any]] = Field(default_factory=list)
