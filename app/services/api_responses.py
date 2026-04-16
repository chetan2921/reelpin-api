from datetime import datetime, timezone

from app.models import ApiErrorResponse, FailureCode, ProcessingJobStatus


_RETRYABLE_FAILURE_CODES = {
    FailureCode.rate_limit,
    FailureCode.provider_timeout,
    FailureCode.internal_error,
}

_STEP_MESSAGES = {
    "queued": "Queued for processing.",
    "starting": "Starting processing.",
    "checking_cache": "Checking for cached results.",
    "downloading": "Downloading media.",
    "transcribing": "Transcribing audio.",
    "ocr": "Reading text from images.",
    "extracting": "Extracting structured details.",
    "categorizing": "Organizing the reel into your categories.",
    "saving": "Saving the reel.",
    "embedding": "Indexing the reel for search.",
    "completed": "Processing completed.",
    "retry_scheduled": "Waiting before the next retry.",
    "dead_lettered": "Processing stopped after a final failure.",
}


class ApiResponseError(Exception):
    def __init__(
        self,
        *,
        status_code: int,
        error_code: str,
        message: str,
        detail: str | None = None,
        retryable: bool = False,
    ):
        super().__init__(detail or message)
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.detail = detail or message
        self.retryable = retryable

    def to_response_body(self) -> ApiErrorResponse:
        return ApiErrorResponse(
            error_code=self.error_code,
            message=self.message,
            detail=self.detail,
            retryable=self.retryable,
        )


def is_retryable_failure_code(code: FailureCode | None) -> bool:
    return code in _RETRYABLE_FAILURE_CODES


def failure_http_status(code: FailureCode) -> int:
    if code == FailureCode.auth_failure:
        return 401
    if code == FailureCode.rate_limit:
        return 429
    if code == FailureCode.request_too_large:
        return 413
    if code in {
        FailureCode.no_audio,
        FailureCode.transcript_unavailable,
        FailureCode.unsupported_post_type,
        FailureCode.ocr_failure,
    }:
        return 422
    if code == FailureCode.provider_timeout:
        return 504
    return 500


def failure_user_message(
    code: FailureCode | None,
    *,
    fallback: str = "Request failed.",
) -> str:
    if code == FailureCode.auth_failure:
        return "The source platform requires a fresh authenticated session."
    if code == FailureCode.rate_limit:
        return "The source platform is rate limiting requests right now."
    if code == FailureCode.no_audio:
        return "This video does not include an audio track."
    if code == FailureCode.transcript_unavailable:
        return "A transcript was not available for this media."
    if code == FailureCode.unsupported_post_type:
        return "This shared post type is not supported yet."
    if code == FailureCode.ocr_failure:
        return "Image text extraction failed for this post."
    if code == FailureCode.provider_timeout:
        return "An upstream provider timed out while processing this request."
    if code == FailureCode.request_too_large:
        return "The media payload was too large to process."
    if code == FailureCode.internal_error:
        return "The server could not finish this request."
    return fallback


def processing_job_status_message(record: dict) -> str:
    status = str(record.get("status") or "").strip().lower()
    step = str(record.get("current_step") or "").strip().lower()
    failure_code = _parse_failure_code(record.get("failure_code"))

    if status == ProcessingJobStatus.completed.value:
        return "Processing completed."
    if status == ProcessingJobStatus.dead_lettered.value:
        return failure_user_message(
            failure_code,
            fallback="Processing stopped after a final failure.",
        )
    if status == ProcessingJobStatus.failed.value:
        return failure_user_message(
            failure_code,
            fallback="Processing failed.",
        )
    if status == ProcessingJobStatus.queued.value and step == "retry_scheduled":
        retry_at = _parse_datetime(record.get("next_retry_at"))
        if retry_at:
            return f"Retry scheduled for {retry_at.astimezone().isoformat(timespec='minutes')}."
        return _STEP_MESSAGES["retry_scheduled"]
    if step in _STEP_MESSAGES:
        return _STEP_MESSAGES[step]
    if status == ProcessingJobStatus.processing.value:
        return "Processing is in progress."
    if status == ProcessingJobStatus.queued.value:
        return "Queued for processing."
    return "Processing state updated."


def processing_job_terminal(record: dict) -> bool:
    return str(record.get("status") or "").strip().lower() in {
        ProcessingJobStatus.completed.value,
        ProcessingJobStatus.failed.value,
        ProcessingJobStatus.dead_lettered.value,
    }


def processing_job_retry_scheduled(record: dict) -> bool:
    return (
        str(record.get("status") or "").strip().lower() == ProcessingJobStatus.queued.value
        and str(record.get("current_step") or "").strip().lower() == "retry_scheduled"
    )


def processing_job_retryable(record: dict) -> bool:
    if processing_job_retry_scheduled(record):
        return True

    status = str(record.get("status") or "").strip().lower()
    if status in {ProcessingJobStatus.processing.value, ProcessingJobStatus.queued.value}:
        return True

    failure_code = _parse_failure_code(record.get("failure_code"))
    return is_retryable_failure_code(failure_code)


def processing_job_recommended_poll_after_seconds(record: dict) -> int | None:
    if processing_job_terminal(record):
        return None

    if processing_job_retry_scheduled(record):
        retry_at = _parse_datetime(record.get("next_retry_at"))
        if retry_at is None:
            return 10
        delta_seconds = int((retry_at - datetime.now(timezone.utc)).total_seconds())
        if delta_seconds <= 0:
            return 2
        return min(max(delta_seconds, 2), 30)

    status = str(record.get("status") or "").strip().lower()
    if status == ProcessingJobStatus.queued.value:
        return 3
    if status == ProcessingJobStatus.processing.value:
        return 2
    return None


def processing_job_progress_percent(record: dict) -> int:
    status = str(record.get("status") or "").strip().lower()
    current = int(record.get("progress_percent", 0) or 0)
    if status in {
        ProcessingJobStatus.completed.value,
        ProcessingJobStatus.failed.value,
        ProcessingJobStatus.dead_lettered.value,
    }:
        return 100
    if status == ProcessingJobStatus.processing.value:
        return max(current, 5)
    return max(min(current, 99), 0)


def _parse_failure_code(value: str | None) -> FailureCode | None:
    if not value:
        return None

    try:
        return FailureCode(value)
    except ValueError:
        return None


def _parse_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
