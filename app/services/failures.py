from app.models import FailureCode


class ProcessingFailure(Exception):
    def __init__(self, code: FailureCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def classify_processing_failure(
    error: Exception,
    *,
    step: str | None = None,
) -> ProcessingFailure:
    if isinstance(error, ProcessingFailure):
        return error

    message = str(error).strip() or "Processing failed."
    lowered = message.lower()
    normalized_step = (step or "").strip().lower()

    if _matches_any(
        lowered,
        [
            "rate limit",
            "rate-limit",
            "too many requests",
            "429",
            "quota exceeded",
            "resource exhausted",
        ],
    ):
        return ProcessingFailure(FailureCode.rate_limit, message)

    if _matches_any(
        lowered,
        [
            "timed out",
            "timeout",
            "deadline exceeded",
            "gateway timeout",
            "connection reset by peer",
        ],
    ):
        return ProcessingFailure(FailureCode.provider_timeout, message)

    if _matches_any(
        lowered,
        [
            "too large",
            "payload too large",
            "request entity too large",
            "413",
            "context length",
            "maximum context length",
        ],
    ):
        return ProcessingFailure(FailureCode.request_too_large, message)

    if _matches_any(
        lowered,
        [
            "login required",
            "authenticated cookies",
            "blocked anonymous download",
            "cookie",
            "cookies",
            "not logged in",
            "authorization",
            "unauthorized",
            "forbidden",
        ],
    ):
        return ProcessingFailure(FailureCode.auth_failure, message)

    if _matches_any(
        lowered,
        [
            "no audio track found",
            "no audio track",
            "audio track not found",
        ],
    ):
        return ProcessingFailure(FailureCode.no_audio, message)

    if _matches_any(
        lowered,
        [
            "no usable youtube transcript",
            "youtube transcript was empty",
            "transcript unavailable",
            "transcript was empty",
            "failed to transcribe audio",
        ],
    ):
        return ProcessingFailure(FailureCode.transcript_unavailable, message)

    if _matches_any(
        lowered,
        [
            "image post instead of a video",
            "unsupported",
            "could not determine the youtube video id",
        ],
    ):
        return ProcessingFailure(FailureCode.unsupported_post_type, message)

    if normalized_step == "ocr":
        return ProcessingFailure(FailureCode.ocr_failure, message)

    return ProcessingFailure(FailureCode.internal_error, message)


def _matches_any(message: str, patterns: list[str]) -> bool:
    return any(pattern in message for pattern in patterns)
