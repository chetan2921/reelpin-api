from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.models import FailureCode, ProcessingJobStatus
from app.services.failures import ProcessingFailure


@dataclass(frozen=True)
class RetryDecision:
    status: ProcessingJobStatus
    current_step: str
    should_retry: bool
    next_retry_at: str
    completed_at: str | None


def build_retry_decision(
    *,
    failure: ProcessingFailure,
    attempt_count: int,
    max_attempts: int,
    transient_retry_delay_seconds: int,
    rate_limit_retry_delay_seconds: int,
    now: datetime | None = None,
) -> RetryDecision:
    current_time = now or datetime.now(timezone.utc)
    now_iso = current_time.isoformat()

    if (
        _is_retryable_failure(failure.code)
        and attempt_count < max_attempts
    ):
        retry_delay_seconds = (
            rate_limit_retry_delay_seconds
            if failure.code == FailureCode.rate_limit
            else transient_retry_delay_seconds
        )
        retry_at = current_time + timedelta(seconds=max(retry_delay_seconds, 0))
        return RetryDecision(
            status=ProcessingJobStatus.queued,
            current_step="retry_scheduled",
            should_retry=True,
            next_retry_at=retry_at.isoformat(),
            completed_at=None,
        )

    return RetryDecision(
        status=ProcessingJobStatus.dead_lettered,
        current_step="dead_lettered",
        should_retry=False,
        next_retry_at=now_iso,
        completed_at=now_iso,
    )


def _is_retryable_failure(code: FailureCode) -> bool:
    return code in {
        FailureCode.rate_limit,
        FailureCode.provider_timeout,
        FailureCode.internal_error,
    }
