from dataclasses import dataclass


@dataclass(frozen=True)
class SubmissionLimitDecision:
    allowed: bool
    error_code: str | None = None
    message: str | None = None
    detail: str | None = None
    retry_after_seconds: int | None = None


def evaluate_submission_limits(
    *,
    recent_submission_count: int,
    active_job_count: int,
    max_submissions_per_hour: int,
    max_active_jobs: int,
) -> SubmissionLimitDecision:
    if active_job_count >= max_active_jobs:
        return SubmissionLimitDecision(
            allowed=False,
            error_code="too_many_active_jobs",
            message="You already have too many reels processing.",
            detail=(
                f"User already has {active_job_count} active jobs, "
                f"which meets the active job limit of {max_active_jobs}."
            ),
            retry_after_seconds=60,
        )

    if recent_submission_count >= max_submissions_per_hour:
        return SubmissionLimitDecision(
            allowed=False,
            error_code="submission_rate_limited",
            message="You have reached the current submission limit.",
            detail=(
                f"User already submitted {recent_submission_count} jobs in the last hour, "
                f"which meets the hourly submission limit of {max_submissions_per_hour}."
            ),
            retry_after_seconds=300,
        )

    return SubmissionLimitDecision(allowed=True)
