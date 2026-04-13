from datetime import datetime, timezone
import unittest

from app.models import FailureCode, ProcessingJobStatus
from app.services.failures import ProcessingFailure
from app.services.retry_policy import build_retry_decision


class RetryPolicyTests(unittest.TestCase):
    def test_rate_limit_is_retried_with_longer_cooldown(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        decision = build_retry_decision(
            failure=ProcessingFailure(FailureCode.rate_limit, "429 rate limit exceeded"),
            attempt_count=1,
            max_attempts=3,
            transient_retry_delay_seconds=60,
            rate_limit_retry_delay_seconds=300,
            now=now,
        )

        self.assertTrue(decision.should_retry)
        self.assertEqual(decision.status, ProcessingJobStatus.queued)
        self.assertEqual(decision.current_step, "retry_scheduled")
        self.assertEqual(decision.completed_at, None)
        self.assertEqual(decision.next_retry_at, "2026-04-13T12:05:00+00:00")

    def test_permanent_failures_go_to_dead_lettered(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        decision = build_retry_decision(
            failure=ProcessingFailure(FailureCode.auth_failure, "cookies missing"),
            attempt_count=1,
            max_attempts=3,
            transient_retry_delay_seconds=60,
            rate_limit_retry_delay_seconds=300,
            now=now,
        )

        self.assertFalse(decision.should_retry)
        self.assertEqual(decision.status, ProcessingJobStatus.dead_lettered)
        self.assertEqual(decision.current_step, "dead_lettered")
        self.assertEqual(decision.completed_at, "2026-04-13T12:00:00+00:00")

    def test_retryable_failures_stop_after_max_attempts(self):
        now = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
        decision = build_retry_decision(
            failure=ProcessingFailure(FailureCode.provider_timeout, "timeout"),
            attempt_count=3,
            max_attempts=3,
            transient_retry_delay_seconds=60,
            rate_limit_retry_delay_seconds=300,
            now=now,
        )

        self.assertFalse(decision.should_retry)
        self.assertEqual(decision.status, ProcessingJobStatus.dead_lettered)


if __name__ == "__main__":
    unittest.main()
