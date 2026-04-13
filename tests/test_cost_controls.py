import unittest

from app.services.cost_controls import evaluate_submission_limits


class CostControlTests(unittest.TestCase):
    def test_blocks_when_active_jobs_hit_limit(self):
        decision = evaluate_submission_limits(
            recent_submission_count=2,
            active_job_count=4,
            max_submissions_per_hour=20,
            max_active_jobs=4,
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.error_code, "too_many_active_jobs")
        self.assertEqual(decision.retry_after_seconds, 60)

    def test_blocks_when_hourly_submission_limit_is_hit(self):
        decision = evaluate_submission_limits(
            recent_submission_count=20,
            active_job_count=1,
            max_submissions_per_hour=20,
            max_active_jobs=4,
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.error_code, "submission_rate_limited")
        self.assertEqual(decision.retry_after_seconds, 300)

    def test_allows_when_under_limits(self):
        decision = evaluate_submission_limits(
            recent_submission_count=3,
            active_job_count=1,
            max_submissions_per_hour=20,
            max_active_jobs=4,
        )

        self.assertTrue(decision.allowed)


if __name__ == "__main__":
    unittest.main()
