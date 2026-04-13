import unittest

from app.models import FailureCode
from app.services.api_responses import (
    failure_http_status,
    failure_user_message,
    processing_job_progress_percent,
    processing_job_recommended_poll_after_seconds,
    processing_job_retry_scheduled,
    processing_job_status_message,
    processing_job_terminal,
)


class ApiResponseTests(unittest.TestCase):
    def test_failure_helpers_return_expected_status_and_message(self):
        self.assertEqual(failure_http_status(FailureCode.rate_limit), 429)
        self.assertEqual(
            failure_user_message(FailureCode.auth_failure),
            "The source platform requires a fresh authenticated session.",
        )

    def test_processing_job_status_metadata_for_retry(self):
        record = {
            "status": "queued",
            "current_step": "retry_scheduled",
            "next_retry_at": "2099-01-01T12:00:00+00:00",
            "progress_percent": 0,
        }

        self.assertTrue(processing_job_retry_scheduled(record))
        self.assertFalse(processing_job_terminal(record))
        self.assertIn("Retry scheduled", processing_job_status_message(record))
        self.assertEqual(processing_job_progress_percent(record), 0)
        self.assertIsNotNone(processing_job_recommended_poll_after_seconds(record))

    def test_processing_job_status_metadata_for_terminal_failure(self):
        record = {
            "status": "dead_lettered",
            "current_step": "dead_lettered",
            "failure_code": "ocr_failure",
            "progress_percent": 17,
        }

        self.assertTrue(processing_job_terminal(record))
        self.assertEqual(
            processing_job_status_message(record),
            "Image text extraction failed for this post.",
        )
        self.assertEqual(processing_job_progress_percent(record), 100)
        self.assertIsNone(processing_job_recommended_poll_after_seconds(record))


if __name__ == "__main__":
    unittest.main()
