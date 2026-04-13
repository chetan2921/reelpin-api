import unittest

from app.models import FailureCode
from app.services.failures import ProcessingFailure, classify_processing_failure


class FailureClassificationTests(unittest.TestCase):
    def test_classifies_auth_failure(self):
        failure = classify_processing_failure(
            Exception("Instagram blocked anonymous download for this URL. Add authenticated cookies to the backend and try again.")
        )
        self.assertEqual(failure.code, FailureCode.auth_failure)

    def test_classifies_rate_limit(self):
        failure = classify_processing_failure(Exception("429 rate limit exceeded"))
        self.assertEqual(failure.code, FailureCode.rate_limit)

    def test_classifies_transcript_unavailable(self):
        failure = classify_processing_failure(
            Exception("No usable YouTube transcript was available for this video.")
        )
        self.assertEqual(failure.code, FailureCode.transcript_unavailable)

    def test_classifies_ocr_step_failures(self):
        failure = classify_processing_failure(Exception("Expecting value"), step="ocr")
        self.assertEqual(failure.code, FailureCode.ocr_failure)

    def test_keeps_typed_failure(self):
        failure = classify_processing_failure(
            ProcessingFailure(FailureCode.request_too_large, "Payload too large")
        )
        self.assertEqual(failure.code, FailureCode.request_too_large)


if __name__ == "__main__":
    unittest.main()
