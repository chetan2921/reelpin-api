import unittest

from app.models import FailureCode
from app.services.failures import ProcessingFailure, classify_processing_failure


class FailureClassificationTests(unittest.TestCase):
    def test_classifies_auth_failure(self):
        failure = classify_processing_failure(
            Exception("Instagram blocked anonymous download for this URL. Add authenticated cookies to the backend and try again.")
        )
        self.assertEqual(failure.code, FailureCode.auth_failure)

    def test_classifies_instagram_public_media_failure_as_auth_failure(self):
        failure = classify_processing_failure(
            Exception("Instagram did not expose a public media URL for this page.")
        )
        self.assertEqual(failure.code, FailureCode.auth_failure)

    def test_classifies_ip_block_as_auth_failure(self):
        failure = classify_processing_failure(
            Exception("Instagram is blocking requests from your IP.")
        )
        self.assertEqual(failure.code, FailureCode.auth_failure)

    def test_classifies_rate_limit(self):
        failure = classify_processing_failure(Exception("429 rate limit exceeded"))
        self.assertEqual(failure.code, FailureCode.rate_limit)

    def test_classifies_instagram_redirect_as_provider_timeout(self):
        failure = classify_processing_failure(
            Exception("Instagram page fetch returned HTTP 302")
        )
        self.assertEqual(failure.code, FailureCode.provider_timeout)

    def test_classifies_transcript_unavailable(self):
        failure = classify_processing_failure(
            Exception("Failed to transcribe audio: provider error")
        )
        self.assertEqual(failure.code, FailureCode.transcript_unavailable)

    def test_classifies_transcript_retrieval_failure(self):
        failure = classify_processing_failure(
            Exception("Could not retrieve a transcript for the video")
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
