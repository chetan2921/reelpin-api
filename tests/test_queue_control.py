import unittest

from app.services.queue_control import (
    active_platform_counts,
    active_source_keys,
    can_claim_job,
    job_source_key,
)


class QueueControlTests(unittest.TestCase):
    def test_job_source_key_uses_platform_content_identity(self):
        key = job_source_key(
            {"url": "https://www.instagram.com/reel/ABC123/?utm_source=share"}
        )

        self.assertEqual(key, "instagram:ABC123")

    def test_can_claim_job_blocks_duplicate_source(self):
        processing_jobs = [
            {"url": "https://www.instagram.com/reel/ABC123/"}
        ]
        queued_job = {"url": "https://instagram.com/reel/ABC123/?igsh=xyz"}

        self.assertFalse(
            can_claim_job(
                queued_job,
                current_platform_counts=active_platform_counts(processing_jobs),
                current_source_keys=active_source_keys(processing_jobs),
                platform_limits={"instagram": 2, "youtube": 2, "tiktok": 1, "web": 1},
            )
        )

    def test_can_claim_job_blocks_platform_when_at_capacity(self):
        processing_jobs = [
            {"url": "https://www.youtube.com/watch?v=abc123"},
            {"url": "https://www.youtube.com/watch?v=def456"},
        ]
        queued_job = {"url": "https://youtu.be/ghi789"}

        self.assertFalse(
            can_claim_job(
                queued_job,
                current_platform_counts=active_platform_counts(processing_jobs),
                current_source_keys=active_source_keys(processing_jobs),
                platform_limits={"instagram": 1, "youtube": 2, "tiktok": 1, "web": 1},
            )
        )

    def test_can_claim_job_allows_distinct_source_under_platform_limit(self):
        processing_jobs = [
            {"url": "https://www.tiktok.com/@creator/video/123"}
        ]
        queued_job = {"url": "https://www.youtube.com/watch?v=abc123"}

        self.assertTrue(
            can_claim_job(
                queued_job,
                current_platform_counts=active_platform_counts(processing_jobs),
                current_source_keys=active_source_keys(processing_jobs),
                platform_limits={"instagram": 1, "youtube": 2, "tiktok": 1, "web": 1},
            )
        )


if __name__ == "__main__":
    unittest.main()
