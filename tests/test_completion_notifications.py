import unittest
from unittest.mock import patch

from app.services.completion_notifications import (
    build_reel_ready_notification_payload,
    send_reel_ready_notification,
)


class CompletionNotificationTests(unittest.TestCase):
    def test_builds_reel_ready_payload(self):
        payload = build_reel_ready_notification_payload(
            reel_id="reel-123",
            job_id="job-456",
            reel_title="Best cafes in Bandra",
        )

        self.assertEqual(payload["title"], "Reel pinned in ReelPin")
        self.assertIn("Best cafes in Bandra", payload["body"])
        self.assertEqual(payload["data"]["type"], "reel_ready")
        self.assertEqual(payload["data"]["reel_id"], "reel-123")
        self.assertEqual(payload["data"]["job_id"], "job-456")

    @patch("app.services.completion_notifications._send_push")
    @patch("app.services.completion_notifications._get_device_tokens")
    def test_sends_notification_to_registered_devices(self, get_tokens, send_push):
        get_tokens.return_value = ["token-1", "token-2"]
        send_push.return_value = 2

        delivered = send_reel_ready_notification(
            user_id="user-1",
            reel_id="reel-123",
            job_id="job-456",
            reel_title="Best cafes in Bandra",
        )

        self.assertEqual(delivered, 2)
        send_push.assert_called_once()


if __name__ == "__main__":
    unittest.main()
