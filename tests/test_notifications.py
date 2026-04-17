import unittest
from types import SimpleNamespace
from unittest.mock import patch

from firebase_admin import messaging

from app.services.notifications import send_push_notification


class NotificationTransportTests(unittest.TestCase):
    @patch("app.services.notifications._get_firebase_app")
    @patch("app.services.notifications.messaging.send_each_for_multicast")
    def test_send_push_notification_uses_expected_mobile_delivery_settings(
        self,
        send_each_for_multicast,
        get_firebase_app,
    ):
        send_each_for_multicast.return_value = SimpleNamespace(
            success_count=1,
            failure_count=0,
            responses=[SimpleNamespace(success=True, exception=None)],
        )

        delivered = send_push_notification(
            tokens=["token-1"],
            title="Reel pinned in ReelPin",
            body="Your saved reel is ready in ReelPin.",
            data={"type": "reel_ready"},
        )

        self.assertEqual(delivered, 1)
        get_firebase_app.assert_called_once()
        message = send_each_for_multicast.call_args.args[0]
        self.assertEqual(message.android.priority, "high")
        self.assertEqual(message.android.notification.channel_id, "reelpin_updates")
        self.assertEqual(message.apns.headers["apns-priority"], "10")
        self.assertEqual(message.apns.payload.aps.sound, "default")

    @patch("app.services.notifications._delete_invalid_tokens")
    @patch("app.services.notifications._get_firebase_app")
    @patch("app.services.notifications.messaging.send_each_for_multicast")
    def test_send_push_notification_removes_invalid_tokens(
        self,
        send_each_for_multicast,
        get_firebase_app,
        delete_invalid_tokens,
    ):
        send_each_for_multicast.return_value = SimpleNamespace(
            success_count=1,
            failure_count=2,
            responses=[
                SimpleNamespace(success=True, exception=None),
                SimpleNamespace(success=False, exception=messaging.UnregisteredError("expired")),
                SimpleNamespace(success=False, exception=messaging.SenderIdMismatchError("wrong sender")),
            ],
        )
        delete_invalid_tokens.return_value = 2

        delivered = send_push_notification(
            tokens=["token-1", "token-2", "token-3"],
            title="Reel pinned in ReelPin",
            body="Your saved reel is ready in ReelPin.",
            data={"type": "reel_ready"},
        )

        self.assertEqual(delivered, 1)
        get_firebase_app.assert_called_once()
        delete_invalid_tokens.assert_called_once_with(["token-2", "token-3"])


if __name__ == "__main__":
    unittest.main()
