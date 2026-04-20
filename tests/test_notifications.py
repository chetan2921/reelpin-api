import sys
from types import ModuleType
import unittest
from types import SimpleNamespace
from unittest.mock import patch

firebase_admin = ModuleType("firebase_admin")
firebase_exceptions = ModuleType("firebase_admin.exceptions")
firebase_credentials = ModuleType("firebase_admin.credentials")
firebase_messaging = ModuleType("firebase_admin.messaging")
pydantic_settings = ModuleType("pydantic_settings")


class _FirebaseError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class _Notification:
    def __init__(self, *, title, body):
        self.title = title
        self.body = body


class _AndroidNotification:
    def __init__(
        self,
        *,
        channel_id,
        priority,
        default_sound,
        sound=None,
        tag=None,
        click_action=None,
    ):
        self.channel_id = channel_id
        self.priority = priority
        self.default_sound = default_sound
        self.sound = sound
        self.tag = tag
        self.click_action = click_action


class _AndroidConfig:
    def __init__(self, *, priority, collapse_key=None, notification=None):
        self.priority = priority
        self.collapse_key = collapse_key
        self.notification = notification


class _Aps:
    def __init__(
        self,
        *,
        sound=None,
        content_available=False,
        mutable_content=False,
        category=None,
    ):
        self.sound = sound
        self.content_available = content_available
        self.mutable_content = mutable_content
        self.category = category


class _APNSPayload:
    def __init__(self, *, aps):
        self.aps = aps


class _APNSConfig:
    def __init__(self, *, headers, payload):
        self.headers = headers
        self.payload = payload


class _MulticastMessage:
    def __init__(self, *, notification, data, tokens, android, apns):
        self.notification = notification
        self.data = data
        self.tokens = tokens
        self.android = android
        self.apns = apns


class _UnregisteredError(_FirebaseError):
    def __init__(self, message: str):
        super().__init__("unregistered", message)


class _SenderIdMismatchError(_FirebaseError):
    def __init__(self, message: str):
        super().__init__("sender-id-mismatch", message)


class _UnavailableError(_FirebaseError):
    def __init__(self, message: str):
        super().__init__("unavailable", message)


firebase_exceptions.FirebaseError = _FirebaseError
firebase_credentials.Certificate = lambda payload: payload
firebase_messaging.Notification = _Notification
firebase_messaging.AndroidNotification = _AndroidNotification
firebase_messaging.AndroidConfig = _AndroidConfig
firebase_messaging.Aps = _Aps
firebase_messaging.APNSPayload = _APNSPayload
firebase_messaging.APNSConfig = _APNSConfig
firebase_messaging.MulticastMessage = _MulticastMessage
firebase_messaging.UnregisteredError = _UnregisteredError
firebase_messaging.SenderIdMismatchError = _SenderIdMismatchError
firebase_messaging.UnavailableError = _UnavailableError
firebase_messaging.send_each_for_multicast = lambda message: None
firebase_admin.exceptions = firebase_exceptions
firebase_admin.credentials = firebase_credentials
firebase_admin.messaging = firebase_messaging
firebase_admin.App = object
firebase_admin.initialize_app = lambda credential: SimpleNamespace(credential=credential)
pydantic_settings.BaseSettings = object

sys.modules.setdefault("firebase_admin", firebase_admin)
sys.modules.setdefault("firebase_admin.exceptions", firebase_exceptions)
sys.modules.setdefault("firebase_admin.credentials", firebase_credentials)
sys.modules.setdefault("firebase_admin.messaging", firebase_messaging)
sys.modules.setdefault("pydantic_settings", pydantic_settings)

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
        self.assertEqual(message.android.notification.click_action, "FLUTTER_NOTIFICATION_CLICK")
        self.assertEqual(message.apns.headers["apns-priority"], "10")
        self.assertEqual(message.apns.headers["apns-push-type"], "alert")
        self.assertEqual(message.apns.payload.aps.sound, "default")
        self.assertTrue(message.apns.payload.aps.content_available)
        self.assertTrue(message.apns.payload.aps.mutable_content)
        self.assertEqual(message.apns.payload.aps.category, "REEL_READY")

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

    @patch("app.services.notifications.time.sleep")
    @patch("app.services.notifications._get_firebase_app")
    @patch("app.services.notifications.messaging.send_each_for_multicast")
    def test_send_push_notification_retries_transient_fcm_failures(
        self,
        send_each_for_multicast,
        get_firebase_app,
        sleep,
    ):
        send_each_for_multicast.side_effect = [
            messaging.UnavailableError("temporary"),
            SimpleNamespace(
                success_count=1,
                failure_count=0,
                responses=[SimpleNamespace(success=True, exception=None)],
            ),
        ]

        delivered = send_push_notification(
            tokens=[" token-1 ", "token-1"],
            title="Reel pinned in ReelPin",
            body="Your saved reel is ready in ReelPin.",
            data={"type": "reel_ready"},
        )

        self.assertEqual(delivered, 1)
        get_firebase_app.assert_called_once()
        self.assertEqual(send_each_for_multicast.call_count, 2)
        sleep.assert_called_once_with(1.0)


if __name__ == "__main__":
    unittest.main()
