import unittest
from unittest.mock import patch

from app.services.ops_alerts import maybe_send_instagram_cookie_alert


class OpsAlertTests(unittest.TestCase):
    @patch("app.services.ops_alerts._upsert_service_health")
    @patch("app.services.ops_alerts._send_push_notification")
    @patch("app.services.ops_alerts._get_device_tokens")
    @patch("app.services.ops_alerts._get_service_health")
    @patch("app.services.ops_alerts.inspect_instagram_cookie_slots")
    def test_sends_instagram_cookie_alert_when_unhealthy(
        self,
        inspect_slots,
        get_alert_state,
        get_tokens,
        send_push,
        upsert_health,
    ):
        inspect_slots.return_value = [
            {
                "slot": "active",
                "configured": True,
                "healthy": False,
                "warning": "sessionid cookie is expired",
                "session_expires_at": None,
            }
        ]
        get_alert_state.return_value = None
        get_tokens.return_value = ["token-1"]
        send_push.return_value = 1

        class Settings:
            ADMIN_ALERT_USER_ID = "user-1"
            ADMIN_ALERT_COOLDOWN_MINUTES = 60

        maybe_send_instagram_cookie_alert(Settings())

        send_push.assert_called_once()
        upsert_health.assert_called()


if __name__ == "__main__":
    unittest.main()
