import base64
import unittest

from app.services.cookie_health import inspect_cookie_slot


class CookieHealthTests(unittest.TestCase):
    def test_cookie_slot_is_healthy_when_sessionid_is_present_and_future_dated(self):
        raw = "\n".join(
            [
                "# Netscape HTTP Cookie File",
                ".instagram.com\tTRUE\t/\tTRUE\t4102444800\tcsrftoken\tabc",
                ".instagram.com\tTRUE\t/\tTRUE\t4102444800\tsessionid\tdef",
            ]
        )
        settings = {
            "INSTAGRAM_ACTIVE_COOKIE_DATA_BASE64": base64.b64encode(raw.encode("utf-8")).decode("utf-8"),
        }

        slot = inspect_cookie_slot(settings, "instagram", "active")

        self.assertTrue(slot["configured"])
        self.assertTrue(slot["healthy"])
        self.assertIsNotNone(slot["session_expires_at"])

    def test_cookie_slot_is_unhealthy_when_sessionid_is_missing(self):
        raw = "\n".join(
            [
                "# Netscape HTTP Cookie File",
                ".instagram.com\tTRUE\t/\tTRUE\t4102444800\tcsrftoken\tabc",
            ]
        )
        settings = {
            "INSTAGRAM_ACTIVE_COOKIE_DATA": raw,
        }

        slot = inspect_cookie_slot(settings, "instagram", "active")

        self.assertTrue(slot["configured"])
        self.assertFalse(slot["healthy"])
        self.assertEqual(slot["warning"], "sessionid cookie is missing")


if __name__ == "__main__":
    unittest.main()
