import unittest

from app.models import DevicePushTokenInput


class DevicePushTokenInputTests(unittest.TestCase):
    def test_accepts_snake_case_payload(self):
        payload = DevicePushTokenInput(
            user_id="user-1",
            token="token-1",
            platform="ios",
        )

        self.assertEqual(payload.user_id, "user-1")
        self.assertEqual(payload.token, "token-1")
        self.assertEqual(payload.platform, "ios")

    def test_accepts_common_frontend_aliases(self):
        payload = DevicePushTokenInput.model_validate(
            {
                "userId": "user-1",
                "fcmToken": "token-1",
                "devicePlatform": "android",
            }
        )

        self.assertEqual(payload.user_id, "user-1")
        self.assertEqual(payload.token, "token-1")
        self.assertEqual(payload.platform, "android")


if __name__ == "__main__":
    unittest.main()
