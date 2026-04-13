import unittest

from app.services.security import (
    build_secret_configuration_summary,
    redact_sensitive_text,
    secret_configuration_warnings,
)


class SecurityTests(unittest.TestCase):
    def test_redact_sensitive_text_masks_cookie_and_token_values(self):
        raw = (
            "authorization=Bearer abc123 token=xyz789 "
            "sessionid=secret-cookie csrftoken=another-cookie"
        )

        redacted = redact_sensitive_text(raw)

        self.assertNotIn("abc123", redacted)
        self.assertNotIn("xyz789", redacted)
        self.assertNotIn("secret-cookie", redacted)
        self.assertNotIn("another-cookie", redacted)
        self.assertIn("[REDACTED]", redacted)

    def test_secret_configuration_summary_is_safe(self):
        summary = build_secret_configuration_summary(
            {
                "SUPABASE_SERVICE_ROLE_KEY": "secret",
                "FIREBASE_SERVICE_ACCOUNT_JSON": '{"private_key":"secret"}',
                "INSTAGRAM_ACTIVE_COOKIE_DATA_BASE64": "Zm9v",
                "SUPABASE_KEY": "",
            }
        )

        self.assertEqual(summary["supabase_key_source"], "SUPABASE_SERVICE_ROLE_KEY")
        self.assertEqual(summary["firebase_credential_source"], "FIREBASE_SERVICE_ACCOUNT_JSON")
        self.assertTrue(summary["cookie_slots"]["instagram"]["active"])
        self.assertEqual(summary["deprecated_envs_in_use"], [])

    def test_secret_configuration_warnings_flag_legacy_env_usage(self):
        warnings = secret_configuration_warnings(
            {
                "SUPABASE_KEY": "legacy-key",
                "INSTAGRAM_COOKIE_DATA": "legacy-cookie",
            }
        )

        self.assertTrue(any("Legacy secret env names" in warning for warning in warnings))


if __name__ == "__main__":
    unittest.main()
