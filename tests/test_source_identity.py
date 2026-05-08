import unittest

from app.services.source_identity import resolve_source_identity


class SourceIdentityTests(unittest.TestCase):
    def test_instagram_reel_query_params_are_removed(self):
        identity = resolve_source_identity(
            "https://www.instagram.com/reel/C8abc123/?utm_source=ig_web_copy_link&igsh=abc"
        )

        self.assertEqual(identity.source_platform, "instagram")
        self.assertEqual(identity.source_content_type, "reel")
        self.assertEqual(identity.source_content_id, "C8abc123")
        self.assertEqual(
            identity.normalized_url,
            "https://www.instagram.com/reel/C8abc123/",
        )

    def test_instagram_post_normalizes_to_canonical_path(self):
        identity = resolve_source_identity("https://instagr.am/p/C8xyz999/?igsh=foo")

        self.assertEqual(identity.source_platform, "instagram")
        self.assertEqual(identity.source_content_type, "post")
        self.assertEqual(identity.source_content_id, "C8xyz999")
        self.assertEqual(
            identity.normalized_url,
            "https://www.instagram.com/p/C8xyz999/",
        )

    def test_non_instagram_url_raises(self):
        with self.assertRaises(ValueError):
            resolve_source_identity("https://www.youtube.com/watch?v=abc123XYZ09")

    def test_blank_url_raises(self):
        with self.assertRaises(ValueError):
            resolve_source_identity("")


if __name__ == "__main__":
    unittest.main()
