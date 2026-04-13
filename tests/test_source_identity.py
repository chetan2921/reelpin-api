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

    def test_youtube_shorts_and_watch_urls_collapse_to_same_video(self):
        shorts_identity = resolve_source_identity(
            "https://youtube.com/shorts/abc123XYZ09?feature=share"
        )
        watch_identity = resolve_source_identity(
            "https://www.youtube.com/watch?v=abc123XYZ09&si=foo"
        )

        self.assertEqual(
            shorts_identity.normalized_url,
            "https://www.youtube.com/watch?v=abc123XYZ09",
        )
        self.assertEqual(shorts_identity.normalized_url, watch_identity.normalized_url)
        self.assertEqual(shorts_identity.source_content_type, "short")

    def test_tiktok_short_links_normalize_to_share_path(self):
        identity = resolve_source_identity("https://vm.tiktok.com/ZM123abc/?utm_source=copy")

        self.assertEqual(identity.source_platform, "tiktok")
        self.assertEqual(identity.source_content_type, "share")
        self.assertEqual(identity.source_content_id, "ZM123abc")
        self.assertEqual(
            identity.normalized_url,
            "https://www.tiktok.com/t/ZM123abc",
        )

    def test_generic_urls_are_sorted_and_tracking_params_removed(self):
        identity = resolve_source_identity(
            "https://Example.com/path/?b=2&utm_medium=social&a=1"
        )

        self.assertEqual(identity.source_platform, "web")
        self.assertEqual(identity.normalized_url, "https://example.com/path?a=1&b=2")


if __name__ == "__main__":
    unittest.main()
