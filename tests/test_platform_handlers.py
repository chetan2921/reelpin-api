import unittest

from app.services.platform_handlers import platform_handler_key
from app.services.source_identity import resolve_source_identity


class PlatformHandlerTests(unittest.TestCase):
    def test_instagram_reel_routes_to_reel_handler(self):
        source = resolve_source_identity("https://www.instagram.com/reel/ABC123/")
        self.assertEqual(platform_handler_key(source), "instagram_reel")

    def test_instagram_post_routes_to_post_handler(self):
        source = resolve_source_identity("https://www.instagram.com/p/ABC123/")
        self.assertEqual(platform_handler_key(source), "instagram_post")

    def test_youtube_shorts_routes_to_short_handler(self):
        source = resolve_source_identity("https://www.youtube.com/shorts/abc123XYZ09")
        self.assertEqual(platform_handler_key(source), "youtube_short")

    def test_tiktok_routes_to_tiktok_handler(self):
        source = resolve_source_identity("https://www.tiktok.com/@creator/video/123456")
        self.assertEqual(platform_handler_key(source), "tiktok")


if __name__ == "__main__":
    unittest.main()
