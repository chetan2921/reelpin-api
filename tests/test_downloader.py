import json
import sys
import tempfile
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

yt_dlp_stub = None
if "yt_dlp" not in sys.modules:
    yt_dlp_stub = ModuleType("yt_dlp")

    class _DownloadError(Exception):
        pass

    class _YoutubeDL:
        pass

    yt_dlp_stub.utils = SimpleNamespace(DownloadError=_DownloadError)
    yt_dlp_stub.YoutubeDL = _YoutubeDL
    sys.modules["yt_dlp"] = yt_dlp_stub

config_stub = None
if "app.config" not in sys.modules:
    config_stub = ModuleType("app.config")

    def _missing_get_settings():
        raise AssertionError("get_settings should be patched in downloader tests")

    config_stub.get_settings = _missing_get_settings
    sys.modules["app.config"] = config_stub

from app.services import downloader
from app.services.downloader import CookieSlot, DownloadedMedia

if yt_dlp_stub is not None:
    del sys.modules["yt_dlp"]
if config_stub is not None:
    del sys.modules["app.config"]


class _FakeHTTPResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DownloaderTests(unittest.TestCase):
    @patch("app.services.downloader._download_authenticated_instagram_media")
    @patch("app.services.downloader._try_instagram_apify_fallback")
    @patch("app.services.downloader._download_public_instagram_media")
    @patch("app.services.downloader._ordered_cookie_slots")
    @patch("app.services.downloader._build_cookie_slots_from_env")
    @patch("app.services.downloader.get_settings")
    @patch("app.services.downloader.logger.warning")
    def test_download_media_tries_apify_before_public_and_cookie_slots(
        self,
        mock_logger_warning,
        mock_get_settings,
        mock_build_cookie_slots,
        mock_ordered_cookie_slots,
        mock_public_fetch,
        mock_apify_fallback,
        mock_authenticated_fetch,
    ):
        with tempfile.TemporaryDirectory() as download_dir:
            mock_get_settings.return_value = SimpleNamespace(
                TEMP_DOWNLOAD_DIR=download_dir,
                YTDLP_COOKIES_FROM_BROWSER=None,
                APIFY_API_TOKEN="apify_api_test",
                APIFY_INSTAGRAM_ACTOR_ID="apify/instagram-scraper",
            )
            cookie_slot = CookieSlot(
                index=1,
                label="active",
                file_path="/tmp/instagram-cookie-slot.txt",
            )
            mock_build_cookie_slots.return_value = ([cookie_slot], [])
            mock_ordered_cookie_slots.return_value = [cookie_slot]
            mock_public_fetch.side_effect = Exception(
                "Instagram did not expose a public media URL for this page."
            )
            mock_apify_fallback.return_value = DownloadedMedia(
                media_type="video",
                media_paths=[f"{download_dir}/apify-video.mp4"],
                caption="Apify caption",
            )

            media = downloader.download_media("https://www.instagram.com/reel/ABC123/")

        self.assertEqual(media.caption, "Apify caption")
        self.assertEqual(media.media_type, "video")
        mock_apify_fallback.assert_called_once()
        mock_public_fetch.assert_not_called()
        mock_authenticated_fetch.assert_not_called()

    @patch("app.services.downloader._download_remote_file")
    @patch("app.services.downloader.urllib.request.urlopen")
    def test_download_instagram_media_via_apify_requests_actor_run_for_reels(
        self,
        mock_urlopen,
        mock_download_remote_file,
    ):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            captured["headers"] = dict(request.header_items())
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            captured["timeout"] = timeout
            return _FakeHTTPResponse(
                [
                    {
                        "caption": "Reel caption",
                        "videoUrl": "https://cdn.example.com/reel.mp4",
                    }
                ]
            )

        mock_urlopen.side_effect = fake_urlopen

        with tempfile.TemporaryDirectory() as download_dir:
            media = downloader._download_instagram_media_via_apify(
                "https://www.instagram.com/reel/ABC123/",
                download_dir,
                api_token="apify_api_test",
                actor_id="apify/instagram-scraper",
            )

        self.assertEqual(
            captured["url"],
            "https://api.apify.com/v2/acts/apify~instagram-scraper/run-sync-get-dataset-items",
        )
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["headers"]["Authorization"], "Bearer apify_api_test")
        self.assertEqual(
            captured["payload"],
            {
                "directUrls": ["https://www.instagram.com/reel/ABC123/"],
                "resultsLimit": 1,
                "resultsType": "reels",
            },
        )
        self.assertEqual(captured["timeout"], 120)
        self.assertEqual(media.media_type, "video")
        self.assertEqual(media.caption, "Reel caption")
        mock_download_remote_file.assert_called_once()

    @patch("app.services.downloader._download_remote_file")
    @patch("app.services.downloader.urllib.request.urlopen")
    def test_download_instagram_media_via_apify_downloads_post_images(
        self,
        mock_urlopen,
        mock_download_remote_file,
    ):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeHTTPResponse(
                [
                    {
                        "caption": "Post caption",
                        "childPosts": [
                            {"displayUrl": "https://cdn.example.com/image-1.jpg"},
                            {"displayUrl": "https://cdn.example.com/image-2.jpg"},
                        ],
                    }
                ]
            )

        mock_urlopen.side_effect = fake_urlopen

        with tempfile.TemporaryDirectory() as download_dir:
            media = downloader._download_instagram_media_via_apify(
                "https://www.instagram.com/p/ABC123/",
                download_dir,
                api_token="apify_api_test",
                actor_id="apify/instagram-scraper",
            )

        self.assertEqual(
            captured["payload"],
            {
                "directUrls": ["https://www.instagram.com/p/ABC123/"],
                "resultsLimit": 1,
                "resultsType": "posts",
            },
        )
        self.assertEqual(media.media_type, "image")
        self.assertEqual(media.caption, "Post caption")
        self.assertEqual(len(media.media_paths), 2)
        self.assertEqual(mock_download_remote_file.call_count, 2)


if __name__ == "__main__":
    unittest.main()
