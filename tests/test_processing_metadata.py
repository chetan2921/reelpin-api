import unittest

from app.services.processing_metadata import (
    PROCESSING_VERSION,
    build_direct_upload_metadata,
    build_url_processing_metadata,
)
from app.services.source_identity import resolve_source_identity


class ProcessingMetadataTests(unittest.TestCase):
    def test_build_url_processing_metadata_for_youtube_transcript(self):
        source = resolve_source_identity("https://www.youtube.com/shorts/abc123XYZ09")
        metadata = build_url_processing_metadata(
            source,
            ingestion_method="youtube_short_pipeline",
            transcript_source="youtube_transcript_api",
        )

        self.assertEqual(metadata["source_platform"], "youtube")
        self.assertEqual(metadata["source_content_type"], "short")
        self.assertEqual(metadata["processing_version"], PROCESSING_VERSION)
        self.assertEqual(metadata["ingestion_method"], "youtube_short_pipeline")
        self.assertEqual(metadata["transcript_source"], "youtube_transcript_api")

    def test_build_direct_upload_metadata_without_url(self):
        metadata = build_direct_upload_metadata("")

        self.assertEqual(metadata["normalized_url"], "direct-upload")
        self.assertEqual(metadata["source_platform"], "upload")
        self.assertEqual(metadata["ingestion_method"], "direct_upload")
        self.assertEqual(metadata["transcript_source"], "groq_whisper")


if __name__ == "__main__":
    unittest.main()
