import unittest

from app.services.processing_metadata import (
    PROCESSING_VERSION,
    build_direct_upload_metadata,
    build_url_processing_metadata,
)
from app.services.source_identity import resolve_source_identity


class ProcessingMetadataTests(unittest.TestCase):
    def test_build_url_processing_metadata_for_instagram_reel(self):
        source = resolve_source_identity("https://www.instagram.com/reel/ABC123/")
        metadata = build_url_processing_metadata(
            source,
            ingestion_method="instagram_reel_pipeline",
            transcript_source="groq_whisper",
        )

        self.assertEqual(metadata["source_platform"], "instagram")
        self.assertEqual(metadata["source_content_type"], "reel")
        self.assertEqual(metadata["processing_version"], PROCESSING_VERSION)
        self.assertEqual(metadata["ingestion_method"], "instagram_reel_pipeline")
        self.assertEqual(metadata["transcript_source"], "groq_whisper")

    def test_build_direct_upload_metadata_without_url(self):
        metadata = build_direct_upload_metadata("")

        self.assertEqual(metadata["normalized_url"], "direct-upload")
        self.assertEqual(metadata["source_platform"], "upload")
        self.assertEqual(metadata["ingestion_method"], "direct_upload")
        self.assertEqual(metadata["transcript_source"], "groq_whisper")


if __name__ == "__main__":
    unittest.main()
