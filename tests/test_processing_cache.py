import unittest

from app.models import ExtractedData, Location
from app.services.processing_cache import (
    build_processing_cache_payload,
    cache_record_to_result,
    deserialize_extracted_data,
)


class ProcessingCacheTests(unittest.TestCase):
    def test_cache_payload_round_trip_preserves_extracted_fields(self):
        extracted = ExtractedData(
            title="Mumbai cafe list",
            summary="A short reel about three cafes in Bandra.",
            content_domain="Food",
            content_format="Cafe Guides",
            topical_tags=["bandra cafes", "mumbai food", "weekend spots"],
            category="Entertainment & Lifestyle",
            subcategory="Food & Restaurants",
            secondary_categories=["Travel & Places"],
            key_facts=["Open late", "Pet friendly"],
            locations=[
                Location(
                    name="Candies",
                    city="Mumbai",
                    state="Maharashtra",
                    country="India",
                    latitude=19.06,
                    longitude=72.83,
                )
            ],
            people_mentioned=["Candies"],
            actionable_items=["Save the cafe list"],
        )

        payload = build_processing_cache_payload(
            source_platform="instagram",
            source_content_id="ABC123",
            source_content_type="reel",
            normalized_url="https://www.instagram.com/reel/ABC123/",
            processing_version="reelpin-v1",
            ingestion_method="url_download",
            transcript_source="groq_whisper",
            transcript="Best Bandra cafes to try this weekend.",
            caption="Three Bandra spots worth saving",
            extracted=extracted,
        )

        result = cache_record_to_result(payload)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source_platform, "instagram")
        self.assertEqual(result.source_content_id, "ABC123")
        self.assertEqual(result.processing_version, "reelpin-v1")
        self.assertEqual(result.ingestion_method, "url_download")
        self.assertEqual(result.transcript_source, "groq_whisper")
        self.assertEqual(result.transcript, "Best Bandra cafes to try this weekend.")
        self.assertEqual(result.caption, "Three Bandra spots worth saving")
        self.assertEqual(result.extracted.content_domain, "Food")
        self.assertEqual(result.extracted.content_format, "Cafe Guides")
        self.assertEqual(result.extracted.topical_tags, ["bandra cafes", "mumbai food", "weekend spots"])
        self.assertEqual(result.extracted.title, extracted.title)
        self.assertEqual(result.extracted.locations[0].name, "Candies")

    def test_deserialize_extracted_data_ignores_bad_locations(self):
        extracted = deserialize_extracted_data(
            {
                "title": "Test",
                "locations": [
                    {"name": "Valid place", "city": "Pune", "country": "India"},
                    "bad",
                    {"city": "Missing name is skipped"},
                ],
                "key_facts": ["one", "", None],
            }
        )

        self.assertEqual(extracted.title, "Test")
        self.assertEqual(len(extracted.locations), 1)
        self.assertEqual(extracted.locations[0].name, "Valid place")
        self.assertEqual(extracted.key_facts, ["one"])


if __name__ == "__main__":
    unittest.main()
