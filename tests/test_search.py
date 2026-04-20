import asyncio
import unittest
from unittest.mock import patch

from app.main import search_reels
from app.models import SearchQuery


def _record(
    reel_id: str,
    *,
    title: str,
    summary: str = "",
    category: str = "Travel",
    subcategory: str = "Coffee Shops",
) -> dict:
    return {
        "id": reel_id,
        "user_id": "user-123",
        "url": f"https://example.com/{reel_id}",
        "title": title,
        "summary": summary,
        "transcript": "",
        "category": category,
        "subcategory": subcategory,
        "secondary_categories": [],
        "key_facts": [],
        "locations": [],
        "people_mentioned": [],
        "actionable_items": [],
        "created_at": "2026-04-20T00:00:00+00:00",
    }


class SearchTests(unittest.TestCase):
    def test_search_reels_falls_back_to_backend_lexical_search_when_semantic_search_fails(
        self,
    ):
        query = SearchQuery(query="coffee", user_id="user-123", limit=5)
        backend_records = [
            _record("travel-1", title="Best coffee in Bangalore"),
            _record(
                "travel-2",
                title="Weekend hikes",
                category="Outdoors",
                subcategory="Trails",
            ),
        ]

        with patch("app.main.search_similar", side_effect=RuntimeError("pinecone down")):
            with patch("app.main.get_reels", return_value=backend_records) as get_reels:
                response = asyncio.run(search_reels(query))

        self.assertEqual(response.total, 1)
        self.assertEqual(response.results[0].reel.id, "travel-1")
        self.assertGreater(response.results[0].relevance_score, 0.0)
        get_reels.assert_called_once()

    def test_search_reels_supplements_sparse_semantic_results_with_backend_fallback(self):
        query = SearchQuery(query="coffee", user_id="user-123", limit=2)
        semantic_record = _record("travel-1", title="Coffee crawl")
        fallback_record = _record("travel-2", title="Best coffee in town")

        with patch(
            "app.main.search_similar",
            return_value=[{"reel_id": "travel-1", "score": 0.91, "metadata": {}}],
        ):
            with patch("app.main.get_reels_by_ids", return_value=[semantic_record]):
                with patch("app.main.get_reels", return_value=[semantic_record, fallback_record]):
                    response = asyncio.run(search_reels(query))

        self.assertEqual(response.total, 2)
        self.assertEqual(response.results[0].reel.id, "travel-1")
        self.assertEqual(response.results[1].reel.id, "travel-2")


if __name__ == "__main__":
    unittest.main()
