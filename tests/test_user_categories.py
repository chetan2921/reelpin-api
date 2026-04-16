import sys
import unittest
from types import ModuleType, SimpleNamespace

config_stub = None
database_stub = None
embedder_stub = None
extractor_stub = None

if "app.config" not in sys.modules:
    config_stub = ModuleType("app.config")
    config_stub.get_settings = lambda: SimpleNamespace(LLM_MODEL="test-model")
    sys.modules["app.config"] = config_stub

if "app.services.database" not in sys.modules:
    database_stub = ModuleType("app.services.database")
    database_stub.list_user_category_pairs = lambda user_id: []
    database_stub.list_user_reels_for_recategorization = lambda user_id, limit=200: []
    database_stub.update_reel_fields = lambda reel_id, updates: None
    sys.modules["app.services.database"] = database_stub

if "app.services.embedder" not in sys.modules:
    embedder_stub = ModuleType("app.services.embedder")
    embedder_stub.embed_and_store = lambda **kwargs: None
    sys.modules["app.services.embedder"] = embedder_stub

if "app.services.extractor" not in sys.modules:
    extractor_stub = ModuleType("app.services.extractor")
    extractor_stub.get_groq_client = lambda: None
    sys.modules["app.services.extractor"] = extractor_stub

from app.models import ExtractedData
from app.services.user_categories import (
    _fallback_assignment,
    _normalize_assignment,
    build_user_category_filters,
)

if config_stub is not None:
    del sys.modules["app.config"]
if database_stub is not None:
    del sys.modules["app.services.database"]
if embedder_stub is not None:
    del sys.modules["app.services.embedder"]
if extractor_stub is not None:
    del sys.modules["app.services.extractor"]


class UserCategoriesTests(unittest.TestCase):
    def test_build_user_category_filters_dedupes_and_skips_generic_labels(self):
        filters = build_user_category_filters(
            [
                {"category": "Movies", "subcategory": "Trailers"},
                {"category": "movies", "subcategory": "Trailers"},
                {"category": "Movies", "subcategory": "Reviews"},
                {"category": "Other", "subcategory": "General"},
                {"category": "", "subcategory": ""},
            ]
        )

        self.assertEqual(
            filters,
            [
                {
                    "category": "Movies",
                    "subcategories": ["Reviews", "Trailers"],
                }
            ],
        )

    def test_fallback_assignment_detects_movie_trailer_content(self):
        assignment = _fallback_assignment(
            ExtractedData(
                title="Big action movie trailer",
                summary="A teaser for an upcoming film release.",
                content_domain="Movies",
                content_format="Trailers",
            ),
            transcript="Official trailer for the new action film releasing next month.",
            caption="Teaser trailer out now",
        )

        self.assertEqual(assignment.category, "Movies")
        self.assertEqual(assignment.subcategory, "Trailers")

    def test_normalize_assignment_reuses_existing_labels(self):
        extracted = ExtractedData(
            title="Film review",
            summary="A review of a new movie.",
            content_domain="Movies",
            content_format="Reviews",
        )
        assignment = _normalize_assignment(
            {
                "category": "movies",
                "subcategory": "reviews",
                "secondary_categories": ["box office"],
            },
            [{"category": "Movies", "subcategories": ["Reviews", "Trailers"]}],
            extracted,
            "A spoiler-free movie review.",
            "",
        )

        self.assertEqual(assignment.category, "Movies")
        self.assertEqual(assignment.subcategory, "Reviews")
        self.assertEqual(assignment.secondary_categories, ["Box Office"])


if __name__ == "__main__":
    unittest.main()
