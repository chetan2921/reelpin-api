import json
import logging
import re
from dataclasses import dataclass

from app.config import get_settings
from app.models import ExtractedData
from app.services.database import (
    list_user_category_pairs,
    list_user_reels_for_recategorization,
    update_reel_fields,
)
from app.services.embedder import embed_and_store
from app.services.extractor import get_groq_client

logger = logging.getLogger(__name__)

_GENERIC_LABELS = {
    "",
    "all",
    "all reels",
    "content",
    "entertainment & lifestyle",
    "general",
    "highlights",
    "humor & memes",
    "knowledge & learning",
    "misc",
    "miscellaneous",
    "other",
    "practical & utility",
    "saved reels",
    "skills & hobbies",
    "uncategorized",
}

_CATEGORY_PROMPT = """You organize saved reels for a single user.

Your job is to assign each reel into a clean 2-level personal taxonomy:
- category: broad, reusable bucket, 1-3 words
- subcategory: more specific reusable bucket under that category, 1-4 words

CONTENT:
- Title: {title}
- Summary: {summary}
- Source Platform: {source_platform}
- Source Content Type: {source_content_type}
- Content Domain Hint: {content_domain}
- Content Format Hint: {content_format}
- Topical Tags: {topical_tags}
- Transcript:
{transcript}
- Caption:
{caption}

EXISTING USER CATEGORY TREE:
{existing_tree}

Return JSON only:
{{
  "category": "Primary category label",
  "subcategory": "Primary subcategory label",
  "secondary_categories": ["Up to 2 extra related labels if truly useful"]
}}

Rules:
- Reuse an existing category and subcategory when they fit well.
- Only create a new category if the current tree does not fit.
- Avoid vague labels like Other, General, Misc, Entertainment, Lifestyle, Viral, Funny.
- Keep labels user-facing and clean.
- Use title case.
- Do not include punctuation-heavy labels.
- For movie or film content, prefer clear buckets like Movies > Trailers, Movies > Reviews, Movies > Scenes, Movies > Fan Edits, Movies > News.
- For TV or series content, prefer TV & Series > Trailers, TV & Series > Reviews, TV & Series > Scenes, TV & Series > News.
- secondary_categories should be concise and optional. Return an empty array when not needed.
"""


@dataclass(frozen=True)
class UserCategoryAssignment:
    category: str
    subcategory: str
    secondary_categories: list[str]


def build_user_category_filters(records: list[dict]) -> list[dict]:
    grouped: dict[str, dict[str, str | set[str]]] = {}

    for record in records:
        category = _normalize_new_label(record.get("category"))
        subcategory = _normalize_new_label(record.get("subcategory"))

        if _is_generic_label(category):
            continue

        category_key = _label_key(category)
        grouped.setdefault(
            category_key,
            {
                "category": category,
                "subcategories": set(),
            },
        )
        if subcategory and not _is_generic_label(subcategory) and subcategory != category:
            grouped[category_key]["subcategories"].add(subcategory)

    return [
        {
            "category": str(group["category"]),
            "subcategories": sorted(group["subcategories"], key=str.lower),
        }
        for _, group in sorted(grouped.items(), key=lambda item: item[0])
    ]


def assign_user_category(
    *,
    user_id: str,
    extracted: ExtractedData,
    transcript: str,
    caption: str | None,
    source_platform: str,
    source_content_type: str | None,
    existing_category_records: list[dict] | None = None,
) -> UserCategoryAssignment:
    category_records = existing_category_records
    if category_records is None:
        category_records = list_user_category_pairs(user_id=user_id)

    existing_filters = build_user_category_filters(category_records)
    prompt = _CATEGORY_PROMPT.format(
        title=extracted.title or "Untitled",
        summary=extracted.summary or "",
        source_platform=source_platform or "",
        source_content_type=source_content_type or "",
        content_domain=extracted.content_domain or "",
        content_format=extracted.content_format or "",
        topical_tags=", ".join(extracted.topical_tags[:6]) or "(none)",
        transcript=(transcript or "(no transcript available)")[:4000],
        caption=(caption or "(no caption provided)")[:1000],
        existing_tree=json.dumps(existing_filters, ensure_ascii=True) if existing_filters else "[]",
    )

    try:
        client = get_groq_client()
        settings = get_settings()
        response = client.chat.completions.create(
            model=settings.LLM_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You assign precise personal content categories. Always return valid JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=350,
            response_format={"type": "json_object"},
        )
        raw_response = response.choices[0].message.content
        data = json.loads(raw_response)
        return _normalize_assignment(data, existing_filters, extracted, transcript, caption)
    except Exception as e:
        logger.warning("User category assignment fell back to deterministic labels: %s", e)
        return _fallback_assignment(extracted, transcript, caption)


def recategorize_user_reels(*, user_id: str, limit: int = 200) -> dict:
    records = list_user_reels_for_recategorization(user_id=user_id, limit=limit)
    dynamic_tree: list[dict] = []
    reviewed = 0
    updated = 0

    for record in records:
        reviewed += 1
        extracted = ExtractedData(
            title=str(record.get("title") or ""),
            summary=str(record.get("summary") or ""),
            category=str(record.get("category") or "Other"),
            subcategory=str(record.get("subcategory") or "Other"),
            secondary_categories=_string_list(record.get("secondary_categories")),
            key_facts=_string_list(record.get("key_facts")),
            people_mentioned=_string_list(record.get("people_mentioned")),
            actionable_items=_string_list(record.get("actionable_items")),
        )
        assignment = assign_user_category(
            user_id=user_id,
            extracted=extracted,
            transcript=str(record.get("transcript") or ""),
            caption="",
            source_platform=str(record.get("source_platform") or ""),
            source_content_type=str(record.get("source_content_type") or ""),
            existing_category_records=dynamic_tree,
        )
        dynamic_tree.append(
            {
                "category": assignment.category,
                "subcategory": assignment.subcategory,
                "secondary_categories": assignment.secondary_categories,
            }
        )

        if (
            record.get("category") == assignment.category
            and record.get("subcategory") == assignment.subcategory
            and _string_list(record.get("secondary_categories")) == assignment.secondary_categories
        ):
            continue

        update_reel_fields(
            record["id"],
            {
                "category": assignment.category,
                "subcategory": assignment.subcategory,
                "secondary_categories": assignment.secondary_categories,
            },
        )

        embed_and_store(
            reel_id=record["id"],
            text=_build_search_text(
                title=str(record.get("title") or ""),
                summary=str(record.get("summary") or ""),
                category=assignment.category,
                subcategory=assignment.subcategory,
                secondary_categories=assignment.secondary_categories,
                transcript=str(record.get("transcript") or ""),
            ),
            metadata={
                "user_id": user_id,
                "title": str(record.get("title") or ""),
                "category": assignment.category,
                "subcategory": assignment.subcategory,
                "summary": str(record.get("summary") or ""),
            },
        )
        updated += 1

    return {
        "user_id": user_id,
        "reviewed": reviewed,
        "updated": updated,
        "categories": build_user_category_filters(dynamic_tree),
    }


def _normalize_assignment(
    data: dict,
    existing_filters: list[dict],
    extracted: ExtractedData,
    transcript: str,
    caption: str | None,
) -> UserCategoryAssignment:
    existing_category_lookup = {
        _label_key(item["category"]): item["category"] for item in existing_filters
    }
    existing_subcategory_lookup = {
        _label_key(item["category"]): {
            _label_key(subcategory): subcategory
            for subcategory in item.get("subcategories", [])
        }
        for item in existing_filters
    }

    raw_category = _normalize_label(data.get("category"))
    raw_subcategory = _normalize_label(data.get("subcategory"))

    category = existing_category_lookup.get(_label_key(raw_category))
    if category is None:
        category = _normalize_new_label(raw_category)

    if _is_generic_label(category):
        fallback = _fallback_assignment(extracted, transcript, caption)
        category = fallback.category
        raw_subcategory = raw_subcategory or fallback.subcategory

    subcategory = existing_subcategory_lookup.get(_label_key(category), {}).get(
        _label_key(raw_subcategory)
    )
    if subcategory is None:
        subcategory = _normalize_new_label(raw_subcategory)

    if _is_generic_label(subcategory) or _label_key(subcategory) == _label_key(category):
        fallback = _fallback_assignment(extracted, transcript, caption)
        if _label_key(category) == _label_key(fallback.category):
            subcategory = fallback.subcategory
        else:
            subcategory = _normalize_new_label(extracted.content_format) or "Highlights"

    secondary_categories: list[str] = []
    for value in _string_list(data.get("secondary_categories")):
        label = _normalize_new_label(value)
        if (
            not label
            or _is_generic_label(label)
            or _label_key(label) in {_label_key(category), _label_key(subcategory)}
            or label in secondary_categories
        ):
            continue
        secondary_categories.append(label)
        if len(secondary_categories) == 2:
            break

    return UserCategoryAssignment(
        category=category,
        subcategory=subcategory,
        secondary_categories=secondary_categories,
    )


def _fallback_assignment(
    extracted: ExtractedData,
    transcript: str,
    caption: str | None,
) -> UserCategoryAssignment:
    text = " ".join(
        [
            extracted.title,
            extracted.summary,
            extracted.content_domain,
            extracted.content_format,
            " ".join(extracted.topical_tags),
            transcript or "",
            caption or "",
        ]
    ).lower()

    if any(token in text for token in ["movie", "film", "cinema", "box office"]):
        return UserCategoryAssignment(
            category="Movies",
            subcategory=_movie_subcategory(text),
            secondary_categories=[],
        )

    if any(token in text for token in ["series", "episode", "season", "netflix show", "web series"]):
        return UserCategoryAssignment(
            category="TV & Series",
            subcategory=_series_subcategory(text),
            secondary_categories=[],
        )

    category = _normalize_new_label(extracted.content_domain) or _normalize_topic_fallback(extracted)
    subcategory = _normalize_new_label(extracted.content_format) or "Highlights"
    return UserCategoryAssignment(
        category=category or "Saved Reels",
        subcategory=subcategory,
        secondary_categories=[],
    )


def _movie_subcategory(text: str) -> str:
    if any(token in text for token in ["trailer", "teaser"]):
        return "Trailers"
    if any(token in text for token in ["review", "rating", "verdict"]):
        return "Reviews"
    if any(token in text for token in ["scene", "clip", "dialogue"]):
        return "Scenes"
    if any(token in text for token in ["fan edit", "edit"]):
        return "Fan Edits"
    if any(token in text for token in ["news", "update", "announcement", "release date"]):
        return "News"
    return "Clips"


def _series_subcategory(text: str) -> str:
    if any(token in text for token in ["trailer", "teaser"]):
        return "Trailers"
    if any(token in text for token in ["review", "rating", "verdict"]):
        return "Reviews"
    if any(token in text for token in ["scene", "clip", "dialogue"]):
        return "Scenes"
    if any(token in text for token in ["news", "update", "announcement", "release date"]):
        return "News"
    return "Highlights"


def _normalize_topic_fallback(extracted: ExtractedData) -> str:
    if extracted.topical_tags:
        return _normalize_new_label(extracted.topical_tags[0]) or "Saved Reels"
    return "Saved Reels"


def _build_search_text(
    *,
    title: str,
    summary: str,
    category: str,
    subcategory: str,
    secondary_categories: list[str],
    transcript: str,
) -> str:
    secondary = ", ".join(secondary_categories)
    return (
        f"{title}. {summary}. "
        f"Primary Category: {category}. "
        f"Subcategory: {subcategory}. "
        f"Secondary Categories: {secondary}. "
        f"Transcript: {transcript}"
    )


def _normalize_label(value) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip()
    cleaned = re.sub(r"[_/]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _normalize_new_label(value) -> str:
    cleaned = _normalize_label(value)
    if not cleaned:
        return ""

    words = re.findall(r"[A-Za-z0-9&+]+", cleaned)
    if not words:
        return ""

    normalized_words = []
    for word in words[:4]:
        lowered = word.lower()
        if lowered in {"tv", "ai", "ui", "ux", "3d", "2d", "vfx"}:
            normalized_words.append(word.upper())
        elif lowered == "&":
            normalized_words.append("&")
        else:
            normalized_words.append(word.capitalize())

    normalized = " ".join(normalized_words).replace(" Tv ", " TV ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if len(normalized.split()) > 4:
        normalized = " ".join(normalized.split()[:4])
    return normalized


def _is_generic_label(value: str) -> bool:
    return _label_key(value) in _GENERIC_LABELS


def _label_key(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    return [
        str(value).strip()
        for value in values
        if value is not None and str(value).strip()
    ]
