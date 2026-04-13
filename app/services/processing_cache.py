from dataclasses import dataclass

from app.models import ExtractedData, Location


@dataclass(frozen=True)
class CachedProcessingResult:
    source_platform: str
    source_content_id: str
    source_content_type: str
    normalized_url: str
    processing_version: str
    ingestion_method: str
    transcript_source: str
    transcript: str
    caption: str
    extracted: ExtractedData


def build_processing_cache_payload(
    *,
    source_platform: str,
    source_content_id: str,
    source_content_type: str,
    normalized_url: str,
    processing_version: str,
    ingestion_method: str,
    transcript_source: str,
    transcript: str,
    caption: str,
    extracted: ExtractedData,
) -> dict:
    return {
        "source_platform": source_platform,
        "source_content_id": source_content_id,
        "source_content_type": source_content_type,
        "normalized_url": normalized_url,
        "processing_version": processing_version,
        "ingestion_method": ingestion_method,
        "transcript_source": transcript_source,
        "transcript": transcript,
        "caption": caption,
        "extracted_data": serialize_extracted_data(extracted),
    }


def cache_record_to_result(record: dict | None) -> CachedProcessingResult | None:
    if not record:
        return None

    source_platform = str(record.get("source_platform") or "").strip()
    source_content_id = str(record.get("source_content_id") or "").strip()
    if not source_platform or not source_content_id:
        return None

    extracted = deserialize_extracted_data(record.get("extracted_data") or {})
    return CachedProcessingResult(
        source_platform=source_platform,
        source_content_id=source_content_id,
        source_content_type=str(record.get("source_content_type") or "").strip(),
        normalized_url=str(record.get("normalized_url") or "").strip(),
        processing_version=str(record.get("processing_version") or "").strip(),
        ingestion_method=str(record.get("ingestion_method") or "").strip(),
        transcript_source=str(record.get("transcript_source") or "").strip(),
        transcript=str(record.get("transcript") or ""),
        caption=str(record.get("caption") or ""),
        extracted=extracted,
    )


def serialize_extracted_data(extracted: ExtractedData) -> dict:
    return {
        "title": extracted.title,
        "summary": extracted.summary,
        "category": extracted.category,
        "subcategory": extracted.subcategory,
        "secondary_categories": list(extracted.secondary_categories),
        "key_facts": list(extracted.key_facts),
        "locations": [location.model_dump() for location in extracted.locations],
        "people_mentioned": list(extracted.people_mentioned),
        "actionable_items": list(extracted.actionable_items),
    }


def deserialize_extracted_data(payload: dict) -> ExtractedData:
    if not isinstance(payload, dict):
        payload = {}

    locations: list[Location] = []
    raw_locations = payload.get("locations", [])
    if isinstance(raw_locations, list):
        for item in raw_locations:
            if not isinstance(item, dict):
                continue
            try:
                locations.append(Location(**item))
            except Exception:
                continue

    return ExtractedData(
        title=str(payload.get("title") or ""),
        summary=str(payload.get("summary") or ""),
        category=str(payload.get("category") or "Other"),
        subcategory=str(payload.get("subcategory") or "Other"),
        secondary_categories=_string_list(payload.get("secondary_categories")),
        key_facts=_string_list(payload.get("key_facts")),
        locations=locations,
        people_mentioned=_string_list(payload.get("people_mentioned")),
        actionable_items=_string_list(payload.get("actionable_items")),
    )


def _string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []

    return [
        str(value).strip()
        for value in values
        if value is not None and str(value).strip()
    ]
