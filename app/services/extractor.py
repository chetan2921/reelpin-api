import json
import logging
import urllib.request
import urllib.parse
import time
from groq import Groq
from app.config import get_settings
from app.models import ExtractedData, Location

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """You are an AI that analyzes Instagram reel content. Given the transcript (and optional caption), extract structured information.

TRANSCRIPT:
{transcript}

CAPTION:
{caption}

TAXONOMY (6 Categories and their associated Subcategories):
- Entertainment & Lifestyle: Food & Restaurants, Travel & Places, Fitness & Gym, Fashion & Style, Beauty & Skincare, Home Decor & Interior, Relationships & Dating, Motivation & Mindset, Humor & Memes, Spirituality & Religion
- Knowledge & Learning: Study & Education, Science & Technology, History & Culture, Language Learning, Books & Reading, General Knowledge & Facts
- Finance & Career: Stock Market & Trading, Personal Finance & Investing, Business & Startups, Career & Jobs, Crypto & Web3, Real Estate
- Health & Wellness: Mental Health, Nutrition & Diet, Yoga & Meditation, Medical & Health Tips, Parenting & Kids
- Skills & Hobbies: Cooking & Recipes, Music & Dance, Art & Drawing, Photography & Videography, DIY & Crafts, Gaming, Sports & Cricket, Gardening & Plants, Pets & Animals
- Practical & Utility: Life Hacks & Tips, Tech & Gadgets, Shopping & Products, Legal & Rights, Government & Schemes, Automotive & Cars

Extract the following as a JSON object:
{{
    "title": "A concise, descriptive title for this reel (max 10 words)",
    "summary": "A 2-3 sentence summary of what this reel is about",
    "category": "Exactly ONE primary category string from the 6 broad sections (e.g., 'Entertainment & Lifestyle')",
    "subcategory": "Exactly ONE specific subcategory string from the matched section (e.g., 'Food & Restaurants')",
    "secondary_categories": ["Up to 2 secondary subcategories from the taxonomy list above that also apply. Empty array if none."],
    "key_facts": ["List of specific facts, tips, or pieces of information mentioned"],
    "locations": [
        {{
            "name": "Name of the place (restaurant, cafe, landmark, attraction, etc.)",
            "neighborhood": "Neighborhood or specific local area",
            "city": "City name",
            "state": "State or province",
            "country": "Country name"
        }}
    ],
    "people_mentioned": ["Names of people, creators, or brands mentioned"],
    "actionable_items": ["Things the viewer might want to do based on this reel"]
}}

Rules:
- Extract EVERY distinct location mention in the reel. If five different cafes, landmarks, stores, or places are mentioned, return all five.
- Never merge multiple places into one item.
- If no locations are mentioned, return an empty locations array
- Never use null for location string fields. Use an empty string or omit the field instead.
- If no people are mentioned, return an empty array
- Be specific with facts — don't be vague
- For locations, ALWAYS include the city and country even if not explicitly stated
- If a business, neighborhood, mall, landmark, street market, gym, hotel, or venue is mentioned and it is likely to exist on Google Maps, include it.
- Correct any obvious phonetic spelling mistakes in city or neighborhood names.
- Provide EXACT matches for Category and Subcategory strings from the taxonomy list. Nothing else.
- Return ONLY the JSON object, no other text
"""


def get_groq_client() -> Groq:
    settings = get_settings()
    return Groq(api_key=settings.GROQ_API_KEY)


def geocode_location(location: Location) -> tuple[float | None, float | None]:
    """
    Geocode a location using Google Maps Geocoding API.

    Args:
        location: Location object with hierarchical fields

    Returns:
        Tuple of (latitude, longitude) or (None, None) if geocoding fails
    """
    settings = get_settings()
    api_key = settings.GOOGLE_MAPS_API_KEY
    if not api_key or api_key == "your_google_maps_api_key_here":
        logger.warning("GOOGLE_MAPS_API_KEY is not configured properly.")
        return None, None

    # Try highly specific query first
    query_parts = [p for p in [location.name, location.neighborhood, location.city, location.state, location.country] if p]
    search_query = ", ".join(query_parts)

    def _call_gmaps(query_string: str) -> tuple[float | None, float | None]:
        try:
            params = urllib.parse.urlencode({
                "address": query_string,
                "key": api_key,
            })
            url = f"https://maps.googleapis.com/maps/api/geocode/json?{params}"

            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())

            if data.get("status") == "OK" and data.get("results"):
                location_data = data["results"][0]["geometry"]["location"]
                lat = float(location_data["lat"])
                lon = float(location_data["lng"])
                logger.info(f"Geocoded '{query_string}' → ({lat}, {lon})")
                return lat, lon
            else:
                logger.warning(f"Google Maps Geocoding returned {data.get('status')} for: '{query_string}'")
                return None, None
        except Exception as e:
            logger.warning(f"Google Maps geocoding failed for '{query_string}': {e}")
            return None, None

    # First attempt: highly specific
    lat, lon = _call_gmaps(search_query)
    
    # Fallback attempt if specific query fails and we have a neighborhood to strip out
    if lat is None and lon is None and location.neighborhood:
        fallback_parts = [p for p in [location.name, location.city, location.state, location.country] if p]
        fallback_query = ", ".join(fallback_parts)
        if fallback_query != search_query:
            logger.info(f"Retrying geocoding with fallback query: '{fallback_query}'")
            lat, lon = _call_gmaps(fallback_query)

    # Final fallback: try the place name alone when the transcript has weak context
    if lat is None and lon is None and location.name:
        simple_query = location.name.strip()
        if simple_query and simple_query not in {search_query, location.address}:
            logger.info(f"Retrying geocoding with name-only query: '{simple_query}'")
            lat, lon = _call_gmaps(simple_query)

    return lat, lon


def geocode_locations(locations: list[Location]) -> list[Location]:
    """
    Enrich a list of Location objects with lat/lng coordinates via geocoding.
    """
    seen = set()
    enriched = []
    for loc in locations:
        dedupe_key = (
            (loc.name or "").strip().lower(),
            (loc.address or "").strip().lower(),
            (loc.city or "").strip().lower(),
            (loc.state or "").strip().lower(),
            (loc.country or "").strip().lower(),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        if loc.latitude is not None and loc.longitude is not None:
            enriched.append(loc)
            continue

        lat, lon = geocode_location(loc)
        loc.latitude = lat
        loc.longitude = lon
        enriched.append(loc)

    return enriched


def extract_structured_data(
    transcript: str, caption: str | None = None
) -> ExtractedData:
    """
    Use Groq LLaMA to extract structured data from a reel transcript,
    then geocode any extracted locations to get lat/lng coordinates.
    """
    settings = get_settings()
    client = get_groq_client()

    prompt = EXTRACTION_PROMPT.format(
        transcript=transcript or "(no audio/transcript available)",
        caption=caption or "(no caption provided)",
    )

    try:
        logger.info("Extracting structured data with LLM...")

        response = client.chat.completions.create(
            model=settings.LLM_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise data extraction assistant. Always respond with valid JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )

        raw_response = response.choices[0].message.content
        logger.info(f"LLM raw response: {raw_response[:200]}...")

        # Parse the JSON response
        data = json.loads(raw_response)

        def _clean_text(value) -> str | None:
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip()
                return cleaned or None
            cleaned = str(value).strip()
            return cleaned or None

        # Build Location objects from parsed JSON
        locations = []
        raw_locations = data.get("locations", [])
        if not isinstance(raw_locations, list):
            raw_locations = []

        for loc_data in raw_locations:
            if not isinstance(loc_data, dict):
                continue

            name = _clean_text(loc_data.get("name"))
            neighborhood = _clean_text(loc_data.get("neighborhood"))
            city = _clean_text(loc_data.get("city"))
            state = _clean_text(loc_data.get("state"))
            country = _clean_text(loc_data.get("country"))

            # Skip malformed location entries that have no usable place text.
            if not any([name, neighborhood, city, state, country]):
                continue

            # If the model omitted the name but still returned a place hierarchy,
            # use the most specific available component so processing never fails.
            resolved_name = name or neighborhood or city or state or country or "Unknown place"
            
            # Reconstruct the legacy 'address' field for Flutter compatibility
            address_parts = [p for p in [neighborhood, city, state, country] if p]
            legacy_address = ", ".join(address_parts) if address_parts else None
            
            locations.append(
                Location(
                    name=resolved_name,
                    address=legacy_address,
                    neighborhood=neighborhood,
                    city=city,
                    state=state,
                    country=country,
                    latitude=loc_data.get("latitude"),
                    longitude=loc_data.get("longitude"),
                )
            )

        # --- GEOCODING STEP ---
        # Resolve lat/lng for any locations that don't have coordinates
        if locations:
            logger.info(f"Geocoding {len(locations)} location(s)...")
            locations = geocode_locations(locations)
            geocoded_count = sum(
                1 for loc in locations
                if loc.latitude is not None and loc.longitude is not None
            )
            logger.info(
                f"Geocoding complete: {geocoded_count}/{len(locations)} locations resolved"
            )

        extracted = ExtractedData(
            title=data.get("title", "Untitled Reel"),
            summary=data.get("summary", ""),
            category=data.get("category", "Other"),
            subcategory=data.get("subcategory", "Other"),
            secondary_categories=data.get("secondary_categories", []),
            key_facts=data.get("key_facts", []),
            locations=locations,
            people_mentioned=data.get("people_mentioned", []),
            actionable_items=data.get("actionable_items", []),
        )

        logger.info(
            f"Extracted: title='{extracted.title}', "
            f"category={extracted.category}, "
            f"{len(extracted.locations)} locations, "
            f"{len(extracted.key_facts)} facts"
        )
        return extracted

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}")
        # Return a minimal extraction rather than failing
        return ExtractedData(
            title="Untitled Reel",
            summary=transcript[:200] if transcript else "",
            category="Other",
            subcategory="Other",
        )
    except Exception as e:
        logger.error(f"Extraction error: {e}")
        raise Exception(f"Failed to extract data: {str(e)}")
