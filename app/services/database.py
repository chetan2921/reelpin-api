import logging
from supabase import create_client, Client
from app.config import get_settings

logger = logging.getLogger(__name__)

_supabase_client: Client | None = None

TABLE_NAME = "reels"


def _get_client() -> Client:
    """Get or create the Supabase client."""
    global _supabase_client
    if _supabase_client is None:
        settings = get_settings()
        _supabase_client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        logger.info("Supabase client initialized")
    return _supabase_client


# ----- SQL to run in Supabase SQL Editor -----
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS reels (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id TEXT NOT NULL DEFAULT 'default-user',
    url TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT 'Untitled',
    summary TEXT DEFAULT '',
    transcript TEXT DEFAULT '',
    category TEXT DEFAULT 'Other',
    subcategory TEXT DEFAULT 'Other',
    secondary_categories JSONB DEFAULT '[]'::jsonb,
    key_facts JSONB DEFAULT '[]'::jsonb,
    locations JSONB DEFAULT '[]'::jsonb,
    people_mentioned JSONB DEFAULT '[]'::jsonb,
    actionable_items JSONB DEFAULT '[]'::jsonb,
    pinecone_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast user lookups
CREATE INDEX IF NOT EXISTS idx_reels_user_id ON reels(user_id);

-- Index for category filtering
CREATE INDEX IF NOT EXISTS idx_reels_category ON reels(category);

-- Index for subcategory filtering
CREATE INDEX IF NOT EXISTS idx_reels_subcategory ON reels(subcategory);
"""


def save_reel(reel_data: dict) -> dict:
    """
    Insert a new reel record into Supabase.

    Args:
        reel_data: Dict with reel fields matching the table schema

    Returns:
        The inserted record as a dict
    """
    client = _get_client()
    try:
        # Convert Location objects to dicts if needed
        if "locations" in reel_data and reel_data["locations"]:
            reel_data["locations"] = [
                loc if isinstance(loc, dict) else loc.model_dump()
                for loc in reel_data["locations"]
            ]

        result = client.table(TABLE_NAME).insert(reel_data).execute()
        record = result.data[0]
        logger.info(f"Saved reel: {record['id']}")
        return record
    except Exception as e:
        logger.error(f"Failed to save reel: {e}")
        raise


def get_reel(reel_id: str) -> dict | None:
    """Fetch a single reel by ID."""
    client = _get_client()
    try:
        result = client.table(TABLE_NAME).select("*").eq("id", reel_id).execute()
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Failed to fetch reel {reel_id}: {e}")
        raise


def get_reels(
    user_id: str | None = None,
    category: str | None = None,
    subcategory: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    List reels with optional filters.

    Args:
        user_id: Filter by user
        category: Filter by category
        subcategory: Filter by subcategory
        limit: Max number of results

    Returns:
        List of reel dicts
    """
    client = _get_client()
    try:
        query = client.table(TABLE_NAME).select("*")

        if user_id:
            query = query.eq("user_id", user_id)
        if category:
            query = query.eq("category", category)
        if subcategory:
            query = query.eq("subcategory", subcategory)

        query = query.order("created_at", desc=True).limit(limit)
        result = query.execute()
        return result.data
    except Exception as e:
        logger.error(f"Failed to list reels: {e}")
        raise


def delete_reel(reel_id: str) -> bool:
    """Delete a reel by ID. Returns True if deleted."""
    client = _get_client()
    try:
        result = client.table(TABLE_NAME).delete().eq("id", reel_id).execute()
        deleted = len(result.data) > 0
        if deleted:
            logger.info(f"Deleted reel: {reel_id}")
        return deleted
    except Exception as e:
        logger.error(f"Failed to delete reel {reel_id}: {e}")
        raise


def get_reels_by_ids(reel_ids: list[str]) -> list[dict]:
    """Fetch multiple reels by their IDs."""
    client = _get_client()
    try:
        result = client.table(TABLE_NAME).select("*").in_("id", reel_ids).execute()
        return result.data
    except Exception as e:
        logger.error(f"Failed to fetch reels by IDs: {e}")
        raise
