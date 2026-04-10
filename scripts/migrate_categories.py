import logging
import sys
from pathlib import Path
from supabase import create_client

# Ensure project root is importable when running `python scripts/...`
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TABLE_NAME = "reels"

# Mapping from specific old category to the new broad category
CATEGORY_MAPPING = {
    "Food": {"category": "Entertainment & Lifestyle", "subcategory": "Food & Restaurants"},
    "Fitness": {"category": "Entertainment & Lifestyle", "subcategory": "Fitness & Gym"},
    "Travel": {"category": "Entertainment & Lifestyle", "subcategory": "Travel & Places"}
}

def migrate_categories():
    try:
        settings = get_settings()
        client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

        # Fetch relevant columns for all reels
        result = client.table(TABLE_NAME).select("id, category, subcategory").limit(10000).execute()
        reels = result.data or []
        
        migrated_count = 0
        logger.info(f"Total reels scanned: {len(reels)}")
        
        for row in reels:
            reel_id = row.get("id")
            old_category = row.get("category")
            current_subcategory = row.get("subcategory")
            
            # If the current category matches one of the specific subcategories
            # it means this reel is using the old format schema.
            if old_category in CATEGORY_MAPPING:
                new_category = CATEGORY_MAPPING[old_category]["category"]
                new_subcategory = CATEGORY_MAPPING[old_category]["subcategory"]

                client.table(TABLE_NAME).update(
                    {
                        "category": new_category,
                        "subcategory": new_subcategory,
                    }
                ).eq("id", reel_id).execute()

                migrated_count += 1
                logger.info(
                    f"Migrated [{reel_id}]: {old_category} => {new_category} / {new_subcategory}"
                )

        logger.info(f"Migration successful! {migrated_count} reels updated.")
    except Exception as e:
        logger.error(f"Migration failed: {e}")

if __name__ == "__main__":
    logger.info("Starting Category Migration...")
    migrate_categories()
