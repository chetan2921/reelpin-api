import psycopg2
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:xovrip-sowzuv-4vixBe@db.rdzykswefpgeolbamend.supabase.co:6543/postgres"

# Mapping from specific old category to the new broad category
CATEGORY_MAPPING = {
    "Food": {"category": "Entertainment & Lifestyle", "subcategory": "Food & Restaurants"},
    "Fitness": {"category": "Entertainment & Lifestyle", "subcategory": "Fitness & Gym"},
    "Travel": {"category": "Entertainment & Lifestyle", "subcategory": "Travel & Places"}
}

def migrate_categories():
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        
        # We need to find rows where `category` is a key in our MAPPING, which means it wasn't migrated
        cur.execute("SELECT id, category, subcategory FROM reels;")
        reels = cur.fetchall()
        
        migrated_count = 0
        logger.info(f"Total reels scanned: {len(reels)}")
        
        for row in reels:
            reel_id, old_category, current_subcategory = row
            
            # If the current category matches one of the specific subcategories
            # it means this reel is using the old format schema.
            if old_category in CATEGORY_MAPPING:
                new_category = CATEGORY_MAPPING[old_category]["category"]
                new_subcategory = CATEGORY_MAPPING[old_category]["subcategory"]
                
                cur.execute(
                    "UPDATE reels SET category = %s, subcategory = %s WHERE id = %s",
                    (new_category, new_subcategory, reel_id)
                )
                migrated_count += 1
                logger.info(f"Migrated [{reel_id}]: {old_category} => {new_category} / {new_subcategory}")
        
        conn.commit()
        logger.info(f"Migration successful! {migrated_count} reels updated.")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Migration failed: {e}")

if __name__ == "__main__":
    logger.info("Starting Category Migration...")
    migrate_categories()
