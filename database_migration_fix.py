"""
Database Migration Script to Fix YouTube Schema Issues
Run this to update your existing database for enhanced collectors
"""

import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate_youtube_schema(db_path: str = "music_data.db"):
    """Migrate existing YouTube schema to support enhanced features"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if channel_id column exists
        cursor.execute("PRAGMA table_info(youtube_videos)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if "channel_id" not in columns:
            logger.info("Adding channel_id column to youtube_videos table...")
            cursor.execute("ALTER TABLE youtube_videos ADD COLUMN channel_id TEXT")
        
        if "subscriber_count" not in columns:
            logger.info("Adding subscriber_count column to youtube_videos table...")
            cursor.execute("ALTER TABLE youtube_videos ADD COLUMN subscriber_count INTEGER DEFAULT 0")
        
        if "description" not in columns:
            logger.info("Adding description column to youtube_videos table...")
            cursor.execute("ALTER TABLE youtube_videos ADD COLUMN description TEXT")
        
        # Create additional tables if they don't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_search_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,
                search_query TEXT,
                position INTEGER,
                relevance_score REAL,
                search_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES youtube_videos (video_id)
            )
        """)
        
        # Add missing indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_channel_id ON youtube_videos(channel_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_published_at ON youtube_videos(published_at)")
        
        conn.commit()
        logger.info("✅ Database migration completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Database migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def fix_itunes_url_issue():
    """Fix the iTunes API URL issue"""
    logger.info("🔧 iTunes URL Issue Fix:")
    logger.info("The iTunes API is experiencing server errors with 200-limit requests.")
    logger.info("Solution: Use the original working URL format with limit ≤ 100")
    logger.info("Updated URL: https://rss.applemarketingtools.com/api/v2/{country}/music/most-played/{limit}/songs.json")


def check_musicbrainz_access():
    """Check MusicBrainz access issues"""
    logger.info("🔧 MusicBrainz 403 Error Fix:")
    logger.info("MusicBrainz is blocking requests. Solutions:")
    logger.info("1. Add User-Agent header to requests")
    logger.info("2. Reduce request frequency")
    logger.info("3. Use simpler queries")
    logger.info("4. Consider using only Last.fm and iTunes for now")


if __name__ == "__main__":
    print("🔧 Database Migration and Fixes")
    print("=" * 50)
    
    # Run database migration
    migrate_youtube_schema()
    
    # Show fix information
    fix_itunes_url_issue()
    check_musicbrainz_access()
    
    print("\n✅ Migration complete! Enhanced YouTube collector should now work.")
