"""
Database Fix Script for Music Prediction Platform
Diagnoses and fixes database connection issues
"""

import os
import sqlite3
import shutil
from pathlib import Path


def diagnose_database_issue():
    """Diagnose what's wrong with the database"""
    db_path = "music_data.db"

    print("🔍 Diagnosing database issue...")

    # Check if file exists
    if os.path.exists(db_path):
        print(f"✅ Database file exists: {db_path}")

        # Check file size
        file_size = os.path.getsize(db_path)
        print(f"📏 File size: {file_size} bytes")

        if file_size == 0:
            print("❌ Database file is empty (0 bytes)")
            return "empty_file"

        # Check if it's actually a SQLite database
        try:
            with open(db_path, "rb") as f:
                header = f.read(16)
                if header.startswith(b"SQLite format 3"):
                    print("✅ File has valid SQLite header")
                else:
                    print(f"❌ Invalid SQLite header: {header}")
                    return "invalid_format"
        except Exception as e:
            print(f"❌ Cannot read file: {e}")
            return "read_error"

        # Try to connect
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            conn.close()
            print(f"✅ Database connection successful, {len(tables)} tables found")
            return "working"
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return "connection_error"
    else:
        print(f"❌ Database file does not exist: {db_path}")
        return "missing_file"


def backup_corrupted_database():
    """Backup any existing corrupted database"""
    db_path = "music_data.db"

    if os.path.exists(db_path):
        backup_path = f"music_data_backup_{int(time.time())}.db"
        try:
            shutil.move(db_path, backup_path)
            print(f"📦 Moved corrupted database to: {backup_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to backup database: {e}")
            return False
    return True


def create_fresh_database():
    """Create a fresh database with proper schema"""
    db_path = "music_data.db"

    print("🗄️ Creating fresh database...")

    try:
        # Remove existing file if it exists
        if os.path.exists(db_path):
            os.remove(db_path)

        # Create new database connection
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create chart_data table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS chart_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_name TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                position INTEGER,
                chart_name TEXT,
                chart_date TEXT,
                additional_info TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create youtube_videos table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS youtube_videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                channel_title TEXT,
                published_at TEXT,
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                duration TEXT,
                tags TEXT,
                category_id TEXT,
                thumbnail_url TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Create youtube_trending table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS youtube_trending (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,
                position INTEGER,
                trending_date TEXT,
                region_code TEXT,
                category TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES youtube_videos (video_id)
            )
        """
        )

        # Create indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_video_id ON youtube_videos(video_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_view_count ON youtube_videos(view_count)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_trending_date ON youtube_trending(trending_date)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chart_date ON chart_data(chart_date)"
        )

        # Test the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        conn.commit()
        conn.close()

        print(f"✅ Database created successfully with {len(tables)} tables:")
        for table in tables:
            print(f"   - {table[0]}")

        return True

    except Exception as e:
        print(f"❌ Failed to create database: {e}")
        return False


def test_database_operations():
    """Test basic database operations"""
    db_path = "music_data.db"

    print("🧪 Testing database operations...")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Test insert
        cursor.execute(
            """
            INSERT INTO chart_data (track_name, artist_name, position, chart_name, chart_date)
            VALUES (?, ?, ?, ?, ?)
        """,
            ("Test Song", "Test Artist", 1, "Test Chart", "2024-01-01"),
        )

        # Test select
        cursor.execute("SELECT * FROM chart_data WHERE track_name = 'Test Song'")
        result = cursor.fetchone()

        if result:
            print("✅ Insert and select operations successful")

            # Clean up test data
            cursor.execute("DELETE FROM chart_data WHERE track_name = 'Test Song'")
            conn.commit()
            print("✅ Delete operation successful")
        else:
            print("❌ Test data not found after insert")
            return False

        conn.close()
        return True

    except Exception as e:
        print(f"❌ Database operations test failed: {e}")
        return False


def fix_database():
    """Main function to fix database issues"""
    import time

    print("🔧 Music Prediction Platform - Database Fix")
    print("=" * 50)

    # Step 1: Diagnose the issue
    issue = diagnose_database_issue()

    if issue == "working":
        print("✅ Database is working correctly!")
        return True

    # Step 2: Backup corrupted database if needed
    if issue in ["connection_error", "invalid_format", "empty_file"]:
        if not backup_corrupted_database():
            print("❌ Failed to backup corrupted database")
            return False

    # Step 3: Create fresh database
    if not create_fresh_database():
        print("❌ Failed to create fresh database")
        return False

    # Step 4: Test operations
    if not test_database_operations():
        print("❌ Database operations test failed")
        return False

    print("✅ Database fixed successfully!")
    return True


def test_chart_collector_with_new_db():
    """Test chart collector with the new database"""
    print("\n🎵 Testing chart collector with new database...")

    try:
        # Import here to avoid issues if modules aren't available
        import sys
        import os

        # Add current directory to Python path
        sys.path.insert(0, os.getcwd())

        from chart_data_collector import ChartDataCollector

        collector = ChartDataCollector()

        # Test with a small collection
        entries = collector.collect_itunes_top_charts("us", limit=3)

        if entries:
            print(f"✅ Chart collector test successful: {len(entries)} entries")

            # Save to database
            collector.save_chart_data(entries)
            print("✅ Data saved to database successfully")

            # Verify data was saved
            import sqlite3

            conn = sqlite3.connect("music_data.db")
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM chart_data")
            count = cursor.fetchone()[0]
            conn.close()

            print(f"✅ Verified: {count} records in database")
            return True
        else:
            print("❌ Chart collector returned no data")
            return False

    except Exception as e:
        print(f"❌ Chart collector test failed: {e}")
        return False


if __name__ == "__main__":
    success = fix_database()

    if success:
        # Test with actual collector
        test_success = test_chart_collector_with_new_db()

        if test_success:
            print("\n🎉 Database fix complete and tested successfully!")
            print("🚀 You can now run: python main_data_pipeline.py")
        else:
            print("\n⚠️ Database fixed but collector test failed")
            print("💡 Try running: python quick_test.py")
    else:
        print("\n❌ Database fix failed")
        print("💡 Try manually deleting music_data.db and running this script again")
