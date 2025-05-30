"""
Fixed YouTube Data Collector for Music Prediction Platform
Includes proper error handling and environment variable support
"""

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import sqlite3
from dataclasses import dataclass
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class YouTubeVideoData:
    """Data class for YouTube video information"""

    video_id: str
    title: str
    channel_title: str
    published_at: str
    view_count: int
    like_count: int
    comment_count: int
    duration: str
    tags: List[str]
    category_id: str
    thumbnail_url: str


class YouTubeDataCollector:
    """Collects music-related data from YouTube Data API v3"""

    def __init__(self, api_key: str = None, db_path: str = None):
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        self.db_path = db_path or "music_data.db"
        self.base_url = "https://www.googleapis.com/youtube/v3"

        if not self.api_key:
            raise ValueError(
                "YouTube API key is required. Set YOUTUBE_API_KEY environment variable."
            )

        self.setup_database()

        # Rate limiting
        self.rate_limit = int(os.getenv("YOUTUBE_RATE_LIMIT", 60))
        self.last_request_time = 0

    def _rate_limit_delay(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 60 / self.rate_limit  # seconds between requests

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def setup_database(self):
        """Setup database tables for YouTube data with proper schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Updated schema to match cleaning pipeline expectations
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
                tags TEXT, -- JSON string of tags
                category_id TEXT,
                thumbnail_url TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

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

        # Add indexes for better performance
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_video_id ON youtube_videos(video_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_view_count ON youtube_videos(view_count)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_trending_date ON youtube_trending(trending_date)"
        )

        conn.commit()
        conn.close()

    def search_music_videos(
        self, query: str = "", max_results: int = 50, published_after: str = None
    ) -> List[str]:
        """Search for music videos on YouTube with proper error handling"""
        url = f"{self.base_url}/search"

        params = {
            "part": "snippet",
            "maxResults": min(max_results, 50),  # YouTube API limit
            "type": "video",
            "videoCategoryId": "10",  # Music category
            "key": self.api_key,
            "order": "relevance",
        }

        if query:
            params["q"] = query
        else:
            params["q"] = "music video 2024 2025"
            params["order"] = "viewCount"

        if published_after:
            params["publishedAfter"] = published_after

        try:
            self._rate_limit_delay()
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Check for API errors
            if "error" in data:
                logger.error(f"YouTube API error: {data['error']}")
                return []

            video_ids = []
            for item in data.get("items", []):
                if "videoId" in item.get("id", {}):
                    video_ids.append(item["id"]["videoId"])

            logger.info(f"Found {len(video_ids)} music videos for query: '{query}'")
            return video_ids

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error searching YouTube videos: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error searching YouTube videos: {e}")
            return []

    def get_video_details(self, video_ids: List[str]) -> List[YouTubeVideoData]:
        """Get detailed information for video IDs with improved error handling"""
        if not video_ids:
            return []

        video_data = []

        # Process in batches of 50 (YouTube API limit)
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i : i + 50]

            url = f"{self.base_url}/videos"
            params = {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(batch_ids),
                "key": self.api_key,
            }

            try:
                self._rate_limit_delay()
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()

                if "error" in data:
                    logger.error(
                        f"YouTube API error in batch {i//50 + 1}: {data['error']}"
                    )
                    continue

                for item in data.get("items", []):
                    try:
                        snippet = item.get("snippet", {})
                        statistics = item.get("statistics", {})
                        content_details = item.get("contentDetails", {})

                        # Safe integer conversion with defaults
                        view_count = self._safe_int(statistics.get("viewCount", 0))
                        like_count = self._safe_int(statistics.get("likeCount", 0))
                        comment_count = self._safe_int(
                            statistics.get("commentCount", 0)
                        )

                        video_info = YouTubeVideoData(
                            video_id=item["id"],
                            title=snippet.get("title", ""),
                            channel_title=snippet.get("channelTitle", ""),
                            published_at=snippet.get("publishedAt", ""),
                            view_count=view_count,
                            like_count=like_count,
                            comment_count=comment_count,
                            duration=content_details.get("duration", ""),
                            tags=snippet.get("tags", []),
                            category_id=snippet.get("categoryId", ""),
                            thumbnail_url=snippet.get("thumbnails", {})
                            .get("high", {})
                            .get("url", ""),
                        )
                        video_data.append(video_info)

                    except Exception as e:
                        logger.warning(
                            f"Error processing video {item.get('id', 'unknown')}: {e}"
                        )
                        continue

            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Network error getting video details for batch {i//50 + 1}: {e}"
                )
                continue
            except Exception as e:
                logger.error(
                    f"Unexpected error getting video details for batch {i//50 + 1}: {e}"
                )
                continue

        logger.info(f"Retrieved details for {len(video_data)} videos")
        return video_data

    def _safe_int(self, value, default=0):
        """Safely convert value to integer"""
        try:
            return int(value) if value else default
        except (ValueError, TypeError):
            return default

    def get_trending_music_videos(
        self, region_code: str = "US", max_results: int = 50
    ) -> List[str]:
        """Get trending music videos with improved error handling"""
        url = f"{self.base_url}/videos"
        params = {
            "part": "snippet,statistics",
            "chart": "mostPopular",
            "regionCode": region_code,
            "videoCategoryId": "10",  # Music category
            "maxResults": min(max_results, 50),
            "key": self.api_key,
        }

        try:
            self._rate_limit_delay()
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if "error" in data:
                logger.error(
                    f"YouTube API error for region {region_code}: {data['error']}"
                )
                return []

            video_ids = []
            trending_date = datetime.now().strftime("%Y-%m-%d")

            for i, item in enumerate(data.get("items", []), 1):
                video_id = item["id"]
                video_ids.append(video_id)

                # Store trending position
                try:
                    self.save_trending_position(
                        video_id, i, trending_date, region_code, "music"
                    )
                except Exception as e:
                    logger.warning(
                        f"Error saving trending position for {video_id}: {e}"
                    )

            logger.info(
                f"Retrieved {len(video_ids)} trending music videos for {region_code}"
            )
            return video_ids

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Network error getting trending videos for {region_code}: {e}"
            )
            return []
        except Exception as e:
            logger.error(
                f"Unexpected error getting trending videos for {region_code}: {e}"
            )
            return []

    def save_video_data(self, video_data_list: List[YouTubeVideoData]):
        """Save video data to database with transaction safety"""
        if not video_data_list:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for video in video_data_list:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO youtube_videos 
                    (video_id, title, channel_title, published_at, view_count, 
                     like_count, comment_count, duration, tags, category_id, thumbnail_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        video.video_id,
                        video.title,
                        video.channel_title,
                        video.published_at,
                        video.view_count,
                        video.like_count,
                        video.comment_count,
                        video.duration,
                        json.dumps(video.tags),
                        video.category_id,
                        video.thumbnail_url,
                    ),
                )

            conn.commit()
            logger.info(f"Saved {len(video_data_list)} videos to database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving video data: {e}")
            raise
        finally:
            conn.close()

    def save_trending_position(
        self,
        video_id: str,
        position: int,
        trending_date: str,
        region_code: str,
        category: str,
    ):
        """Save trending position data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO youtube_trending 
                (video_id, position, trending_date, region_code, category)
                VALUES (?, ?, ?, ?, ?)
            """,
                (video_id, position, trending_date, region_code, category),
            )

            conn.commit()
        except Exception as e:
            logger.error(f"Error saving trending position: {e}")
        finally:
            conn.close()

    def collect_trending_music_data(self, regions: List[str] = None):
        """Collect trending music data from multiple regions"""
        if not regions:
            regions = ["US", "GB", "CA", "AU"]  # Reduced for testing

        all_video_ids = set()

        for region in regions:
            logger.info(f"Collecting trending music for {region}")
            try:
                video_ids = self.get_trending_music_videos(region, max_results=25)
                all_video_ids.update(video_ids)
            except Exception as e:
                logger.error(f"Error collecting from region {region}: {e}")
                continue

        # Get detailed data for all unique videos
        if all_video_ids:
            video_data = self.get_video_details(list(all_video_ids))
            if video_data:
                self.save_video_data(video_data)
            return video_data

        return []

    def get_summary_stats(self) -> pd.DataFrame:
        """Get summary statistics of collected YouTube data"""
        conn = sqlite3.connect(self.db_path)

        try:
            query = """
                SELECT 
                    COUNT(*) as total_videos,
                    COALESCE(AVG(view_count), 0) as avg_views,
                    COALESCE(MAX(view_count), 0) as max_views,
                    COALESCE(AVG(like_count), 0) as avg_likes,
                    COUNT(DISTINCT channel_title) as unique_channels,
                    MIN(published_at) as earliest_video,
                    MAX(published_at) as latest_video
                FROM youtube_videos
            """

            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            logger.error(f"Error getting summary stats: {e}")
            return pd.DataFrame()
        finally:
            conn.close()


# Test function
def test_youtube_collector():
    """Test the YouTube collector"""
    try:
        collector = YouTubeDataCollector()

        # Test search
        print("Testing YouTube search...")
        video_ids = collector.search_music_videos("pop music 2024", max_results=5)
        print(f"Found {len(video_ids)} videos")

        if video_ids:
            # Test video details
            print("Getting video details...")
            video_data = collector.get_video_details(video_ids[:3])
            print(f"Retrieved details for {len(video_data)} videos")

            if video_data:
                # Save data
                collector.save_video_data(video_data)

                # Show summary
                summary = collector.get_summary_stats()
                print("\nYouTube Data Summary:")
                print(summary)

                return True

        return False

    except Exception as e:
        print(f"Test failed: {e}")
        return False


if __name__ == "__main__":
    # Check if API key is set
    if not os.getenv("YOUTUBE_API_KEY"):
        print("❌ YOUTUBE_API_KEY not found in environment variables")
        print("\nSetup Instructions:")
        print("1. Create .env file in project root")
        print("2. Add: YOUTUBE_API_KEY=your_api_key_here")
        print("3. Get API key from: https://console.cloud.google.com/")
    else:
        print("✅ YouTube API key found")
        success = test_youtube_collector()
        if success:
            print("✅ YouTube collector test passed")
        else:
            print("❌ YouTube collector test failed")
