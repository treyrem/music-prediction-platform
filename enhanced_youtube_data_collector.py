"""
Enhanced YouTube Data Collector with Optimized Quota Usage
Maximizes data collection within API limits
"""

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import sqlite3
from dataclasses import dataclass
import re
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class YouTubeVideoData:
    """Enhanced data class for YouTube video information"""
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
    description: str = ""
    channel_id: str = ""
    subscriber_count: int = 0


class EnhancedYouTubeDataCollector:
    """Enhanced YouTube collector with optimized quota usage and higher limits"""

    def __init__(self, api_key: str = None, db_path: str = None):
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        self.db_path = db_path or "music_data.db"
        self.base_url = "https://www.googleapis.com/youtube/v3"
        
        if not self.api_key:
            raise ValueError("YouTube API key is required")

        self.setup_database()
        
        # Enhanced quota management
        self.daily_quota = int(os.getenv("YOUTUBE_DAILY_QUOTA", 10000))
        self.used_quota = 0
        self.quota_costs = {
            "search": 100,
            "videos": 1,
            "channels": 1,
            "playlists": 1,
        }
        
        # Rate limiting
        self.rate_limit = int(os.getenv("YOUTUBE_RATE_LIMIT", 60))
        self.last_request_time = 0

    def _check_quota(self, operation: str, count: int = 1) -> bool:
        """Check if we have enough quota for the operation"""
        cost = self.quota_costs.get(operation, 1) * count
        if self.used_quota + cost > self.daily_quota:
            logger.warning(f"Quota limit reached. Used: {self.used_quota}, Needed: {cost}")
            return False
        return True

    def _use_quota(self, operation: str, count: int = 1):
        """Track quota usage"""
        cost = self.quota_costs.get(operation, 1) * count
        self.used_quota += cost
        logger.info(f"Used {cost} quota units for {operation}. Total used: {self.used_quota}/{self.daily_quota}")

    def setup_database(self):
        """Enhanced database setup with additional fields"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS youtube_videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                channel_title TEXT,
                channel_id TEXT,
                published_at TEXT,
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                subscriber_count INTEGER DEFAULT 0,
                duration TEXT,
                description TEXT,
                tags TEXT,
                category_id TEXT,
                thumbnail_url TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
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
        """)

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

        # Add indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_video_id ON youtube_videos(video_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_view_count ON youtube_videos(view_count)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_channel_id ON youtube_videos(channel_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_published_at ON youtube_videos(published_at)")

        conn.commit()
        conn.close()

    def get_trending_videos_comprehensive(self, regions: List[str] = None, max_per_region: int = 50) -> List[str]:
        """
        Get trending videos from multiple regions with higher limits
        Cost: 1 unit per video retrieved
        """
        if not regions:
            regions = ["US", "GB", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "MX", "KR", "ES"]
        
        all_video_ids = set()
        
        for region in regions:
            if not self._check_quota("videos", max_per_region):
                logger.warning(f"Quota exhausted, stopping at region {region}")
                break
                
            url = f"{self.base_url}/videos"
            params = {
                "part": "snippet,statistics",
                "chart": "mostPopular",
                "regionCode": region,
                "videoCategoryId": "10",  # Music category
                "maxResults": min(max_per_region, 50),
                "key": self.api_key,
            }

            try:
                self._rate_limit_delay()
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    logger.error(f"API error for region {region}: {data['error']}")
                    continue

                video_ids = []
                trending_date = datetime.now().strftime("%Y-%m-%d")

                for i, item in enumerate(data.get("items", []), 1):
                    video_id = item["id"]
                    video_ids.append(video_id)
                    all_video_ids.add(video_id)

                    # Save trending position
                    self.save_trending_position(video_id, i, trending_date, region, "music")

                self._use_quota("videos", len(video_ids))
                logger.info(f"Collected {len(video_ids)} trending videos from {region}")

            except Exception as e:
                logger.error(f"Error collecting trending videos for {region}: {e}")
                continue

        return list(all_video_ids)

    def search_music_videos_comprehensive(self, queries: List[str] = None, max_per_query: int = 50) -> List[str]:
        """
        Comprehensive music video search with multiple queries
        Cost: 100 units per search + 1 unit per video retrieved
        """
        if not queries:
            queries = [
                "new music 2025",
                "trending music",
                "pop music 2025",
                "hip hop 2025",
                "rock music 2025",
                "electronic music 2025",
                "country music 2025",
                "r&b music 2025",
                "indie music 2025",
                "latin music 2025"
            ]
        
        all_video_ids = set()
        
        for query in queries:
            if not self._check_quota("search", 1):
                logger.warning(f"Quota exhausted, stopping at query: {query}")
                break
                
            url = f"{self.base_url}/search"
            params = {
                "part": "snippet",
                "maxResults": min(max_per_query, 50),
                "type": "video",
                "videoCategoryId": "10",
                "q": query,
                "order": "relevance",
                "publishedAfter": (datetime.now() - timedelta(days=30)).isoformat() + "Z",
                "key": self.api_key,
            }

            try:
                self._rate_limit_delay()
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    logger.error(f"API error for query '{query}': {data['error']}")
                    continue

                video_ids = []
                search_date = datetime.now().strftime("%Y-%m-%d")

                for i, item in enumerate(data.get("items", []), 1):
                    if "videoId" in item.get("id", {}):
                        video_id = item["id"]["videoId"]
                        video_ids.append(video_id)
                        all_video_ids.add(video_id)
                        
                        # Save search result
                        self.save_search_result(video_id, query, i, search_date)

                self._use_quota("search", 1)
                logger.info(f"Found {len(video_ids)} videos for query: '{query}'")

            except Exception as e:
                logger.error(f"Error searching for '{query}': {e}")
                continue

        return list(all_video_ids)

    def get_video_details_enhanced(self, video_ids: List[str]) -> List[YouTubeVideoData]:
        """
        Get enhanced video details including channel information
        Cost: 1 unit per video + 1 unit per unique channel
        """
        if not video_ids:
            return []

        video_data = []
        channel_cache = {}  # Cache channel data to avoid duplicate requests

        # Process in batches of 50 (YouTube API limit)
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i + 50]
            
            if not self._check_quota("videos", len(batch_ids)):
                logger.warning(f"Quota exhausted, stopping at batch {i//50 + 1}")
                break

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
                    logger.error(f"API error in batch {i//50 + 1}: {data['error']}")
                    continue

                for item in data.get("items", []):
                    try:
                        snippet = item.get("snippet", {})
                        statistics = item.get("statistics", {})
                        content_details = item.get("contentDetails", {})
                        channel_id = snippet.get("channelId", "")

                        # Get channel subscriber count if not cached
                        subscriber_count = 0
                        if channel_id and channel_id not in channel_cache:
                            subscriber_count = self.get_channel_subscriber_count(channel_id)
                            channel_cache[channel_id] = subscriber_count
                        elif channel_id in channel_cache:
                            subscriber_count = channel_cache[channel_id]

                        video_info = YouTubeVideoData(
                            video_id=item["id"],
                            title=snippet.get("title", ""),
                            channel_title=snippet.get("channelTitle", ""),
                            channel_id=channel_id,
                            published_at=snippet.get("publishedAt", ""),
                            view_count=self._safe_int(statistics.get("viewCount", 0)),
                            like_count=self._safe_int(statistics.get("likeCount", 0)),
                            comment_count=self._safe_int(statistics.get("commentCount", 0)),
                            subscriber_count=subscriber_count,
                            duration=content_details.get("duration", ""),
                            description=snippet.get("description", "")[:500],  # Limit description length
                            tags=snippet.get("tags", []),
                            category_id=snippet.get("categoryId", ""),
                            thumbnail_url=snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                        )
                        video_data.append(video_info)

                    except Exception as e:
                        logger.warning(f"Error processing video {item.get('id', 'unknown')}: {e}")
                        continue

                self._use_quota("videos", len(batch_ids))

            except Exception as e:
                logger.error(f"Error getting video details for batch {i//50 + 1}: {e}")
                continue

        logger.info(f"Retrieved enhanced details for {len(video_data)} videos")
        return video_data

    def get_channel_subscriber_count(self, channel_id: str) -> int:
        """Get subscriber count for a channel"""
        if not self._check_quota("channels", 1):
            return 0

        url = f"{self.base_url}/channels"
        params = {
            "part": "statistics",
            "id": channel_id,
            "key": self.api_key,
        }

        try:
            self._rate_limit_delay()
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "items" in data and data["items"]:
                subscriber_count = data["items"][0].get("statistics", {}).get("subscriberCount", "0")
                self._use_quota("channels", 1)
                return self._safe_int(subscriber_count)

        except Exception as e:
            logger.warning(f"Error getting subscriber count for channel {channel_id}: {e}")

        return 0

    def save_search_result(self, video_id: str, query: str, position: int, search_date: str):
        """Save search result data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO youtube_search_results 
                (video_id, search_query, position, search_date)
                VALUES (?, ?, ?, ?)
            """, (video_id, query, position, search_date))
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving search result: {e}")
        finally:
            conn.close()

    def save_trending_position(self, video_id: str, position: int, trending_date: str, 
                             region_code: str, category: str):
        """Save trending position data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO youtube_trending 
                (video_id, position, trending_date, region_code, category)
                VALUES (?, ?, ?, ?, ?)
            """, (video_id, position, trending_date, region_code, category))
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving trending position: {e}")
        finally:
            conn.close()

    def save_video_data_enhanced(self, video_data_list: List[YouTubeVideoData]):
        """Save enhanced video data to database"""
        if not video_data_list:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for video in video_data_list:
                cursor.execute("""
                    INSERT OR REPLACE INTO youtube_videos 
                    (video_id, title, channel_title, channel_id, published_at, view_count, 
                     like_count, comment_count, subscriber_count, duration, description,
                     tags, category_id, thumbnail_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    video.video_id, video.title, video.channel_title, video.channel_id,
                    video.published_at, video.view_count, video.like_count, video.comment_count,
                    video.subscriber_count, video.duration, video.description,
                    json.dumps(video.tags), video.category_id, video.thumbnail_url,
                ))

            conn.commit()
            logger.info(f"Saved {len(video_data_list)} enhanced videos to database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving enhanced video data: {e}")
            raise
        finally:
            conn.close()

    def collect_comprehensive_music_data(self, 
                                       trending_regions: List[str] = None,
                                       search_queries: List[str] = None,
                                       max_trending_per_region: int = 50,
                                       max_search_per_query: int = 50) -> List[YouTubeVideoData]:
        """
        Comprehensive data collection optimized for quota usage
        """
        logger.info("🚀 Starting comprehensive YouTube music data collection...")
        
        all_video_ids = set()
        
        # 1. Collect trending videos (efficient: 1 unit per video)
        logger.info("📈 Collecting trending music videos...")
        trending_ids = self.get_trending_videos_comprehensive(
            trending_regions, max_trending_per_region
        )
        all_video_ids.update(trending_ids)
        
        # 2. Search for specific music (expensive: 100 units per search)
        logger.info("🔍 Searching for specific music videos...")
        search_ids = self.search_music_videos_comprehensive(
            search_queries, max_search_per_query
        )
        all_video_ids.update(search_ids)
        
        # 3. Get detailed information for all unique videos
        logger.info(f"📊 Getting detailed info for {len(all_video_ids)} unique videos...")
        video_data = self.get_video_details_enhanced(list(all_video_ids))
        
        # 4. Save all data
        if video_data:
            self.save_video_data_enhanced(video_data)
        
        logger.info(f"✅ Collection complete! Quota used: {self.used_quota}/{self.daily_quota}")
        return video_data

    def _safe_int(self, value, default=0):
        """Safely convert value to integer"""
        try:
            return int(value) if value else default
        except (ValueError, TypeError):
            return default

    def _rate_limit_delay(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 60 / self.rate_limit

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def get_quota_efficiency_report(self) -> Dict:
        """Generate a report on quota usage efficiency"""
        return {
            "total_quota": self.daily_quota,
            "used_quota": self.used_quota,
            "remaining_quota": self.daily_quota - self.used_quota,
            "efficiency": (self.used_quota / self.daily_quota) * 100,
            "estimated_videos_possible": (self.daily_quota - self.used_quota) // 1,  # Assuming 1 unit per video detail
            "estimated_searches_possible": (self.daily_quota - self.used_quota) // 100,  # 100 units per search
        }


# Usage example and quota calculations
def calculate_collection_potential():
    """Calculate potential data collection with different quota limits"""
    
    scenarios = {
        "Default (10k)": 10000,
        "Small Extension (100k)": 100000,
        "Medium Extension (500k)": 500000,
        "Large Extension (1M)": 1000000,
    }
    
    print("YouTube Data Collection Potential Analysis")
    print("=" * 60)
    
    for scenario_name, quota in scenarios.items():
        print(f"\n{scenario_name} Quota:")
        
        # Trending videos (1 unit each)
        trending_videos = min(quota // 2, 12 * 50)  # 12 regions × 50 videos max
        remaining_quota = quota - trending_videos
        
        # Search operations (100 units each)
        searches = min(remaining_quota // 100, 20)  # Max 20 different searches
        search_quota_used = searches * 100
        remaining_quota -= search_quota_used
        
        # Search results (assume 50 videos per search, 1 unit each for details)
        search_videos = searches * 50
        video_details_quota = min(remaining_quota, search_videos + trending_videos)
        
        total_unique_videos = min(trending_videos + search_videos, video_details_quota)
        
        print(f"  • Trending videos: {trending_videos:,}")
        print(f"  • Search queries: {searches}")
        print(f"  • Search results: {search_videos:,}")
        print(f"  • Total unique videos: {total_unique_videos:,}")
        print(f"  • Quota utilization: {((trending_videos + search_quota_used + total_unique_videos) / quota * 100):.1f}%")


if __name__ == "__main__":
    # Show collection potential
    calculate_collection_potential()
    
    # Test enhanced collector (uncomment to run)
    # collector = EnhancedYouTubeDataCollector()
    # videos = collector.collect_comprehensive_music_data(
    #     max_trending_per_region=25,
    #     max_search_per_query=25
    # )