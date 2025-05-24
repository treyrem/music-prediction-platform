"""
YouTube Data Collector for Music Prediction Platform
Collects music video data from YouTube API
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import sqlite3
from dataclasses import dataclass
import re

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

    def __init__(self, api_key: str, db_path: str = "music_data.db"):
        self.api_key = api_key
        self.db_path = db_path
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.setup_database()

    def setup_database(self):
        """Setup database tables for YouTube data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS youtube_videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                channel_title TEXT,
                published_at TEXT,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER,
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

        conn.commit()
        conn.close()

    def search_music_videos(
        self, query: str = "", max_results: int = 50, published_after: str = None
    ) -> List[str]:
        """
        Search for music videos on YouTube
        Returns list of video IDs
        """
        url = f"{self.base_url}/search"

        params = {
            "part": "snippet",
            "maxResults": max_results,
            "type": "video",
            "videoCategoryId": "10",  # Music category
            "key": self.api_key,
            "order": "relevance",
        }

        if query:
            params["q"] = query
        else:
            # Search for trending music terms
            params["q"] = "music video 2024 2025"
            params["order"] = "viewCount"

        if published_after:
            params["publishedAfter"] = published_after

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            video_ids = []
            for item in data.get("items", []):
                video_ids.append(item["id"]["videoId"])

            logger.info(f"Found {len(video_ids)} music videos for query: '{query}'")
            return video_ids

        except Exception as e:
            logger.error(f"Error searching YouTube videos: {e}")
            return []

    def get_video_details(self, video_ids: List[str]) -> List[YouTubeVideoData]:
        """
        Get detailed information for a list of video IDs
        YouTube API allows up to 50 video IDs per request
        """
        video_data = []

        # Process in batches of 50
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i : i + 50]

            url = f"{self.base_url}/videos"
            params = {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(batch_ids),
                "key": self.api_key,
            }

            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                for item in data.get("items", []):
                    snippet = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    content_details = item.get("contentDetails", {})

                    video_info = YouTubeVideoData(
                        video_id=item["id"],
                        title=snippet.get("title", ""),
                        channel_title=snippet.get("channelTitle", ""),
                        published_at=snippet.get("publishedAt", ""),
                        view_count=int(statistics.get("viewCount", 0)),
                        like_count=int(statistics.get("likeCount", 0)),
                        comment_count=int(statistics.get("commentCount", 0)),
                        duration=content_details.get("duration", ""),
                        tags=snippet.get("tags", []),
                        category_id=snippet.get("categoryId", ""),
                        thumbnail_url=snippet.get("thumbnails", {})
                        .get("high", {})
                        .get("url", ""),
                    )
                    video_data.append(video_info)

                time.sleep(0.1)  # Rate limiting

            except Exception as e:
                logger.error(f"Error getting video details: {e}")
                continue

        logger.info(f"Retrieved details for {len(video_data)} videos")
        return video_data

    def get_trending_music_videos(
        self, region_code: str = "US", max_results: int = 50
    ) -> List[str]:
        """
        Get trending music videos from YouTube
        """
        url = f"{self.base_url}/videos"
        params = {
            "part": "snippet,statistics",
            "chart": "mostPopular",
            "regionCode": region_code,
            "videoCategoryId": "10",  # Music category
            "maxResults": max_results,
            "key": self.api_key,
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            video_ids = []
            trending_date = datetime.now().strftime("%Y-%m-%d")

            for i, item in enumerate(data.get("items", []), 1):
                video_id = item["id"]
                video_ids.append(video_id)

                # Store trending position
                self.save_trending_position(
                    video_id, i, trending_date, region_code, "music"
                )

            logger.info(
                f"Retrieved {len(video_ids)} trending music videos for {region_code}"
            )
            return video_ids

        except Exception as e:
            logger.error(f"Error getting trending videos: {e}")
            return []

    def save_video_data(self, video_data_list: List[YouTubeVideoData]):
        """Save video data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

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
        conn.close()
        logger.info(f"Saved {len(video_data_list)} videos to database")

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

        cursor.execute(
            """
            INSERT INTO youtube_trending 
            (video_id, position, trending_date, region_code, category)
            VALUES (?, ?, ?, ?, ?)
        """,
            (video_id, position, trending_date, region_code, category),
        )

        conn.commit()
        conn.close()

    def extract_artist_track_from_title(self, title: str) -> tuple:
        """
        Extract artist and track name from YouTube video title
        This is heuristic-based and may not be 100% accurate
        """
        # Common patterns in music video titles
        patterns = [
            r"^(.+?)\s*[-–—]\s*(.+?)(?:\s*\(.*\))?(?:\s*\[.*\])?$",  # Artist - Track
            r'^(.+?)\s*[""]\s*(.+?)\s*[""]\s*',  # Artist "Track"
            r"^(.+?)\s*:\s*(.+?)(?:\s*\(.*\))?$",  # Artist: Track
        ]

        title = title.strip()

        for pattern in patterns:
            match = re.match(pattern, title, re.IGNORECASE)
            if match:
                artist = match.group(1).strip()
                track = match.group(2).strip()

                # Filter out common video-specific terms
                exclude_terms = ["official", "video", "music", "mv", "clip", "hd", "4k"]

                if not any(term in artist.lower() for term in exclude_terms):
                    return artist, track

        # Fallback: assume channel name is artist if available
        return None, title

    def collect_trending_music_data(self, regions: List[str] = None):
        """
        Collect trending music data from multiple regions
        """
        if not regions:
            regions = ["US", "GB", "CA", "AU", "DE", "FR"]

        all_video_ids = set()

        for region in regions:
            logger.info(f"Collecting trending music for {region}")
            video_ids = self.get_trending_music_videos(region, max_results=25)
            all_video_ids.update(video_ids)
            time.sleep(1)  # Rate limiting

        # Get detailed data for all unique videos
        if all_video_ids:
            video_data = self.get_video_details(list(all_video_ids))
            self.save_video_data(video_data)

            return video_data

        return []

    def search_specific_songs(self, song_list: List[Dict[str, str]]):
        """
        Search for specific songs on YouTube
        song_list: [{'artist': 'Artist Name', 'track': 'Track Name'}, ...]
        """
        all_video_data = []

        for song in song_list:
            query = f"{song['artist']} {song['track']} official music video"
            logger.info(f"Searching for: {query}")

            video_ids = self.search_music_videos(query, max_results=5)

            if video_ids:
                video_data = self.get_video_details(video_ids)
                all_video_data.extend(video_data)

            time.sleep(1)  # Rate limiting

        if all_video_data:
            self.save_video_data(all_video_data)

        return all_video_data

    def get_summary_stats(self) -> pd.DataFrame:
        """Get summary statistics of collected YouTube data"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT 
                COUNT(*) as total_videos,
                AVG(view_count) as avg_views,
                MAX(view_count) as max_views,
                AVG(like_count) as avg_likes,
                COUNT(DISTINCT channel_title) as unique_channels,
                MIN(published_at) as earliest_video,
                MAX(published_at) as latest_video
            FROM youtube_videos
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df


# Example usage and setup guide
if __name__ == "__main__":
    # You need to set up YouTube Data API v3 key
    # 1. Go to Google Cloud Console: https://console.cloud.google.com/
    # 2. Create a new project or select existing
    # 3. Enable YouTube Data API v3
    # 4. Create credentials (API key)
    # 5. Set quota limits (default is 10,000 units/day)

    API_KEY = "YOUR_YOUTUBE_API_KEY_HERE"  # Replace with actual key

    if API_KEY != "YOUR_YOUTUBE_API_KEY_HERE":
        collector = YouTubeDataCollector(API_KEY)

        # Test trending collection
        print("Testing YouTube trending music collection...")
        trending_data = collector.collect_trending_music_data(["US"])

        if trending_data:
            print(f"Collected {len(trending_data)} trending videos")
            print(f"Sample video: {trending_data[0].title}")

        # Get summary
        summary = collector.get_summary_stats()
        print("\nYouTube Data Summary:")
        print(summary)
    else:
        print("Setup Instructions:")
        print("1. Get YouTube Data API v3 key from Google Cloud Console")
        print("2. Replace API_KEY variable with your actual key")
        print("3. Run the script to collect YouTube music data")
        print("\nAPI Setup Guide:")
        print("- Visit: https://console.cloud.google.com/")
        print("- Create project → Enable YouTube Data API v3 → Create API key")
        print("- Daily quota: 10,000 units (each video details request = ~4 units)")
