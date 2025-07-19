"""
Chart Data Collector for Music Prediction Platform
Collects data from free, accessible music charts
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
from dataclasses import dataclass
import sqlite3  # For local testing before PostgreSQL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ChartEntry:
    """Data class for chart entries"""

    position: int
    track_name: str
    artist_name: str
    chart_name: str
    chart_date: str
    additional_data: Optional[Dict] = None


class ChartDataCollector:
    """Collects chart data from various free sources"""

    def __init__(self, db_path: str = "music_data.db"):
        self.db_path = db_path
        self.setup_database()

    def setup_database(self):
        """Setup SQLite database for initial testing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create simplified tables for testing
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

        conn.commit()
        conn.close()

    def collect_itunes_top_charts(
        self, country_code: str = "us", limit: int = 100
    ) -> List[ChartEntry]:
        """
        Collect iTunes top songs chart data
        iTunes provides free RSS feeds for their charts
        """
        url = f"https://rss.applemarketingtools.com/api/v2/{country_code}/music/most-played/{limit}/songs.json"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            chart_entries = []
            chart_date = datetime.now().strftime("%Y-%m-%d")

            for i, entry in enumerate(data["feed"]["results"], 1):
                chart_entry = ChartEntry(
                    position=i,
                    track_name=entry["name"],
                    artist_name=entry["artistName"],
                    chart_name=f"iTunes Top {limit} {country_code.upper()}",
                    chart_date=chart_date,
                    additional_data={
                        "genre": entry.get("genres", [{}])[0].get("name", ""),
                        "release_date": entry.get("releaseDate", ""),
                        "itunes_url": entry.get("url", ""),
                    },
                )
                chart_entries.append(chart_entry)

            logger.info(
                f"Collected {len(chart_entries)} entries from iTunes {country_code.upper()} chart"
            )
            return chart_entries

        except Exception as e:
            logger.error(f"Error collecting iTunes chart data: {e}")
            return []

    def collect_lastfm_top_tracks(
        self, api_key: str, period: str = "7day", limit: int = 50
    ) -> List[ChartEntry]:
        """
        Collect Last.fm top tracks
        Requires free Last.fm API key: https://www.last.fm/api/account/create
        """
        url = "http://ws.audioscrobbler.com/2.0/"
        params = {
            "method": "chart.gettoptracks",
            "api_key": api_key,
            "period": period,
            "limit": limit,
            "format": "json",
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            chart_entries = []
            chart_date = datetime.now().strftime("%Y-%m-%d")

            if "tracks" in data and "track" in data["tracks"]:
                for entry in data["tracks"]["track"]:
                    chart_entry = ChartEntry(
                        position=int(entry.get("@attr", {}).get("rank", 0)),
                        track_name=entry["name"],
                        artist_name=entry["artist"]["name"],
                        chart_name=f"Last.fm Top {limit} ({period})",
                        chart_date=chart_date,
                        additional_data={
                            "playcount": entry.get("playcount", "0"),
                            "listeners": entry.get("listeners", "0"),
                            "lastfm_url": entry.get("url", ""),
                        },
                    )
                    chart_entries.append(chart_entry)

            logger.info(f"Collected {len(chart_entries)} entries from Last.fm chart")
            return chart_entries

        except Exception as e:
            logger.error(f"Error collecting Last.fm chart data: {e}")
            return []

    def collect_musicbrainz_popular_releases(self, limit: int = 25) -> List[ChartEntry]:
        """
        Collect popular releases from MusicBrainz (open source music database)
        This gives us recent popular releases rather than current charts
        """
        # MusicBrainz API to get recent releases with high rating
        url = "https://musicbrainz.org/ws/2/release"
        params = {
            "query": "status:official AND date:[2023-01-01 TO *]",
            "limit": limit,
            "fmt": "json",
            "inc": "artist-credits+recordings",
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            chart_entries = []
            chart_date = datetime.now().strftime("%Y-%m-%d")

            for i, release in enumerate(data.get("releases", []), 1):
                if release.get("artist-credit"):
                    artist_name = release["artist-credit"][0]["artist"]["name"]
                    chart_entry = ChartEntry(
                        position=i,
                        track_name=release["title"],
                        artist_name=artist_name,
                        chart_name="MusicBrainz Popular Releases",
                        chart_date=chart_date,
                        additional_data={
                            "release_date": release.get("date", ""),
                            "country": release.get("country", ""),
                            "musicbrainz_id": release.get("id", ""),
                        },
                    )
                    chart_entries.append(chart_entry)

            logger.info(f"Collected {len(chart_entries)} entries from MusicBrainz")
            return chart_entries

        except Exception as e:
            logger.error(f"Error collecting MusicBrainz data: {e}")
            return []

    def save_chart_data(self, chart_entries: List[ChartEntry]):
        """Save chart entries to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for entry in chart_entries:
            cursor.execute(
                """
                INSERT INTO chart_data 
                (track_name, artist_name, position, chart_name, chart_date, additional_info)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    entry.track_name,
                    entry.artist_name,
                    entry.position,
                    entry.chart_name,
                    entry.chart_date,
                    (
                        json.dumps(entry.additional_data)
                        if entry.additional_data
                        else None
                    ),
                ),
            )

        conn.commit()
        conn.close()
        logger.info(f"Saved {len(chart_entries)} chart entries to database")

    def collect_all_charts(self, lastfm_api_key: str = None):
        """Collect data from all available chart sources"""
        all_entries = []

        # iTunes charts (multiple countries)
        countries = ["us", "gb", "ca", "au"]
        for country in countries:
            entries = self.collect_itunes_top_charts(country, limit=200)
            all_entries.extend(entries)
            time.sleep(1)  # Be respectful to APIs

        # Last.fm (if API key provided)
        if lastfm_api_key:
            entries = self.collect_lastfm_top_tracks(
                lastfm_api_key, period="7day", limit=200
            )
            all_entries.extend(entries)
            time.sleep(1)

        # MusicBrainz
        entries = self.collect_musicbrainz_popular_releases(limit=25)
        all_entries.extend(entries)

        # Save all data
        if all_entries:
            self.save_chart_data(all_entries)
            logger.info(f"Total collected: {len(all_entries)} chart entries")

        return all_entries

    def get_chart_summary(self) -> pd.DataFrame:
        """Get summary of collected chart data"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT 
                chart_name,
                chart_date,
                COUNT(*) as entry_count,
                MIN(created_at) as first_collected,
                MAX(created_at) as last_collected
            FROM chart_data 
            GROUP BY chart_name, chart_date
            ORDER BY last_collected DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df


# Example usage and testing
if __name__ == "__main__":
    collector = ChartDataCollector()

    # Test iTunes collection
    print("Testing iTunes chart collection...")
    itunes_data = collector.collect_itunes_top_charts("us", limit=10)

    if itunes_data:
        print(f"Sample iTunes entry: {itunes_data[0]}")
        collector.save_chart_data(itunes_data)

    # Get summary
    summary = collector.get_chart_summary()
    print("\nChart Data Summary:")
    print(summary)

    print("\nChart data collection setup complete!")
    print("Next steps:")
    print("1. Get Last.fm API key from: https://www.last.fm/api/account/create")
    print("2. Run collector.collect_all_charts(lastfm_api_key='your_key')")
    print("3. Set up scheduled collection (daily/weekly)")
