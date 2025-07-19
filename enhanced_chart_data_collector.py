"""
Fixed Enhanced Chart Data Collector
Addresses iTunes URL and MusicBrainz access issues
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


class EnhancedChartDataCollector:
    """Fixed enhanced collector with working APIs and proper rate limiting"""

    def __init__(self, db_path: str = "music_data.db"):
        self.db_path = db_path
        self.setup_database()

        # Add headers for better API compatibility
        self.headers = {
            "User-Agent": "Music-Research-Platform/1.0 (Educational Purpose)",
            "Accept": "application/json",
        }

    def setup_database(self):
        """Setup SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

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

        # Add indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chart_track_artist ON chart_data(track_name, artist_name)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_chart_name_date ON chart_data(chart_name, chart_date)"
        )

        conn.commit()
        conn.close()

    def collect_itunes_charts_fixed(
        self, countries: List[str] = None, limit: int = 100
    ) -> List[ChartEntry]:
        """
        Fixed iTunes collection with working limits and proper error handling
        """
        if not countries:
            countries = ["us", "gb", "ca", "au", "de", "fr", "jp", "br"]

        all_entries = []
        # Use max 100 to avoid server errors
        safe_limit = min(limit, 100)

        for country in countries:
            try:
                # Use the working URL format
                url = f"https://rss.applemarketingtools.com/api/v2/{country}/music/most-played/{safe_limit}/songs.json"

                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()

                chart_entries = []
                chart_date = datetime.now().strftime("%Y-%m-%d")

                for i, entry in enumerate(data["feed"]["results"], 1):
                    chart_entry = ChartEntry(
                        position=i,
                        track_name=entry["name"],
                        artist_name=entry["artistName"],
                        chart_name=f"iTunes Top {safe_limit} {country.upper()}",
                        chart_date=chart_date,
                        additional_data={
                            "genre": entry.get("genres", [{}])[0].get("name", ""),
                            "release_date": entry.get("releaseDate", ""),
                            "itunes_url": entry.get("url", ""),
                            "country": country,
                            "collection_price": entry.get("collectionPrice", ""),
                        },
                    )
                    chart_entries.append(chart_entry)

                all_entries.extend(chart_entries)
                logger.info(
                    f"✅ Collected {len(chart_entries)} entries from iTunes {country.upper()}"
                )

                # Be respectful to APIs
                time.sleep(1)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 500:
                    logger.warning(
                        f"⚠️ iTunes server error for {country} with limit {safe_limit}, trying smaller limit..."
                    )
                    # Try with smaller limit
                    try:
                        smaller_limit = 50
                        url = f"https://rss.applemarketingtools.com/api/v2/{country}/music/most-played/{smaller_limit}/songs.json"
                        response = requests.get(url, headers=self.headers, timeout=30)
                        response.raise_for_status()
                        data = response.json()

                        chart_entries = []
                        for i, entry in enumerate(data["feed"]["results"], 1):
                            chart_entry = ChartEntry(
                                position=i,
                                track_name=entry["name"],
                                artist_name=entry["artistName"],
                                chart_name=f"iTunes Top {smaller_limit} {country.upper()}",
                                chart_date=chart_date,
                                additional_data={
                                    "genre": entry.get("genres", [{}])[0].get(
                                        "name", ""
                                    ),
                                    "release_date": entry.get("releaseDate", ""),
                                    "country": country,
                                },
                            )
                            chart_entries.append(chart_entry)

                        all_entries.extend(chart_entries)
                        logger.info(
                            f"✅ Collected {len(chart_entries)} entries from iTunes {country.upper()} (reduced limit)"
                        )

                    except Exception as e2:
                        logger.error(f"❌ Failed iTunes collection for {country}: {e2}")
                else:
                    logger.error(f"❌ iTunes HTTP error for {country}: {e}")

            except Exception as e:
                logger.error(f"❌ iTunes error for {country}: {e}")
                continue

        return all_entries

    def collect_lastfm_comprehensive(
        self, api_key: str, limit: int = 200
    ) -> List[ChartEntry]:
        """
        Enhanced Last.fm collection with better error handling
        """
        all_entries = []
        periods = ["7day", "1month", "3month", "6month", "12month"]
        countries = ["United States", "United Kingdom", "Germany", "France"]

        # Global charts
        for period in periods:
            try:
                url = "http://ws.audioscrobbler.com/2.0/"
                params = {
                    "method": "chart.gettoptracks",
                    "api_key": api_key,
                    "period": period,
                    "limit": min(limit, 200),  # Last.fm safe limit
                    "format": "json",
                }

                response = requests.get(
                    url, params=params, headers=self.headers, timeout=30
                )
                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    logger.error(
                        f"Last.fm API error for {period}: {data.get('message', 'Unknown error')}"
                    )
                    continue

                chart_entries = []
                chart_date = datetime.now().strftime("%Y-%m-%d")

                if "tracks" in data and "track" in data["tracks"]:
                    for entry in data["tracks"]["track"]:
                        chart_entry = ChartEntry(
                            position=int(entry.get("@attr", {}).get("rank", 0)),
                            track_name=entry["name"],
                            artist_name=entry["artist"]["name"],
                            chart_name=f"Last.fm Global Top {limit} ({period})",
                            chart_date=chart_date,
                            additional_data={
                                "playcount": entry.get("playcount", "0"),
                                "listeners": entry.get("listeners", "0"),
                                "period": period,
                                "region": "global",
                            },
                        )
                        chart_entries.append(chart_entry)

                all_entries.extend(chart_entries)
                logger.info(
                    f"✅ Collected {len(chart_entries)} from Last.fm Global ({period})"
                )

                # Rate limiting
                time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Last.fm Global error for {period}: {e}")
                continue

        # Country-specific charts
        for country in countries:
            try:
                url = "http://ws.audioscrobbler.com/2.0/"
                params = {
                    "method": "geo.gettoptracks",
                    "api_key": api_key,
                    "country": country,
                    "limit": 50,  # Smaller limit for country charts
                    "format": "json",
                }

                response = requests.get(
                    url, params=params, headers=self.headers, timeout=30
                )
                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    logger.error(
                        f"Last.fm API error for {country}: {data.get('message', 'Unknown error')}"
                    )
                    continue

                chart_entries = []
                chart_date = datetime.now().strftime("%Y-%m-%d")

                if "tracks" in data and "track" in data["tracks"]:
                    for i, entry in enumerate(data["tracks"]["track"], 1):
                        chart_entry = ChartEntry(
                            position=i,
                            track_name=entry["name"],
                            artist_name=entry["artist"]["name"],
                            chart_name=f"Last.fm {country} Top 50",
                            chart_date=chart_date,
                            additional_data={
                                "playcount": entry.get("playcount", "0"),
                                "listeners": entry.get("listeners", "0"),
                                "region": country.lower().replace(" ", "_"),
                            },
                        )
                        chart_entries.append(chart_entry)

                all_entries.extend(chart_entries)
                logger.info(f"✅ Collected {len(chart_entries)} from Last.fm {country}")

                # Rate limiting
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"❌ Last.fm error for {country}: {e}")
                continue

        return all_entries

    def collect_musicbrainz_simple(self, limit: int = 50) -> List[ChartEntry]:
        """
        Simplified MusicBrainz collection to avoid 403 errors
        """
        all_entries = []

        # Use simpler queries that are less likely to be blocked
        simple_queries = [
            "status:official AND date:[2024-01-01 TO *]",
            "status:official AND date:[2023-01-01 TO 2023-12-31]",
        ]

        for i, query in enumerate(simple_queries):
            try:
                url = "https://musicbrainz.org/ws/2/release"
                params = {
                    "query": query,
                    "limit": min(limit, 25),  # Much smaller limit
                    "fmt": "json",
                    "inc": "artist-credits",  # Reduced includes
                }

                # Add proper headers for MusicBrainz
                mb_headers = {
                    "User-Agent": "MusicResearchPlatform/1.0 ( contact@example.com )",
                    "Accept": "application/json",
                }

                response = requests.get(
                    url, params=params, headers=mb_headers, timeout=30
                )

                # Check for rate limiting
                if response.status_code == 503:
                    logger.warning("MusicBrainz rate limited, waiting longer...")
                    time.sleep(5)
                    continue

                response.raise_for_status()
                data = response.json()

                chart_entries = []
                chart_date = datetime.now().strftime("%Y-%m-%d")

                for j, release in enumerate(data.get("releases", []), 1):
                    if release.get("artist-credit"):
                        artist_name = release["artist-credit"][0]["artist"]["name"]

                        chart_entry = ChartEntry(
                            position=j,
                            track_name=release["title"],
                            artist_name=artist_name,
                            chart_name=f"MusicBrainz Recent Releases ({2024 if '2024' in query else 2023})",
                            chart_date=chart_date,
                            additional_data={
                                "release_date": release.get("date", ""),
                                "musicbrainz_id": release.get("id", ""),
                                "status": release.get("status", ""),
                            },
                        )
                        chart_entries.append(chart_entry)

                all_entries.extend(chart_entries)
                logger.info(
                    f"✅ Collected {len(chart_entries)} from MusicBrainz (query {i+1})"
                )

                # Longer rate limiting for MusicBrainz
                time.sleep(2)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    logger.warning(
                        f"⚠️ MusicBrainz access forbidden - skipping remaining queries"
                    )
                    break
                else:
                    logger.error(f"❌ MusicBrainz HTTP error: {e}")

            except Exception as e:
                logger.error(f"❌ MusicBrainz error for query {i+1}: {e}")
                continue

        return all_entries

    def save_chart_data(self, chart_entries: List[ChartEntry]):
        """Save chart entries with duplicate detection"""
        if not chart_entries:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        saved_count = 0
        duplicate_count = 0

        try:
            for entry in chart_entries:
                # Check for duplicates
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM chart_data 
                    WHERE track_name = ? AND artist_name = ? AND chart_name = ? AND chart_date = ?
                """,
                    (
                        entry.track_name,
                        entry.artist_name,
                        entry.chart_name,
                        entry.chart_date,
                    ),
                )

                if cursor.fetchone()[0] == 0:
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
                    saved_count += 1
                else:
                    duplicate_count += 1

            conn.commit()
            logger.info(
                f"💾 Saved {saved_count} new entries ({duplicate_count} duplicates skipped)"
            )

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error saving chart data: {e}")
            raise
        finally:
            conn.close()

    def collect_all_charts_enhanced(
        self,
        lastfm_api_key: str = None,
        itunes_limit: int = 100,
        lastfm_limit: int = 200,
        musicbrainz_limit: int = 25,
    ) -> List[ChartEntry]:
        """
        Fixed enhanced collection with working APIs
        """
        logger.info("🚀 Starting FIXED enhanced chart data collection...")
        all_entries = []

        # 1. iTunes charts (fixed limits)
        logger.info("🍎 Collecting iTunes charts (fixed)...")
        itunes_entries = self.collect_itunes_charts_fixed(limit=itunes_limit)
        all_entries.extend(itunes_entries)

        # 2. Last.fm comprehensive collection
        if lastfm_api_key:
            logger.info("📻 Collecting Last.fm charts...")
            lastfm_entries = self.collect_lastfm_comprehensive(
                lastfm_api_key, lastfm_limit
            )
            all_entries.extend(lastfm_entries)
        else:
            logger.warning("⚠️ No Last.fm API key - skipping Last.fm")

        # 3. MusicBrainz simple collection
        logger.info("🗃️ Collecting MusicBrainz data (simplified)...")
        musicbrainz_entries = self.collect_musicbrainz_simple(musicbrainz_limit)
        all_entries.extend(musicbrainz_entries)

        # 4. Save all data
        if all_entries:
            self.save_chart_data(all_entries)
            logger.info(
                f"✅ FIXED collection complete: {len(all_entries)} total entries"
            )
        else:
            logger.warning("⚠️ No chart data collected")

        return all_entries

    def get_collection_summary(self) -> pd.DataFrame:
        """Get summary of collected data"""
        conn = sqlite3.connect(self.db_path)

        query = """
            SELECT 
                chart_name,
                COUNT(*) as entry_count,
                MIN(position) as top_position,
                MAX(position) as bottom_position,
                MAX(chart_date) as latest_date
            FROM chart_data 
            GROUP BY chart_name
            ORDER BY entry_count DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df


if __name__ == "__main__":
    print("🔧 Fixed Enhanced Chart Data Collector")
    print("=" * 50)
    print("Fixes applied:")
    print("✅ iTunes: Reduced limits to avoid 500 errors")
    print("✅ MusicBrainz: Simplified queries to avoid 403 errors")
    print("✅ Last.fm: Enhanced error handling")
    print("✅ Better rate limiting and headers")
