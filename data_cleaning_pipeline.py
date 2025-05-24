"""
Data Cleaning and Preprocessing Pipeline for Music Prediction Platform
Handles cleaning and standardization of data from multiple sources
"""

import pandas as pd
import numpy as np
import re
import sqlite3
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import difflib
from dataclasses import dataclass
import unicodedata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CleaningStats:
    """Statistics from data cleaning process"""

    records_processed: int
    duplicates_removed: int
    missing_values_filled: int
    standardized_names: int
    invalid_records_removed: int


class MusicDataCleaner:
    """Comprehensive data cleaning for music data from multiple sources"""

    def __init__(self, db_path: str = "music_data.db"):
        self.db_path = db_path

        # Common music terms to standardize
        self.title_replacements = {
            r"\(official\s+music\s+video\)": "",
            r"\(official\s+video\)": "",
            r"\(official\s+audio\)": "",
            r"\[official\s+music\s+video\]": "",
            r"\[official\s+video\]": "",
            r"\[official\s+audio\]": "",
            r"\(music\s+video\)": "",
            r"\(audio\s+only\)": "",
            r"\(lyric\s+video\)": "",
            r"\(lyrics\)": "",
            r"\(hd\)": "",
            r"\(4k\)": "",
            r"\(remastered\)": "",
            r"\s+\-\s+topic$": "",
            r"\s+\-\s+official$": "",
        }

        # Artist name variations to standardize
        self.artist_standardizations = {
            "ft.": "feat.",
            "ft ": "feat. ",
            "featuring": "feat.",
            " & ": " and ",
            " vs ": " vs. ",
            " x ": " feat. ",
        }

    def clean_text(self, text: str) -> str:
        """Clean and standardize text fields"""
        if not text or pd.isna(text):
            return ""

        # Convert to string and normalize unicode
        text = str(text)
        text = unicodedata.normalize("NFKD", text)

        # Remove extra whitespace
        text = " ".join(text.split())

        # Remove common encoding artifacts
        text = text.replace("â€™", "'").replace("â€œ", '"').replace("â€", '"')

        return text.strip()

    def clean_track_title(self, title: str) -> str:
        """Clean track titles by removing common video-specific terms"""
        if not title:
            return ""

        title = self.clean_text(title)
        title_lower = title.lower()

        # Apply regex replacements
        for pattern, replacement in self.title_replacements.items():
            title = re.sub(pattern, replacement, title, flags=re.IGNORECASE)

        # Remove bracketed content that's likely metadata
        title = re.sub(
            r"\[[^\]]*(?:remix|version|edit|mix)\]", "", title, flags=re.IGNORECASE
        )
        title = re.sub(
            r"\([^)]*(?:remix|version|edit|mix)\)", "", title, flags=re.IGNORECASE
        )

        # Clean up extra spaces and punctuation
        title = re.sub(r"\s+", " ", title)
        title = title.strip(" -–—")

        return title

    def clean_artist_name(self, artist: str) -> str:
        """Clean and standardize artist names"""
        if not artist:
            return ""

        artist = self.clean_text(artist)

        # Standardize common variations
        for old, new in self.artist_standardizations.items():
            artist = artist.replace(old, new)

        # Remove "- Topic" suffix from YouTube channels
        artist = re.sub(r"\s*-\s*Topic$", "", artist, flags=re.IGNORECASE)

        # Standardize featuring formats
        artist = re.sub(r"\bfeat\b\.?", "feat.", artist, flags=re.IGNORECASE)

        return artist.strip()

    def detect_duplicates(
        self, df: pd.DataFrame, similarity_threshold: float = 0.85
    ) -> pd.DataFrame:
        """
        Detect potential duplicates using fuzzy matching on track and artist names
        """
        duplicates = []

        for i, row1 in df.iterrows():
            for j, row2 in df.iterrows():
                if i >= j:  # Only check upper triangle
                    continue

                # Calculate similarity scores
                track_similarity = difflib.SequenceMatcher(
                    None,
                    row1["clean_track_name"].lower(),
                    row2["clean_track_name"].lower(),
                ).ratio()

                artist_similarity = difflib.SequenceMatcher(
                    None,
                    row1["clean_artist_name"].lower(),
                    row2["clean_artist_name"].lower(),
                ).ratio()

                # Consider duplicates if both track and artist are similar
                if (
                    track_similarity >= similarity_threshold
                    and artist_similarity >= similarity_threshold
                ):
                    duplicates.append(
                        {
                            "index1": i,
                            "index2": j,
                            "track1": row1["clean_track_name"],
                            "track2": row2["clean_track_name"],
                            "artist1": row1["clean_artist_name"],
                            "artist2": row2["clean_artist_name"],
                            "track_similarity": track_similarity,
                            "artist_similarity": artist_similarity,
                        }
                    )

        return pd.DataFrame(duplicates)

    def merge_duplicate_records(
        self, df: pd.DataFrame, duplicates_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge duplicate records, keeping the one with more complete data
        """
        indices_to_remove = set()

        for _, dup in duplicates_df.iterrows():
            idx1, idx2 = dup["index1"], dup["index2"]

            if idx1 in indices_to_remove or idx2 in indices_to_remove:
                continue

            row1, row2 = df.iloc[idx1], df.iloc[idx2]

            # Choose record with more non-null values
            score1 = row1.notna().sum()
            score2 = row2.notna().sum()

            # If YouTube data available, prefer that (more detailed)
            if hasattr(row1, "view_count") and pd.notna(row1["view_count"]):
                score1 += 10
            if hasattr(row2, "view_count") and pd.notna(row2["view_count"]):
                score2 += 10

            # Remove the record with lower score
            if score1 >= score2:
                indices_to_remove.add(idx2)
            else:
                indices_to_remove.add(idx1)

        # Remove duplicate indices
        cleaned_df = df.drop(index=list(indices_to_remove)).reset_index(drop=True)

        logger.info(f"Removed {len(indices_to_remove)} duplicate records")
        return cleaned_df

    def clean_chart_data(self) -> pd.DataFrame:
        """Clean chart data from database"""
        conn = sqlite3.connect(self.db_path)

        # Load chart data
        df = pd.read_sql_query(
            """
            SELECT * FROM chart_data
        """,
            conn,
        )

        conn.close()

        if df.empty:
            logger.warning("No chart data found in database")
            return df

        logger.info(f"Cleaning {len(df)} chart records")

        # Clean text fields
        df["clean_track_name"] = df["track_name"].apply(self.clean_track_title)
        df["clean_artist_name"] = df["artist_name"].apply(self.clean_artist_name)

        # Remove records with empty track or artist names
        initial_count = len(df)
        df = df[(df["clean_track_name"] != "") & (df["clean_artist_name"] != "")]
        removed_empty = initial_count - len(df)

        # Handle missing positions
        df["position"] = pd.to_numeric(df["position"], errors="coerce")

        # Parse additional info JSON
        def parse_additional_info(info_str):
            if pd.isna(info_str) or info_str == "":
                return {}
            try:
                return json.loads(info_str)
            except:
                return {}

        df["additional_info_parsed"] = df["additional_info"].apply(
            parse_additional_info
        )

        # Detect and handle duplicates
        duplicates = self.detect_duplicates(df)
        if not duplicates.empty:
            df = self.merge_duplicate_records(df, duplicates)

        logger.info(
            f"Chart data cleaning complete: {removed_empty} empty records removed"
        )
        return df

    def clean_youtube_data(self) -> pd.DataFrame:
        """Clean YouTube video data"""
        conn = sqlite3.connect(self.db_path)

        # Load YouTube data
        df = pd.read_sql_query(
            """
            SELECT * FROM youtube_videos
        """,
            conn,
        )

        conn.close()

        if df.empty:
            logger.warning("No YouTube data found in database")
            return df

        logger.info(f"Cleaning {len(df)} YouTube records")

        # Clean text fields
        df["clean_title"] = df["title"].apply(self.clean_track_title)
        df["clean_channel"] = df["channel_title"].apply(self.clean_artist_name)

        # Extract artist and track from title
        df[["extracted_artist", "extracted_track"]] = df["clean_title"].apply(
            lambda x: pd.Series(self.extract_artist_track_from_title(x))
        )

        # Use channel name as artist if extraction failed
        df["final_artist"] = df.apply(
            lambda row: (
                row["extracted_artist"]
                if row["extracted_artist"]
                else row["clean_channel"]
            ),
            axis=1,
        )

        df["final_track"] = df.apply(
            lambda row: (
                row["extracted_track"] if row["extracted_track"] else row["clean_title"]
            ),
            axis=1,
        )

        # Clean numeric fields
        numeric_cols = ["view_count", "like_count", "comment_count"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Parse duration (ISO 8601 format: PT4M13S)
        def parse_duration(duration_str):
            if not duration_str:
                return 0

            # Extract minutes and seconds
            match = re.match(r"PT(?:(\d+)M)?(?:(\d+)S)?", duration_str)
            if match:
                minutes = int(match.group(1) or 0)
                seconds = int(match.group(2) or 0)
                return minutes * 60 + seconds
            return 0

        df["duration_seconds"] = df["duration"].apply(parse_duration)

        # Parse tags
        def parse_tags(tags_str):
            if pd.isna(tags_str) or tags_str == "":
                return []
            try:
                return json.loads(tags_str)
            except:
                return []

        df["tags_list"] = df["tags"].apply(parse_tags)

        # Remove records with invalid data
        initial_count = len(df)
        df = df[(df["final_track"] != "") & (df["final_artist"] != "")]
        removed_invalid = initial_count - len(df)

        logger.info(
            f"YouTube data cleaning complete: {removed_invalid} invalid records removed"
        )
        return df

    def create_unified_dataset(self) -> pd.DataFrame:
        """
        Create a unified dataset combining chart and YouTube data
        """
        # Clean individual datasets
        chart_df = self.clean_chart_data()
        youtube_df = self.clean_youtube_data()

        unified_records = []

        # Process chart data
        for _, row in chart_df.iterrows():
            record = {
                "track_name": row["clean_track_name"],
                "artist_name": row["clean_artist_name"],
                "source": "chart",
                "chart_name": row.get("chart_name", ""),
                "chart_position": row.get("position", None),
                "chart_date": row.get("chart_date", ""),
                "view_count": None,
                "like_count": None,
                "duration_seconds": None,
                "original_title": row["track_name"],
                "original_artist": row["artist_name"],
            }
            unified_records.append(record)

        # Process YouTube data
        for _, row in youtube_df.iterrows():
            record = {
                "track_name": row["final_track"],
                "artist_name": row["final_artist"],
                "source": "youtube",
                "chart_name": None,
                "chart_position": None,
                "chart_date": None,
                "view_count": row["view_count"],
                "like_count": row["like_count"],
                "duration_seconds": row["duration_seconds"],
                "original_title": row["title"],
                "original_artist": row["channel_title"],
            }
            unified_records.append(record)

        unified_df = pd.DataFrame(unified_records)

        # Final deduplication across sources
        if not unified_df.empty:
            duplicates = self.detect_duplicates(
                unified_df.rename(
                    columns={
                        "track_name": "clean_track_name",
                        "artist_name": "clean_artist_name",
                    }
                )
            )

            if not duplicates.empty:
                # For cross-source duplicates, merge the data
                unified_df = self.merge_cross_source_duplicates(unified_df, duplicates)

        logger.info(f"Created unified dataset with {len(unified_df)} records")
        return unified_df

    def merge_cross_source_duplicates(
        self, df: pd.DataFrame, duplicates_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge duplicates across different data sources, combining their information
        """
        indices_to_remove = set()

        for _, dup in duplicates_df.iterrows():
            idx1, idx2 = dup["index1"], dup["index2"]

            if idx1 in indices_to_remove or idx2 in indices_to_remove:
                continue

            row1, row2 = df.iloc[idx1].copy(), df.iloc[idx2].copy()

            # Merge data from both sources
            merged_row = row1.copy()

            # Fill missing values from the other source
            for col in df.columns:
                if pd.isna(merged_row[col]) and pd.notna(row2[col]):
                    merged_row[col] = row2[col]

            # Update the first record with merged data
            df.iloc[idx1] = merged_row

            # Mark second record for removal
            indices_to_remove.add(idx2)

        # Remove duplicate indices
        cleaned_df = df.drop(index=list(indices_to_remove)).reset_index(drop=True)

        logger.info(f"Merged {len(indices_to_remove)} cross-source duplicates")
        return cleaned_df

    def extract_artist_track_from_title(self, title: str) -> Tuple[Optional[str], str]:
        """
        Extract artist and track name from title using various patterns
        """
        if not title:
            return None, ""

        # Common patterns in music titles
        patterns = [
            r"^(.+?)\s*[-–—]\s*(.+?)(?:\s*\(.*\))?(?:\s*\[.*\])?$",  # Artist - Track
            r'^(.+?)\s*[""]\s*(.+?)\s*[""]\s*',  # Artist "Track"
            r"^(.+?)\s*:\s*(.+?)(?:\s*\(.*\))?$",  # Artist: Track
            r"^(.+?)\s+by\s+(.+?)(?:\s*\(.*\))?$",  # Track by Artist
        ]

        for pattern in patterns:
            match = re.match(pattern, title, re.IGNORECASE)
            if match:
                artist = match.group(1).strip()
                track = match.group(2).strip()

                # Filter out common video-specific terms
                exclude_terms = [
                    "official",
                    "music",
                    "video",
                    "audio",
                    "lyrics",
                    "hd",
                    "4k",
                ]

                if not any(term in artist.lower() for term in exclude_terms):
                    return artist, track

        return None, title

    def save_cleaned_data(
        self, df: pd.DataFrame, table_name: str = "cleaned_music_data"
    ):
        """Save cleaned unified dataset to database"""
        conn = sqlite3.connect(self.db_path)

        # Create table if it doesn't exist
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        conn.commit()
        conn.close()

        logger.info(f"Saved {len(df)} cleaned records to {table_name} table")

    def generate_cleaning_report(
        self, original_chart_count: int, original_youtube_count: int, final_count: int
    ) -> Dict:
        """Generate a report of the cleaning process"""
        report = {
            "original_chart_records": original_chart_count,
            "original_youtube_records": original_youtube_count,
            "total_original_records": original_chart_count + original_youtube_count,
            "final_unified_records": final_count,
            "total_removed": (original_chart_count + original_youtube_count)
            - final_count,
            "cleaning_timestamp": datetime.now().isoformat(),
        }

        return report


# Example usage and testing
if __name__ == "__main__":
    cleaner = MusicDataCleaner()

    print("Starting data cleaning process...")

    # Get original counts
    conn = sqlite3.connect("music_data.db")
    chart_count = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM chart_data", conn
    ).iloc[0]["count"]
    youtube_count = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM youtube_videos", conn
    ).iloc[0]["count"]
    conn.close()

    print(
        f"Original data: {chart_count} chart records, {youtube_count} YouTube records"
    )

    # Create unified dataset
    unified_df = cleaner.create_unified_dataset()

    if not unified_df.empty:
        # Save cleaned data
        cleaner.save_cleaned_data(unified_df)

        # Generate report
        report = cleaner.generate_cleaning_report(
            chart_count, youtube_count, len(unified_df)
        )

        print("\nCleaning Report:")
        print(f"- Original records: {report['total_original_records']}")
        print(f"- Final records: {report['final_unified_records']}")
        print(f"- Records removed: {report['total_removed']}")
        print(
            f"- Data quality improvement: {((report['final_unified_records']/report['total_original_records'])*100):.1f}% retained"
        )

        # Show sample of cleaned data
        print("\nSample of cleaned data:")
        print(
            unified_df[
                ["track_name", "artist_name", "source", "view_count", "chart_position"]
            ].head()
        )
    else:
        print("No data available for cleaning. Please run data collection first.")
