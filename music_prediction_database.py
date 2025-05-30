"""
Complete Database Schema for Music Popularity Prediction Platform
Implements the database structure needed for the project requirements
"""

import sqlite3
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MusicPredictionDatabase:
    """Main database class for the music prediction platform"""

    def __init__(self, db_path: str = "music_prediction.db"):
        self.db_path = db_path
        self.setup_database()

    def setup_database(self):
        """Create all necessary tables for the music prediction platform"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")

            # 1. ARTISTS TABLE - Store artist information
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS artists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    spotify_id TEXT UNIQUE,
                    name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL,
                    followers INTEGER DEFAULT 0,
                    popularity INTEGER DEFAULT 0,
                    genres TEXT, -- JSON array of genres
                    image_url TEXT,
                    external_urls TEXT, -- JSON object
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(normalized_name)
                )
            """
            )

            # 2. TRACKS TABLE - Store track information
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tracks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    spotify_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL,
                    artist_id INTEGER NOT NULL,
                    album_name TEXT,
                    release_date DATE,
                    duration_ms INTEGER,
                    explicit BOOLEAN DEFAULT FALSE,
                    popularity INTEGER DEFAULT 0,
                    preview_url TEXT,
                    external_urls TEXT, -- JSON object
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (artist_id) REFERENCES artists (id),
                    UNIQUE(spotify_id)
                )
            """
            )

            # 3. AUDIO FEATURES TABLE - Spotify audio features
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS audio_features (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL,
                    danceability REAL,
                    energy REAL,
                    key_signature INTEGER,
                    loudness REAL,
                    mode INTEGER,
                    speechiness REAL,
                    acousticness REAL,
                    instrumentalness REAL,
                    liveness REAL,
                    valence REAL,
                    tempo REAL,
                    time_signature INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (track_id) REFERENCES tracks (id),
                    UNIQUE(track_id)
                )
            """
            )

            # 4. CHART ENTRIES TABLE - Chart positions
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS chart_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL,
                    chart_name TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    chart_date DATE NOT NULL,
                    region_code TEXT,
                    chart_type TEXT, -- 'spotify', 'itunes', 'billboard', etc.
                    streams INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (track_id) REFERENCES tracks (id),
                    UNIQUE(track_id, chart_name, chart_date, region_code)
                )
            """
            )

            # 5. SOCIAL MEDIA METRICS TABLE - TikTok, YouTube, etc.
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS social_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL,
                    platform TEXT NOT NULL, -- 'tiktok', 'youtube', 'soundcloud', 'instagram'
                    platform_track_id TEXT, -- ID on that platform
                    view_count INTEGER DEFAULT 0,
                    like_count INTEGER DEFAULT 0,
                    comment_count INTEGER DEFAULT 0,
                    share_count INTEGER DEFAULT 0,
                    video_count INTEGER DEFAULT 0, -- For TikTok: number of videos using this song
                    engagement_rate REAL,
                    collected_date DATE NOT NULL,
                    additional_data TEXT, -- JSON for platform-specific metrics
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (track_id) REFERENCES tracks (id),
                    UNIQUE(track_id, platform, collected_date)
                )
            """
            )

            # 6. POPULARITY LABELS TABLE - Our target variable
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS popularity_labels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL,
                    label_date DATE NOT NULL,
                    is_hit BOOLEAN NOT NULL, -- Binary classification
                    popularity_score REAL, -- Continuous score (0-100)
                    streams_first_week INTEGER,
                    streams_first_month INTEGER,
                    peak_chart_position INTEGER,
                    chart_weeks INTEGER, -- How many weeks on charts
                    label_criteria TEXT, -- How we defined "hit" for this record
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (track_id) REFERENCES tracks (id),
                    UNIQUE(track_id, label_date)
                )
            """
            )

            # 7. PREDICTIONS TABLE - Store model predictions
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL,
                    model_name TEXT NOT NULL,
                    model_version TEXT NOT NULL,
                    prediction_date DATE NOT NULL,
                    predicted_probability REAL, -- For classification
                    predicted_value REAL, -- For regression
                    confidence_score REAL,
                    feature_importances TEXT, -- JSON object
                    actual_outcome BOOLEAN, -- For evaluation
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (track_id) REFERENCES tracks (id)
                )
            """
            )

            # 8. FEATURE ENGINEERING TABLE - Engineered features
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS engineered_features (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL,
                    feature_date DATE NOT NULL,
                    
                    -- Time-based features
                    days_since_release INTEGER,
                    release_day_of_week INTEGER,
                    release_month INTEGER,
                    
                    -- Artist features
                    artist_follower_count INTEGER,
                    artist_past_hits INTEGER,
                    artist_genre_diversity REAL,
                    
                    -- Social momentum features
                    tiktok_growth_rate REAL,
                    youtube_growth_rate REAL,
                    social_buzz_score REAL,
                    
                    -- Audio feature combinations
                    energy_valence_ratio REAL,
                    danceability_energy_product REAL,
                    tempo_category TEXT,
                    
                    -- Competition features
                    same_genre_releases_week INTEGER,
                    artist_competition_score REAL,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (track_id) REFERENCES tracks (id),
                    UNIQUE(track_id, feature_date)
                )
            """
            )

            # 9. MODEL EXPERIMENTS TABLE - Track model performance
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS model_experiments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    experiment_name TEXT NOT NULL,
                    model_type TEXT NOT NULL, -- 'logistic', 'random_forest', 'xgboost', etc.
                    features_used TEXT, -- JSON array of feature names
                    hyperparameters TEXT, -- JSON object
                    train_start_date DATE,
                    train_end_date DATE,
                    
                    -- Performance metrics
                    auc_score REAL,
                    precision REAL,
                    recall REAL,
                    f1_score REAL,
                    rmse REAL,
                    
                    model_file_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(experiment_name)
                )
            """
            )

            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_artists_spotify_id ON artists(spotify_id)",
                "CREATE INDEX IF NOT EXISTS idx_artists_normalized ON artists(normalized_name)",
                "CREATE INDEX IF NOT EXISTS idx_tracks_spotify_id ON tracks(spotify_id)",
                "CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist_id)",
                "CREATE INDEX IF NOT EXISTS idx_tracks_release_date ON tracks(release_date)",
                "CREATE INDEX IF NOT EXISTS idx_chart_entries_track ON chart_entries(track_id)",
                "CREATE INDEX IF NOT EXISTS idx_chart_entries_date ON chart_entries(chart_date)",
                "CREATE INDEX IF NOT EXISTS idx_chart_entries_position ON chart_entries(position)",
                "CREATE INDEX IF NOT EXISTS idx_social_metrics_track ON social_metrics(track_id)",
                "CREATE INDEX IF NOT EXISTS idx_social_metrics_platform ON social_metrics(platform)",
                "CREATE INDEX IF NOT EXISTS idx_social_metrics_date ON social_metrics(collected_date)",
                "CREATE INDEX IF NOT EXISTS idx_popularity_labels_track ON popularity_labels(track_id)",
                "CREATE INDEX IF NOT EXISTS idx_predictions_track ON predictions(track_id)",
                "CREATE INDEX IF NOT EXISTS idx_predictions_model ON predictions(model_name)",
                "CREATE INDEX IF NOT EXISTS idx_engineered_features_track ON engineered_features(track_id)",
            ]

            for index_sql in indexes:
                cursor.execute(index_sql)

            conn.commit()
            logger.info("Database schema created successfully")

        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def normalize_text(self, text: str) -> str:
        """Normalize text for consistent matching"""
        if not text:
            return ""

        import unicodedata
        import re

        # Normalize unicode and convert to lowercase
        text = unicodedata.normalize("NFKD", text.lower())

        # Remove special characters but keep spaces and basic punctuation
        text = re.sub(r"[^\w\s\-\.\'\&]", "", text)

        # Normalize spaces
        text = " ".join(text.split())

        return text.strip()

    def insert_artist(self, artist_data: Dict) -> int:
        """Insert or update artist and return artist ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            normalized_name = self.normalize_text(artist_data.get("name", ""))

            # Check if artist exists
            cursor.execute(
                "SELECT id FROM artists WHERE spotify_id = ? OR normalized_name = ?",
                (artist_data.get("spotify_id"), normalized_name),
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing artist
                cursor.execute(
                    """
                    UPDATE artists SET 
                        name = ?, followers = ?, popularity = ?, genres = ?, 
                        image_url = ?, external_urls = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """,
                    (
                        artist_data.get("name"),
                        artist_data.get("followers", 0),
                        artist_data.get("popularity", 0),
                        json.dumps(artist_data.get("genres", [])),
                        artist_data.get("image_url"),
                        json.dumps(artist_data.get("external_urls", {})),
                        existing[0],
                    ),
                )
                artist_id = existing[0]
            else:
                # Insert new artist
                cursor.execute(
                    """
                    INSERT INTO artists 
                    (spotify_id, name, normalized_name, followers, popularity, genres, image_url, external_urls)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        artist_data.get("spotify_id"),
                        artist_data.get("name"),
                        normalized_name,
                        artist_data.get("followers", 0),
                        artist_data.get("popularity", 0),
                        json.dumps(artist_data.get("genres", [])),
                        artist_data.get("image_url"),
                        json.dumps(artist_data.get("external_urls", {})),
                    ),
                )
                artist_id = cursor.lastrowid

            conn.commit()
            return artist_id

        except Exception as e:
            logger.error(f"Error inserting artist: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_track(self, track_data: Dict, artist_id: int) -> int:
        """Insert or update track and return track ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            normalized_name = self.normalize_text(track_data.get("name", ""))

            # Check if track exists
            cursor.execute(
                "SELECT id FROM tracks WHERE spotify_id = ?",
                (track_data.get("spotify_id"),),
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing track
                cursor.execute(
                    """
                    UPDATE tracks SET 
                        name = ?, album_name = ?, release_date = ?, duration_ms = ?,
                        explicit = ?, popularity = ?, preview_url = ?, external_urls = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """,
                    (
                        track_data.get("name"),
                        track_data.get("album_name"),
                        track_data.get("release_date"),
                        track_data.get("duration_ms"),
                        track_data.get("explicit", False),
                        track_data.get("popularity", 0),
                        track_data.get("preview_url"),
                        json.dumps(track_data.get("external_urls", {})),
                        existing[0],
                    ),
                )
                track_id = existing[0]
            else:
                # Insert new track
                cursor.execute(
                    """
                    INSERT INTO tracks 
                    (spotify_id, name, normalized_name, artist_id, album_name, release_date, 
                     duration_ms, explicit, popularity, preview_url, external_urls)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        track_data.get("spotify_id"),
                        track_data.get("name"),
                        normalized_name,
                        artist_id,
                        track_data.get("album_name"),
                        track_data.get("release_date"),
                        track_data.get("duration_ms"),
                        track_data.get("explicit", False),
                        track_data.get("popularity", 0),
                        track_data.get("preview_url"),
                        json.dumps(track_data.get("external_urls", {})),
                    ),
                )
                track_id = cursor.lastrowid

            conn.commit()
            return track_id

        except Exception as e:
            logger.error(f"Error inserting track: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_audio_features(self, track_id: int, features: Dict):
        """Insert audio features for a track"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO audio_features 
                (track_id, danceability, energy, key_signature, loudness, mode,
                 speechiness, acousticness, instrumentalness, liveness, valence,
                 tempo, time_signature)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    track_id,
                    features.get("danceability"),
                    features.get("energy"),
                    features.get("key"),
                    features.get("loudness"),
                    features.get("mode"),
                    features.get("speechiness"),
                    features.get("acousticness"),
                    features.get("instrumentalness"),
                    features.get("liveness"),
                    features.get("valence"),
                    features.get("tempo"),
                    features.get("time_signature"),
                ),
            )

            conn.commit()

        except Exception as e:
            logger.error(f"Error inserting audio features: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_social_metrics(
        self, track_id: int, platform: str, metrics: Dict, date: str
    ):
        """Insert social media metrics for a track"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO social_metrics 
                (track_id, platform, platform_track_id, view_count, like_count,
                 comment_count, share_count, video_count, engagement_rate,
                 collected_date, additional_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    track_id,
                    platform,
                    metrics.get("platform_track_id"),
                    metrics.get("view_count", 0),
                    metrics.get("like_count", 0),
                    metrics.get("comment_count", 0),
                    metrics.get("share_count", 0),
                    metrics.get("video_count", 0),
                    metrics.get("engagement_rate"),
                    date,
                    json.dumps(metrics.get("additional_data", {})),
                ),
            )

            conn.commit()

        except Exception as e:
            logger.error(f"Error inserting social metrics: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_tracks_for_prediction(self, limit: int = 100) -> pd.DataFrame:
        """Get tracks with all their features for prediction"""
        conn = sqlite3.connect(self.db_path)

        query = """
        SELECT 
            t.id as track_id,
            t.spotify_id,
            t.name as track_name,
            t.popularity as spotify_popularity,
            t.release_date,
            t.duration_ms,
            t.explicit,
            
            a.name as artist_name,
            a.followers as artist_followers,
            a.popularity as artist_popularity,
            a.genres as artist_genres,
            
            af.danceability,
            af.energy,
            af.key_signature,
            af.loudness,
            af.mode,
            af.speechiness,
            af.acousticness,
            af.instrumentalness,
            af.liveness,
            af.valence,
            af.tempo,
            af.time_signature,
            
            pl.is_hit,
            pl.popularity_score,
            pl.streams_first_week
            
        FROM tracks t
        JOIN artists a ON t.artist_id = a.id
        LEFT JOIN audio_features af ON t.id = af.track_id
        LEFT JOIN popularity_labels pl ON t.id = pl.track_id
        WHERE af.track_id IS NOT NULL
        ORDER BY t.created_at DESC
        LIMIT ?
        """

        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()

        return df

    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {}

        try:
            # Basic counts
            tables = [
                "artists",
                "tracks",
                "audio_features",
                "chart_entries",
                "social_metrics",
                "popularity_labels",
                "predictions",
            ]

            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()[0]

            # Additional insights
            cursor.execute("SELECT COUNT(*) FROM tracks WHERE popularity > 70")
            stats["high_popularity_tracks"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT artist_id) FROM tracks")
            stats["unique_artists_with_tracks"] = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT COUNT(*) FROM popularity_labels WHERE is_hit = 1
            """
            )
            stats["labeled_hits"] = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT AVG(streams_first_week) FROM popularity_labels 
                WHERE streams_first_week IS NOT NULL
            """
            )
            result = cursor.fetchone()
            stats["avg_first_week_streams"] = result[0] if result[0] else 0

        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
        finally:
            conn.close()

        return stats


def create_sample_data():
    """Create some sample data for testing"""
    db = MusicPredictionDatabase()

    # Sample artist
    artist_data = {
        "spotify_id": "1uNFoZAHBGtllmzznpCI3s",
        "name": "Justin Bieber",
        "followers": 70000000,
        "popularity": 95,
        "genres": ["pop", "dance pop"],
        "image_url": "https://example.com/image.jpg",
        "external_urls": {
            "spotify": "https://open.spotify.com/artist/1uNFoZAHBGtllmzznpCI3s"
        },
    }

    artist_id = db.insert_artist(artist_data)

    # Sample track
    track_data = {
        "spotify_id": "4iV5W9uYEdYUVa79Axb7Rh",
        "name": "Never Say Never",
        "album_name": "Never Say Never",
        "release_date": "2024-01-15",
        "duration_ms": 240000,
        "explicit": False,
        "popularity": 85,
        "preview_url": "https://example.com/preview.mp3",
        "external_urls": {
            "spotify": "https://open.spotify.com/track/4iV5W9uYEdYUVa79Axb7Rh"
        },
    }

    track_id = db.insert_track(track_data, artist_id)

    # Sample audio features
    audio_features = {
        "danceability": 0.735,
        "energy": 0.578,
        "key": 7,
        "loudness": -5.594,
        "mode": 1,
        "speechiness": 0.0461,
        "acousticness": 0.514,
        "instrumentalness": 0.0,
        "liveness": 0.144,
        "valence": 0.636,
        "tempo": 150.062,
        "time_signature": 4,
    }

    db.insert_audio_features(track_id, audio_features)

    # Sample social metrics
    social_metrics = {
        "platform_track_id": "yt_12345",
        "view_count": 10000000,
        "like_count": 500000,
        "comment_count": 25000,
        "video_count": 100,  # For TikTok
        "engagement_rate": 0.05,
    }

    db.insert_social_metrics(track_id, "youtube", social_metrics, "2024-01-20")

    logger.info("Sample data created successfully")
    return db


if __name__ == "__main__":
    # Create database
    db = MusicPredictionDatabase()

    # Create sample data
    create_sample_data()

    # Get stats
    stats = db.get_database_stats()
    print("Database Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print("\nDatabase setup complete! Ready for data collection and modeling.")
