"""
Configuration management for Music Popularity Prediction Platform
"""

import os
import json
from dataclasses import dataclass
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    """Configuration for external APIs"""

    spotify_client_id: Optional[str] = None
    spotify_client_secret: Optional[str] = None
    youtube_api_key: Optional[str] = None
    lastfm_api_key: Optional[str] = None
    soundcloud_client_id: Optional[str] = None

    def __post_init__(self):
        # Try to load from environment variables
        self.spotify_client_id = self.spotify_client_id or os.getenv(
            "SPOTIFY_CLIENT_ID"
        )
        self.spotify_client_secret = self.spotify_client_secret or os.getenv(
            "SPOTIFY_CLIENT_SECRET"
        )
        self.youtube_api_key = self.youtube_api_key or os.getenv("YOUTUBE_API_KEY")
        self.lastfm_api_key = self.lastfm_api_key or os.getenv("LASTFM_API_KEY")
        self.soundcloud_client_id = self.soundcloud_client_id or os.getenv(
            "SOUNDCLOUD_CLIENT_ID"
        )

    def validate(self) -> tuple[bool, List[str]]:
        """Validate that required API keys are present"""
        missing_keys = []

        if not self.spotify_client_id:
            missing_keys.append("Spotify Client ID")
        if not self.spotify_client_secret:
            missing_keys.append("Spotify Client Secret")

        # Optional APIs
        optional_missing = []
        if not self.youtube_api_key:
            optional_missing.append("YouTube API Key")
        if not self.lastfm_api_key:
            optional_missing.append("Last.fm API Key")
        if not self.soundcloud_client_id:
            optional_missing.append("SoundCloud Client ID")

        if missing_keys:
            logger.error(f"Missing required API keys: {', '.join(missing_keys)}")
            return False, missing_keys

        if optional_missing:
            logger.warning(f"Missing optional API keys: {', '.join(optional_missing)}")

        return True, []


@dataclass
class DatabaseConfig:
    """Database configuration"""

    db_path: str = "music_prediction.db"
    backup_interval_hours: int = 24


@dataclass
class CollectionConfig:
    """Data collection configuration"""

    # Spotify settings
    spotify_regions: List[str] = None
    spotify_chart_limit: int = 50

    # Collection intervals
    collection_interval_hours: int = 24
    social_metrics_interval_hours: int = 6

    # Data retention
    keep_predictions_days: int = 365
    keep_social_metrics_days: int = 90

    def __post_init__(self):
        if self.spotify_regions is None:
            self.spotify_regions = ["US", "GB", "CA", "AU", "DE", "FR", "JP"]


@dataclass
class ModelConfig:
    """Machine learning model configuration"""

    # Feature engineering
    min_days_since_release: int = 1
    max_days_since_release: int = 30

    # Labeling criteria
    hit_threshold_percentile: float = 95.0  # Top 5% = hit
    min_streams_for_hit: int = 1000000

    # Model training
    train_test_split_ratio: float = 0.8
    validation_split_ratio: float = 0.2
    random_state: int = 42

    # Model types to train
    models_to_train: List[str] = None

    def __post_init__(self):
        if self.models_to_train is None:
            self.models_to_train = ["logistic", "random_forest", "xgboost"]


class Config:
    """Main configuration class"""

    def __init__(self, config_file: Optional[str] = None):
        self.api = APIConfig()
        self.database = DatabaseConfig()
        self.collection = CollectionConfig()
        self.model = ModelConfig()

        if config_file:
            self.load_from_file(config_file)

    def load_from_file(self, config_file: str = "config.json"):
        """Load configuration from JSON file"""
        try:
            with open(config_file, "r") as f:
                config_data = json.load(f)

            # Update configurations
            if "api" in config_data:
                for key, value in config_data["api"].items():
                    if hasattr(self.api, key):
                        setattr(self.api, key, value)

            if "database" in config_data:
                for key, value in config_data["database"].items():
                    if hasattr(self.database, key):
                        setattr(self.database, key, value)

            if "collection" in config_data:
                for key, value in config_data["collection"].items():
                    if hasattr(self.collection, key):
                        setattr(self.collection, key, value)

            if "model" in config_data:
                for key, value in config_data["model"].items():
                    if hasattr(self.model, key):
                        setattr(self.model, key, value)

            logger.info(f"Configuration loaded from {config_file}")

        except FileNotFoundError:
            logger.info(
                f"Config file {config_file} not found, using defaults and environment variables"
            )
        except Exception as e:
            logger.error(f"Error loading config: {e}")

    def save_to_file(self, config_file: str = "config.json"):
        """Save current configuration to JSON file"""
        config_data = {
            "api": {
                "spotify_client_id": self.api.spotify_client_id,
                "spotify_client_secret": self.api.spotify_client_secret,
                "youtube_api_key": self.api.youtube_api_key,
                "lastfm_api_key": self.api.lastfm_api_key,
                "soundcloud_client_id": self.api.soundcloud_client_id,
            },
            "database": {
                "db_path": self.database.db_path,
                "backup_interval_hours": self.database.backup_interval_hours,
            },
            "collection": {
                "spotify_regions": self.collection.spotify_regions,
                "spotify_chart_limit": self.collection.spotify_chart_limit,
                "collection_interval_hours": self.collection.collection_interval_hours,
                "social_metrics_interval_hours": self.collection.social_metrics_interval_hours,
                "keep_predictions_days": self.collection.keep_predictions_days,
                "keep_social_metrics_days": self.collection.keep_social_metrics_days,
            },
            "model": {
                "min_days_since_release": self.model.min_days_since_release,
                "max_days_since_release": self.model.max_days_since_release,
                "hit_threshold_percentile": self.model.hit_threshold_percentile,
                "min_streams_for_hit": self.model.min_streams_for_hit,
                "train_test_split_ratio": self.model.train_test_split_ratio,
                "validation_split_ratio": self.model.validation_split_ratio,
                "random_state": self.model.random_state,
                "models_to_train": self.model.models_to_train,
            },
        }

        try:
            with open(config_file, "w") as f:
                json.dump(config_data, f, indent=2)
            logger.info(f"Configuration saved to {config_file}")
        except Exception as e:
            logger.error(f"Error saving config: {e}")

    def validate_all(self) -> bool:
        """Validate all configurations"""
        api_valid, missing_keys = self.api.validate()

        if not api_valid:
            print(f"❌ Missing required API keys: {', '.join(missing_keys)}")
            print("\nTo get API keys:")
            print("1. Spotify: https://developer.spotify.com/dashboard/")
            print("2. YouTube: https://console.cloud.google.com/")
            print("3. Last.fm: https://www.last.fm/api/account/create")
            print("4. SoundCloud: https://developers.soundcloud.com/")
            return False

        print("✅ Configuration validation passed!")
        return True


# Global config instance
config = Config()


def setup_environment():
    """Setup environment variables and config file template"""

    # Check if .env file exists, if not create template
    env_file = ".env"
    if not os.path.exists(env_file):
        env_template = """# API Keys for Music Prediction Platform
# Get these from the respective developer portals

# Spotify API (Required)
SPOTIFY_CLIENT_ID=your_spotify_client_id_here
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret_here

# YouTube API (Optional but recommended)
YOUTUBE_API_KEY=your_youtube_api_key_here

# Last.fm API (Optional)
LASTFM_API_KEY=your_lastfm_api_key_here

# SoundCloud API (Optional)
SOUNDCLOUD_CLIENT_ID=your_soundcloud_client_id_here
"""

        with open(env_file, "w") as f:
            f.write(env_template)

        print(f"Created {env_file} template. Please fill in your API keys.")

    # Check if config.json exists, if not create template
    config_file = "config.json"
    if not os.path.exists(config_file):
        config.save_to_file(config_file)
        print(f"Created {config_file} with default settings.")

    return env_file, config_file


if __name__ == "__main__":
    # Setup environment
    env_file, config_file = setup_environment()

    # Load configuration
    config.load_from_file(config_file)

    # Validate
    if config.validate_all():
        print("🎵 Ready to start collecting music data!")
    else:
        print("⚠️  Please configure your API keys before proceeding.")
        print(f"Edit {env_file} or {config_file} with your API credentials.")
