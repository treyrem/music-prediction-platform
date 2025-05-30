"""
Main Data Collection and Processing Pipeline
Orchestrates chart collection, YouTube data collection, and data cleaning
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Import our custom modules
from chart_data_collector import ChartDataCollector
from youtube_data_collector import YouTubeDataCollector
from data_cleaning_pipeline import MusicDataCleaner

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class MusicDataPipeline:
    """Main pipeline orchestrator"""

    def __init__(self, db_path: str = "music_data.db"):
        self.db_path = db_path
        self.setup_directories()

        # Initialize collectors
        self.chart_collector = ChartDataCollector(db_path)

        # YouTube collector (optional if no API key)
        self.youtube_collector = None
        if os.getenv("YOUTUBE_API_KEY"):
            try:
                self.youtube_collector = YouTubeDataCollector(db_path=db_path)
                logger.info("✅ YouTube collector initialized")
            except Exception as e:
                logger.warning(f"⚠️ YouTube collector failed to initialize: {e}")
        else:
            logger.warning(
                "⚠️ No YouTube API key found - skipping YouTube data collection"
            )

        # Data cleaner
        self.cleaner = MusicDataCleaner(db_path)

    def setup_directories(self):
        """Create necessary directories"""
        directories = ["logs", "data", "reports"]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def collect_chart_data(self) -> bool:
        """Collect chart data from all sources"""
        logger.info("🎵 Starting chart data collection...")

        try:
            # Get Last.fm API key if available
            lastfm_key = os.getenv("LASTFM_API_KEY")

            # Collect all chart data
            chart_entries = self.chart_collector.collect_all_charts(lastfm_key)

            if chart_entries:
                logger.info(f"✅ Collected {len(chart_entries)} chart entries")
                return True
            else:
                logger.warning("⚠️ No chart data collected")
                return False

        except Exception as e:
            logger.error(f"❌ Chart data collection failed: {e}")
            return False

    def collect_youtube_data(self) -> bool:
        """Collect YouTube data if collector is available"""
        if not self.youtube_collector:
            logger.info("⏭️ Skipping YouTube data collection (no API key)")
            return True

        logger.info("📺 Starting YouTube data collection...")

        try:
            # Collect trending music data
            video_data = self.youtube_collector.collect_trending_music_data()

            if video_data:
                logger.info(f"✅ Collected {len(video_data)} YouTube videos")
                return True
            else:
                logger.warning("⚠️ No YouTube data collected")
                return False

        except Exception as e:
            logger.error(f"❌ YouTube data collection failed: {e}")
            return False

    def clean_and_process_data(self) -> pd.DataFrame:
        """Clean and process all collected data"""
        logger.info("🧹 Starting data cleaning and processing...")

        try:
            # Create unified dataset
            unified_df = self.cleaner.create_unified_dataset()

            if not unified_df.empty:
                # Save cleaned data
                self.cleaner.save_cleaned_data(unified_df, "cleaned_music_data")
                logger.info(f"✅ Processed {len(unified_df)} records")
                return unified_df
            else:
                logger.warning("⚠️ No data to process")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"❌ Data cleaning failed: {e}")
            return pd.DataFrame()

    def generate_report(self, unified_df: pd.DataFrame) -> dict:
        """Generate data collection report"""
        if unified_df.empty:
            return {"status": "failed", "message": "No data collected"}

        # Basic statistics
        total_records = len(unified_df)
        chart_records = len(unified_df[unified_df["source"] == "chart"])
        youtube_records = len(unified_df[unified_df["source"] == "youtube"])
        unique_tracks = unified_df["track_name"].nunique()
        unique_artists = unified_df["artist_name"].nunique()

        # Top artists by number of tracks
        top_artists = unified_df["artist_name"].value_counts().head(10)

        # Chart distribution
        chart_distribution = unified_df[unified_df["source"] == "chart"][
            "chart_name"
        ].value_counts()

        report = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "total_records": total_records,
            "chart_records": chart_records,
            "youtube_records": youtube_records,
            "unique_tracks": unique_tracks,
            "unique_artists": unique_artists,
            "top_artists": top_artists.to_dict(),
            "chart_distribution": chart_distribution.to_dict(),
            "data_quality": {
                "completeness": (
                    unified_df.notna().sum().sum()
                    / (len(unified_df) * len(unified_df.columns))
                )
                * 100,
                "duplicate_rate": (
                    ((total_records - unique_tracks) / total_records) * 100
                    if total_records > 0
                    else 0
                ),
            },
        }

        return report

    def save_report(self, report: dict):
        """Save report to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"reports/data_collection_report_{timestamp}.json"

        import json

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"📊 Report saved to {report_path}")

    def run_full_pipeline(self) -> bool:
        """Run the complete data collection and processing pipeline"""
        logger.info("🚀 Starting full music data pipeline...")

        success_flags = []

        # Step 1: Collect chart data
        chart_success = self.collect_chart_data()
        success_flags.append(chart_success)

        # Step 2: Collect YouTube data (optional)
        youtube_success = self.collect_youtube_data()
        success_flags.append(youtube_success)

        # Step 3: Clean and process data
        unified_df = self.clean_and_process_data()
        processing_success = not unified_df.empty
        success_flags.append(processing_success)

        # Step 4: Generate report
        if processing_success:
            report = self.generate_report(unified_df)
            self.save_report(report)

            # Print summary
            self.print_summary(report)

            logger.info("✅ Pipeline completed successfully")
            return True
        else:
            logger.error("❌ Pipeline failed - no data processed")
            return False

    def print_summary(self, report: dict):
        """Print a nice summary of the pipeline results"""
        print("\n" + "=" * 60)
        print("📈 MUSIC DATA PIPELINE SUMMARY")
        print("=" * 60)
        print(f"📅 Timestamp: {report['timestamp']}")
        print(f"📊 Total Records: {report['total_records']:,}")
        print(f"🎵 Chart Records: {report['chart_records']:,}")
        print(f"📺 YouTube Records: {report['youtube_records']:,}")
        print(f"🎤 Unique Artists: {report['unique_artists']:,}")
        print(f"🎶 Unique Tracks: {report['unique_tracks']:,}")
        print(f"✨ Data Completeness: {report['data_quality']['completeness']:.1f}%")

        print("\n📊 Top Artists:")
        for artist, count in list(report["top_artists"].items())[:5]:
            print(f"   • {artist}: {count} tracks")

        if report["chart_distribution"]:
            print("\n📈 Chart Sources:")
            for chart, count in report["chart_distribution"].items():
                print(f"   • {chart}: {count} entries")

        print("=" * 60)


def main():
    """Main execution function"""
    print("🎵 Music Prediction Platform - Data Collection Pipeline")
    print("=" * 60)

    # Check for required environment variables
    required_vars = []
    optional_vars = ["YOUTUBE_API_KEY", "LASTFM_API_KEY"]

    print("\n🔍 Environment Check:")
    for var in optional_vars:
        if os.getenv(var):
            print(f"   ✅ {var}: Available")
        else:
            print(f"   ⚠️ {var}: Not set (optional)")

    # Initialize and run pipeline
    try:
        pipeline = MusicDataPipeline()
        success = pipeline.run_full_pipeline()

        if success:
            print("\n🎉 Pipeline completed successfully!")
            print("📁 Check the 'reports' folder for detailed results")
            print("🗄️ Data saved to 'music_data.db'")
            return 0
        else:
            print("\n❌ Pipeline failed. Check logs for details")
            return 1

    except Exception as e:
        logger.error(f"❌ Pipeline crashed: {e}")
        print(f"\n💥 Pipeline crashed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
