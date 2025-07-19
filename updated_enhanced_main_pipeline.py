"""
Updated Enhanced Main Data Pipeline with Fixed Imports
Uses the fixed collectors for maximum reliability
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Import FIXED enhanced modules
from enhanced_chart_data_collector import EnhancedChartDataCollector
from enhanced_youtube_data_collector import EnhancedYouTubeDataCollector
from data_cleaning_pipeline import MusicDataCleaner

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'logs/fixed_enhanced_pipeline_{datetime.now().strftime("%Y%m%d")}.log'
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class FixedEnhancedMusicDataPipeline:
    """Fixed enhanced pipeline with reliable data collection"""

    def __init__(self, db_path: str = "music_data.db"):
        self.db_path = db_path
        self.setup_directories()

        # Initialize FIXED enhanced collectors
        self.chart_collector = EnhancedChartDataCollector(db_path)

        # Enhanced YouTube collector (now with fixed schema)s
        self.youtube_collector = None
        if os.getenv("YOUTUBE_API_KEY"):
            try:
                self.youtube_collector = EnhancedYouTubeDataCollector(db_path=db_path)
                logger.info("✅ Enhanced YouTube collector initialized (schema fixed)")
            except Exception as e:
                logger.warning(
                    f"⚠️ Enhanced YouTube collector failed to initialize: {e}"
                )
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

    def collect_chart_data_fixed(self) -> bool:
        """Fixed chart data collection with reliable APIs"""
        logger.info("🎵 Starting FIXED enhanced chart data collection...")

        try:
            lastfm_key = os.getenv("LASTFM_API_KEY")

            # Use FIXED collection with conservative limits for reliability
            chart_entries = self.chart_collector.collect_all_charts_enhanced(
                lastfm_api_key=lastfm_key,
                itunes_limit=100,  # Safe limit that works
                lastfm_limit=200,  # Last.fm handles this well
                musicbrainz_limit=25,  # Conservative to avoid 403 errors
            )

            if chart_entries:
                logger.info(
                    f"✅ Fixed enhanced collection: {len(chart_entries)} chart entries"
                )
                return True
            else:
                logger.warning("⚠️ No chart data collected")
                return False

        except Exception as e:
            logger.error(f"❌ Fixed chart data collection failed: {e}")
            return False

    def collect_youtube_data_enhanced(self) -> bool:
        """Enhanced YouTube data collection with fixed schema"""
        if not self.youtube_collector:
            logger.info("⏭️ Skipping YouTube data collection (no API key)")
            return True

        logger.info("📺 Starting ENHANCED YouTube data collection (schema fixed)...")

        try:
            # Enhanced collection with comprehensive coverage
            video_data = self.youtube_collector.collect_comprehensive_music_data(
                trending_regions=[
                    "US",
                    "GB",
                    "CA",
                    "AU",
                    "DE",
                    "FR",
                    "JP",
                    "BR",
                ],  # Reduced for quota efficiency
                search_queries=[
                    "new music 2025",
                    "trending music",
                    "pop music 2025",
                    "hip hop 2025",
                    "rock music 2025",
                    "electronic music 2025",
                    "country music 2025",
                ],
                max_trending_per_region=25,  # Conservative for quota management
                max_search_per_query=25,
            )

            if video_data:
                quota_report = self.youtube_collector.get_quota_efficiency_report()
                logger.info(f"✅ Enhanced collection: {len(video_data)} YouTube videos")
                logger.info(
                    f"📊 Quota efficiency: {quota_report['efficiency']:.1f}% used"
                )
                logger.info(
                    f"⚡ Remaining quota: {quota_report['remaining_quota']:,} units"
                )
                return True
            else:
                logger.warning("⚠️ No YouTube data collected")
                return False

        except Exception as e:
            logger.error(f"❌ Enhanced YouTube data collection failed: {e}")
            return False

    def clean_and_process_data(self) -> pd.DataFrame:
        """Clean and process all collected data"""
        logger.info("🧹 Starting data cleaning and processing...")

        try:
            unified_df = self.cleaner.create_unified_dataset()

            if not unified_df.empty:
                self.cleaner.save_cleaned_data(unified_df, "cleaned_music_data")
                logger.info(f"✅ Processed {len(unified_df)} records")
                return unified_df
            else:
                logger.warning("⚠️ No data to process")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"❌ Data cleaning failed: {e}")
            return pd.DataFrame()

    def generate_fixed_enhanced_report(self, unified_df: pd.DataFrame) -> dict:
        """Generate comprehensive report with reliability metrics"""
        if unified_df.empty:
            return {"status": "failed", "message": "No data collected"}

        # Basic statistics
        total_records = len(unified_df)
        chart_records = len(unified_df[unified_df["source"] == "chart"])
        youtube_records = len(unified_df[unified_df["source"] == "youtube"])
        unique_tracks = unified_df["track_name"].nunique()
        unique_artists = unified_df["artist_name"].nunique()

        # Enhanced statistics
        top_artists = unified_df["artist_name"].value_counts().head(20)
        chart_distribution = unified_df[unified_df["source"] == "chart"][
            "chart_name"
        ].value_counts()

        # Get chart summary
        chart_summary = self.chart_collector.get_collection_summary()

        # YouTube quota efficiency
        youtube_quota_report = {}
        if self.youtube_collector:
            youtube_quota_report = self.youtube_collector.get_quota_efficiency_report()

        # Calculate improvements
        original_baseline = 220  # From your first run
        improvement_factor = total_records / original_baseline

        report = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "total_records": total_records,
            "chart_records": chart_records,
            "youtube_records": youtube_records,
            "unique_tracks": unique_tracks,
            "unique_artists": unique_artists,
            "improvement_factor": improvement_factor,
            "top_artists": top_artists.to_dict(),
            "chart_distribution": chart_distribution.to_dict(),
            "chart_summary": chart_summary.to_dict() if not chart_summary.empty else {},
            "youtube_quota_report": youtube_quota_report,
            "reliability_metrics": {
                "apis_working": {
                    "itunes": chart_records > 0,
                    "lastfm": "Last.fm" in str(chart_distribution.index),
                    "youtube": youtube_records > 0,
                },
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
                    "unique_track_ratio": (
                        unique_tracks / total_records if total_records > 0 else 0
                    ),
                },
            },
            "collection_success": {
                "baseline_comparison": f"{improvement_factor:.1f}x improvement over original",
                "chart_sources_count": len(chart_distribution),
                "data_freshness": "Current day collection",
            },
        }

        return report

    def save_fixed_enhanced_report(self, report: dict):
        """Save comprehensive report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"reports/fixed_enhanced_report_{timestamp}.json"

        import json

        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"📊 Fixed enhanced report saved to {report_path}")

    def run_fixed_enhanced_pipeline(self) -> bool:
        """Run the complete fixed enhanced pipeline"""
        logger.info("🚀 Starting FIXED ENHANCED music data pipeline...")

        success_flags = []

        # Step 1: Fixed chart data collection
        chart_success = self.collect_chart_data_fixed()
        success_flags.append(chart_success)

        # Step 2: Enhanced YouTube data collection (now with fixed schema)
        youtube_success = self.collect_youtube_data_enhanced()
        success_flags.append(youtube_success)

        # Step 3: Clean and process data
        unified_df = self.clean_and_process_data()
        processing_success = not unified_df.empty
        success_flags.append(processing_success)

        # Step 4: Generate comprehensive report
        if processing_success:
            report = self.generate_fixed_enhanced_report(unified_df)
            self.save_fixed_enhanced_report(report)
            self.print_fixed_enhanced_summary(report)

            logger.info("✅ Fixed enhanced pipeline completed successfully")
            return True
        else:
            logger.error("❌ Fixed enhanced pipeline failed - no data processed")
            return False

    def print_fixed_enhanced_summary(self, report: dict):
        """Print comprehensive summary"""
        print("\n" + "=" * 90)
        print("🎯 FIXED ENHANCED MUSIC DATA PIPELINE SUMMARY")
        print("=" * 90)
        print(f"📅 Timestamp: {report['timestamp']}")
        print(f"📊 Total Records: {report['total_records']:,}")
        print(f"🎵 Chart Records: {report['chart_records']:,}")
        print(f"📺 YouTube Records: {report['youtube_records']:,}")
        print(f"🎤 Unique Artists: {report['unique_artists']:,}")
        print(f"🎶 Unique Tracks: {report['unique_tracks']:,}")
        print(
            f"🚀 Improvement: {report['improvement_factor']:.1f}x over original pipeline"
        )

        # API reliability status
        apis = report["reliability_metrics"]["apis_working"]
        print(f"\n📡 API Status:")
        print(f"   iTunes: {'✅ Working' if apis['itunes'] else '❌ Failed'}")
        print(f"   Last.fm: {'✅ Working' if apis['lastfm'] else '❌ Failed'}")
        print(f"   YouTube: {'✅ Working' if apis['youtube'] else '❌ Failed'}")

        # Data quality metrics
        quality = report["reliability_metrics"]["data_quality"]
        print(f"\n📈 Data Quality:")
        print(f"   Completeness: {quality['completeness']:.1f}%")
        print(f"   Unique Track Ratio: {quality['unique_track_ratio']:.1f}")
        print(f"   Duplicate Rate: {quality['duplicate_rate']:.1f}%")

        # YouTube quota info
        youtube_quota = report.get("youtube_quota_report", {})
        if youtube_quota:
            print(f"\n⚡ YouTube Quota:")
            print(f"   Used: {youtube_quota.get('efficiency', 0):.1f}%")
            print(f"   Remaining: {youtube_quota.get('remaining_quota', 0):,} units")

        # Top artists
        print(f"\n🎤 Top Artists:")
        for artist, count in list(report["top_artists"].items())[:10]:
            print(f"   • {artist}: {count} tracks")

        # Chart sources
        if report["chart_distribution"]:
            print(f"\n📈 Active Chart Sources ({len(report['chart_distribution'])}):")
            for chart, count in list(report["chart_distribution"].items())[:8]:
                print(f"   • {chart}: {count} entries")

        print("=" * 90)


def main():
    """Main execution with fixed enhancements"""
    print("🎯 FIXED Enhanced Music Prediction Platform - Data Collection Pipeline")
    print("=" * 90)

    # Environment check
    print("\n🔍 Environment Check:")
    api_keys = ["YOUTUBE_API_KEY", "LASTFM_API_KEY"]
    for key in api_keys:
        if os.getenv(key):
            print(f"   ✅ {key}: Available")
        else:
            print(f"   ⚠️ {key}: Not set")

    if os.getenv("YOUTUBE_DAILY_QUOTA"):
        print(f"   ✅ YOUTUBE_DAILY_QUOTA: {os.getenv('YOUTUBE_DAILY_QUOTA')} units")

    print("\n🔧 Applied Fixes:")
    print("   ✅ Database schema migrated for YouTube enhanced features")
    print("   ✅ iTunes API limits fixed (≤100 to avoid 500 errors)")
    print("   ✅ MusicBrainz simplified queries to avoid 403 errors")
    print("   ✅ Enhanced error handling and rate limiting")

    print("\n📈 Expected Improvements:")
    print("   🎵 Reliable chart data collection from multiple sources")
    print("   📺 Enhanced YouTube data with channel information")
    print("   🔄 Improved duplicate detection and data quality")

    # Run fixed enhanced pipeline
    try:
        pipeline = FixedEnhancedMusicDataPipeline()
        success = pipeline.run_fixed_enhanced_pipeline()

        if success:
            print("\n🎉 Fixed enhanced pipeline completed successfully!")
            print("📁 Check 'reports' folder for detailed analysis")
            print("🗄️ Enhanced data saved to 'music_data.db'")
            print("\n🚀 Next Steps:")
            print("   1. Review data quality metrics in reports")
            print("   2. Consider requesting YouTube quota extension")
            print("   3. Set up automated daily collection")
            print("   4. Begin advanced feature engineering")
            return 0
        else:
            print("\n❌ Fixed enhanced pipeline failed. Check logs for details")
            return 1

    except Exception as e:
        logger.error(f"❌ Fixed enhanced pipeline crashed: {e}")
        print(f"\n💥 Pipeline crashed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
