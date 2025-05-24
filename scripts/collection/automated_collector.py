import schedule
import time
from datetime import datetime
import logging


def daily_collection_job():
    """Run daily data collection from all sources"""
    try:
        # iTunes charts
        chart_collector = ChartDataCollector()
        chart_data = chart_collector.collect_all_charts()

        # YouTube trending
        youtube_collector = YouTubeDataCollector(API_KEY)
        youtube_data = youtube_collector.collect_trending_music_data()

        # Log success
        logging.info(
            f"Daily collection completed: {len(chart_data + youtube_data)} records"
        )

    except Exception as e:
        logging.error(f"Daily collection failed: {e}")


# Schedule daily collection
schedule.every().day.at("02:00").do(daily_collection_job)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)
