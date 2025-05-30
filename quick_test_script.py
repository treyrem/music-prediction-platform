"""
Quick Test Script for Music Prediction Platform
Tests each component individually to identify issues
"""

import os
import sys
import sqlite3
from dotenv import load_dotenv

load_dotenv()

def test_chart_collector():
    """Test chart data collection"""
    print("🎵 Testing Chart Data Collector...")
    
    try:
        from chart_data_collector import ChartDataCollector
        
        collector = ChartDataCollector()
        
        # Test iTunes (no API key required)
        print("   Testing iTunes charts...")
        itunes_data = collector.collect_itunes_top_charts("us", limit=3)
        
        if itunes_data:
            print(f"   ✅ iTunes: {len(itunes_data)} entries")
            print(f"   Sample: {itunes_data[0].track_name} by {itunes_data[0].artist_name}")
        else:
            print("   ❌ iTunes: No data returned")
            return False
        
        # Test Last.fm if API key available
        lastfm_key = os.getenv('LASTFM_API_KEY')
        if lastfm_key and lastfm_key != 'your_lastfm_api_key_here':
            print("   Testing Last.fm charts...")
            lastfm_data = collector.collect_lastfm_top_tracks(lastfm_key, limit=3)
            if lastfm_data:
                print(f"   ✅ Last.fm: {len(lastfm_data)} entries")
            else:
                print("   ⚠️ Last.fm: No data returned")
        else:
            print("   ⏭️ Last.fm: Skipped (no API key)")
        
        # Test MusicBrainz
        print("   Testing MusicBrainz...")
        mb_data = collector.collect_musicbrainz_popular_releases(limit=3)
        if mb_data:
            print(f"   ✅ MusicBrainz: {len(mb_data)} entries")
        else:
            print("   ⚠️ MusicBrainz: No data returned")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Chart collector failed: {e}")
        return False

def test_youtube_collector():
    """Test YouTube data collection"""
    print("\n📺 Testing YouTube Data Collector...")
    
    youtube_key = os.getenv('YOUTUBE_API_KEY')
    if not youtube_key or youtube_key == 'your_youtube_api_key_here':
        print("   ⏭️ Skipped (no API key configured)")
        return True
    
    try:
        from youtube_data_collector import YouTubeDataCollector
        
        collector = YouTubeDataCollector()
        
        # Test search
        print("   Testing video search...")
        video_ids = collector.search_music_videos("pop music", max_results=3)
        
        if video_ids:
            print(f"   ✅ Search: Found {len(video_ids)} videos")
            
            # Test video details
            print("   Testing video details...")
            video_data = collector.get_video_details(video_ids[:2])
            
            if video_data:
                print(f"   ✅ Details: {len(video_data)} videos processed")
                sample = video_data[0]
                print(f"   Sample: {sample.title} ({sample.view_count:,} views)")
                return True
            else:
                print("   ❌ No video details retrieved")
                return False
        else:
            print("   ❌ No videos found in search")
            return False
            
    except Exception as e:
        print(f"   ❌ YouTube collector failed: {e}")
        return False

def test_data_cleaning():
    """Test data cleaning pipeline"""
    print("\n🧹 Testing Data Cleaning Pipeline...")
    
    try:
        from data_cleaning_pipeline import MusicDataCleaner
        
        cleaner = MusicDataCleaner()
        
        # Check if we have data to clean
        conn = sqlite3.connect("music_data.db")
        cursor = conn.cursor()
        
        # Check chart data
        cursor.execute("SELECT COUNT(*) FROM chart_data")
        chart_count = cursor.fetchone()[0]
        
        # Check YouTube data
        try:
            cursor.execute("SELECT COUNT(*) FROM youtube_videos")
            youtube_count = cursor.fetchone()[0]
        except:
            youtube_count = 0
        
        conn.close()
        
        print(f"   Data available: {chart_count} chart records, {youtube_count} YouTube records")
        
        if chart_count == 0 and youtube_count == 0:
            print("   ⚠️ No data to clean. Run data collection first.")
            return True
        
        # Test cleaning
        print("   Testing data cleaning...")
        unified_df = cleaner.create_unified_dataset()
        
        if not unified_df.empty:
            print(f"   ✅ Cleaning successful: {len(unified_df)} unified records")
            
            # Show sample
            sample = unified_df.head(1).iloc[0]
            print(f"   Sample: {sample['track_name']} by {sample['artist_name']} ({sample['source']})")
            return True
        else:
            print("   ❌ Cleaning produced no results")
            return False
            
    except Exception as e:
        print(f"   ❌ Data cleaning failed: {e}")
        return False

def test_database():
    """Test database operations"""
    print("\n🗄️ Testing Database...")
    
    try:
        conn = sqlite3.connect("music_data.db")
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"   Tables found: {', '.join(tables)}")
        
        # Check data counts
        for table in tables:
            if table != 'sqlite_sequence':
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {table}: {count} records")
        
        conn.close()
        print("   ✅ Database operations successful")
        return True
        
    except Exception as e:
        print(f"   ❌ Database test failed: {e}")
        return False

def test_full_pipeline():
    """Test the complete pipeline"""
    print("\n🚀 Testing Full Pipeline...")
    
    try:
        from main_data_pipeline import MusicDataPipeline
        
        pipeline = MusicDataPipeline()
        
        # Test individual components
        print("   Testing chart collection...")
        chart_success = pipeline.collect_chart_data()
        
        print("   Testing YouTube collection...")
        youtube_success = pipeline.collect_youtube_data()
        
        print("   Testing data processing...")
        unified_df = pipeline.clean_and_process_data()
        processing_success = not unified_df.empty
        
        if processing_success:
            report = pipeline.generate_report(unified_df)
            print(f"   ✅ Pipeline successful: {report['total_records']} total records")
            return True
        else:
            print("   ❌ Pipeline failed: No data processed")
            return False
            
    except Exception as e:
        print(f"   ❌ Full pipeline test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Music Prediction Platform - Component Tests")
    print("=" * 60)
    
    tests = [
        ("Database Operations", test_database),
        ("Chart Data Collector", test_chart_collector),
        ("YouTube Data Collector", test_youtube_collector),
        ("Data Cleaning Pipeline", test_data_cleaning),
        ("Full Pipeline", test_full_pipeline)
    ]
    
    results = {}
    
    for test_name, test_function in tests:
        try:
            success = test_function()
            results[test_name] = success
        except Exception as e:
            print(f"\n❌ {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🏁 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your setup is working correctly.")
        print("\n🚀 You can now run the full pipeline:")
        print("   python main_data_pipeline.py")
    else:
        print("⚠️ Some tests failed. Please check the errors above.")
        print("\n🔧 Common fixes:")
        print("   - Make sure you've run: python setup.py")
        print("   - Check your .env file for correct API keys")
        print("   - Ensure all dependencies are installed")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)