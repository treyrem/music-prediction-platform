Music Prediction Platform - Complete Project Explanation


📋 Project Overview
This is a data collection and preprocessing pipeline for building a music prediction platform. It collects music data from multiple sources, cleans and unifies it, then prepares it for machine learning models that can predict song popularity.
🏗️ Architecture Overview
Data Sources → Collectors → Database → Cleaner → Unified Dataset → Reports
     ↓             ↓          ↓         ↓           ↓            ↓
  iTunes API   Chart        SQLite   Data       Cleaned      JSON
  Last.fm API  Collector             Cleaning   Music Data   Reports
  YouTube API  YouTube               Pipeline
               Collector

📁 File-by-File Breakdown
1. chart_data_collector.py - Music Chart Data Collection
Purpose: Collects trending music data from free chart sources (no paid APIs needed)

Key Classes & Functions:
ChartEntry (Data Class):
python@dataclass
class ChartEntry:
    position: int           # Chart position (1, 2, 3...)
    track_name: str        # Song title
    artist_name: str       # Artist name
    chart_name: str        # Which chart (e.g., "iTunes Top 100 US")
    chart_date: str        # When collected
    additional_data: Dict  # Extra info (genre, URL, etc.)
ChartDataCollector Class:
Core Functions:

collect_itunes_top_charts(country_code, limit)

What it does: Gets current top songs from iTunes charts
How: Uses iTunes RSS API (free, no key needed)
Countries: US, UK, Canada, Australia
Returns: List of ChartEntry objects
Example data: "What I Want" by Morgan Wallen, position 1, iTunes Top 100 US

collect_lastfm_top_tracks(api_key, period, limit)

What it does: Gets trending tracks from Last.fm
How: Uses Last.fm API (free with registration)
Data: Weekly/monthly trending songs with play counts
Returns: Chart entries with listening statistics


collect_musicbrainz_popular_releases(limit)

What it does: Gets recent popular album releases
How: Queries MusicBrainz open database
Note: Sometimes blocked (403 errors) due to rate limiting
Returns: Recent official releases


save_chart_data(chart_entries)

What it does: Saves collected chart data to SQLite database
Database table: chart_data
Process: Converts ChartEntry objects to database rows


collect_all_charts(lastfm_api_key)

What it does: Master function that collects from ALL sources
Process:

iTunes charts from 4 countries (200 songs)
Last.fm trends (50 songs)
MusicBrainz releases (25 albums)


Total: ~275 music entries per run

2. youtube_data_collector.py - YouTube Music Data
Purpose: Collects detailed music video data from YouTube with metrics
Key Classes & Functions:
YouTubeVideoData (Data Class):
python@dataclass
class YouTubeVideoData:
    video_id: str          # YouTube video ID
    title: str             # Video title
    channel_title: str     # Channel/artist name
    published_at: str      # Upload date
    view_count: int        # Total views
    like_count: int        # Likes
    comment_count: int     # Comments
    duration: str          # Video length (ISO format)
    tags: List[str]        # Video tags
    category_id: str       # YouTube category
    thumbnail_url: str     # Thumbnail image
YouTubeDataCollector Class:
Core Functions:

search_music_videos(query, max_results)

What it does: Searches for music videos
How: Uses YouTube Search API with music category filter
Returns: List of video IDs matching the search
Rate limiting: Built-in delays to respect API quotas

get_video_details(video_ids)

What it does: Gets detailed statistics for videos
Process: Batch requests (50 videos at a time)
Data collected: Views, likes, duration, tags, etc.
Error handling: Safely handles missing data


get_trending_music_videos(region_code, max_results)

What it does: Gets currently trending music videos
Regions: US, UK, Canada, Australia
Returns: Top 25-50 trending music videos per region
Saves: Trending positions with dates


collect_trending_music_data(regions)

What it does: Master function for YouTube collection
Process:

Get trending videos from multiple regions
Remove duplicates
Fetch detailed statistics
Save to database


Result: ~68-100 music videos with full metrics

save_video_data(video_data_list)

What it does: Saves video data to database
Tables: youtube_videos and youtube_trending
Safety: Transaction-safe with rollback on errors




3. data_cleaning_pipeline.py - Data Processing & Cleaning
Purpose: Cleans, standardizes, and unifies data from multiple sources
Key Classes & Functions:
MusicDataCleaner Class:
Text Cleaning Functions:

clean_track_title(title)

What it does: Standardizes song titles
Process:

Removes "(Official Music Video)", "[HD]", etc.
Cleans encoding artifacts (â€™ → ')
Removes extra whitespace


Example: "Song Title (Official Video)" → "Song Title"


clean_artist_name(artist)

What it does: Standardizes artist names
Process:

"ft." → "feat."
"Artist - Topic" → "Artist"
Standardizes featuring formats
Example: "Artist ft. Other" → "Artist feat. Other"



Duplicate Detection:

detect_duplicates(df, similarity_threshold=0.85)

What it does: Finds similar songs across sources
How: Uses fuzzy string matching on titles and artists
Algorithm: Compares every song pair using SequenceMatcher
Returns: DataFrame of potential duplicates with similarity scores


merge_duplicate_records(df, duplicates_df)

What it does: Combines duplicate records intelligently
Logic: Keeps record with more complete data
Priority: YouTube data preferred (more detailed metrics)



Data Processing:

clean_chart_data()

What it does: Processes chart data from database
Steps:

Load from chart_data table
Clean titles and artist names
Remove empty records
Parse JSON additional info
Detect and merge duplicates

clean_youtube_data()

What it does: Processes YouTube data
Steps:

Extract artist/track from video titles
Parse duration (PT4M13S → 253 seconds)
Clean numeric fields
Parse tags from JSON




create_unified_dataset()

What it does: Creates master dataset combining all sources
Process:

Clean chart and YouTube data separately
Convert to unified format
Cross-source duplicate detection
Merge overlapping records


Result: Single DataFrame with all music data



Artist/Track Extraction:

extract_artist_track_from_title(title)

What it does: Extracts artist and song from YouTube titles
Patterns it recognizes:

"Artist - Song Title"
"Artist: Song Title"
"Artist "Song Title""


Fallback: Uses channel name as artist

4. main_data_pipeline.py - Pipeline Orchestrator
Purpose: Coordinates the entire data collection and processing workflow
Key Classes & Functions:
MusicDataPipeline Class:
Setup Functions:

__init__(db_path)

What it does: Initializes all collectors and cleaners
Process:

Creates ChartDataCollector
Creates YouTubeDataCollector (if API key available)
Creates MusicDataCleaner
Sets up logging and directories




setup_directories()

What it does: Creates project folder structure
Folders: logs/, reports/, data/



Collection Functions:

collect_chart_data()

What it does: Runs chart data collection
Calls: chart_collector.collect_all_charts()
Result: ~250 chart entries saved to database
collect_youtube_data()

What it does: Runs YouTube data collection
Calls: youtube_collector.collect_trending_music_data()
Result: ~68 YouTube videos with metrics


clean_and_process_data()

What it does: Runs data cleaning and unification
Calls: cleaner.create_unified_dataset()
Result: ~208 unique, clean music records



Reporting Functions:

generate_report(unified_df)

What it does: Creates detailed analytics report
Statistics calculated:

Total records by source
Unique artists and tracks
Top artists by track count
Chart distribution
Data quality metrics (completeness, duplicates)




save_report(report)

What it does: Saves report as JSON file
Location: reports/data_collection_report_YYYYMMDD_HHMMSS.json

Main Workflow:

run_full_pipeline()

What it does: Executes complete workflow
Steps:

Collect chart data
Collect YouTube data
Clean and process
Generate report
Print summary






5. setup.py - Environment Setup
Purpose: Sets up the project environment and validates configuration
Key Functions:

create_project_structure()

Creates folders: data/, logs/, reports/, scripts/, config/, tests/


install_dependencies()

Installs packages from requirements.txt


create_env_file()

Creates .env template with API key placeholders


test_database_connection()

Verifies SQLite database can be created and accessed


test_imports()

Checks all required Python packages are available

validate_api_setup()

Checks if API keys are configured in .env file


run_basic_test()

Tests chart collector with small iTunes request




6. quick_test_script.py - Component Testing
Purpose: Tests each component individually to identify issues
Key Functions:

test_database()

Checks database tables and record counts


test_chart_collector()

Tests iTunes, Last.fm, and MusicBrainz collection


test_youtube_collector()

Tests YouTube search and video details (if API key available)


test_data_cleaning()

Tests data cleaning and unification


test_full_pipeline()

Tests complete pipeline end-to-end




🗄️ Database Schema
Tables Created:
chart_data
sql- id (PRIMARY KEY)
- track_name (TEXT)
- artist_name (TEXT)
- position (INTEGER)
- chart_name (TEXT)
- chart_date (TEXT)
- additional_info (TEXT, JSON)
- created_at (TIMESTAMP)

youtube_videos
sql- id (PRIMARY KEY)
- video_id (TEXT UNIQUE)
- title (TEXT)
- channel_title (TEXT)
- published_at (TEXT)
- view_count (INTEGER)
- like_count (INTEGER)
- comment_count (INTEGER)
- duration (TEXT)
- tags (TEXT, JSON)
- category_id (TEXT)
- thumbnail_url (TEXT)
- collected_at (TIMESTAMP)

youtube_trending
sql- id (PRIMARY KEY)
- video_id (TEXT)
- position (INTEGER)
- trending_date (TEXT)
- region_code (TEXT)
- category (TEXT)
- created_at (TIMESTAMP)

cleaned_music_data
sql- track_name (TEXT)
- artist_name (TEXT)
- source (TEXT: 'chart' or 'youtube')
- chart_name (TEXT)
- chart_position (INTEGER)
- chart_date (TEXT)
- view_count (INTEGER)
- like_count (INTEGER)
- duration_seconds (INTEGER)
- original_title (TEXT)
- original_artist (TEXT)

🔄 Complete Workflow Example
When you run python main_data_pipeline.py:

Initialization

Creates database if not exists
Sets up collectors with API keys
Creates log and report directories


Chart Collection (2-3 minutes)

iTunes US: 50 songs
iTunes UK: 50 songs
iTunes CA: 50 songs
iTunes AU: 50 songs
Last.fm: 50 trending tracks
Total: ~250 chart entries


YouTube Collection (3-5 minutes)

US trending: 25 videos
UK trending: 25 videos
CA trending: 25 videos
AU trending: 25 videos
Get details for ~68 unique videos
Total: ~68 YouTube records
Data Cleaning (30 seconds)

Clean 250 chart records
Clean 68 YouTube records
Remove 105 chart duplicates
Merge 5 cross-source duplicates
Result: 208 unified, clean records


Report Generation

Calculate statistics
Save JSON report
Print summary to console




📊 Final Output
Database: music_data.db

208 unique, cleaned music records
Complete metadata and metrics
Ready for machine learning
