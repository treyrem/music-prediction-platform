"""
Project Setup Script
Sets up the music prediction platform environment and validates configuration
"""

import os
import sys
import subprocess
import sqlite3
from pathlib import Path

def create_project_structure():
    """Create necessary project directories"""
    directories = [
        'data',
        'logs', 
        'reports',
        'scripts',
        'config',
        'tests'
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Created directory: {directory}")

def install_dependencies():
    """Install required Python packages"""
    print("📦 Installing dependencies...")
    
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def create_env_file():
    """Create .env file if it doesn't exist"""
    env_file = Path(".env")
    
    if env_file.exists():
        print("✅ .env file already exists")
        return True
    
    env_template = """# Music Prediction Platform Environment Variables

# Database Configuration
DATABASE_URL=sqlite:///music_data.db

# API Keys (Get these from respective platforms)
YOUTUBE_API_KEY=your_youtube_api_key_here
LASTFM_API_KEY=your_lastfm_api_key_here

# Optional: Spotify API (for future integration)
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Data Collection Settings
COLLECTION_FREQUENCY=daily
MAX_RECORDS_PER_SOURCE=1000
LOG_LEVEL=INFO

# Rate Limiting (requests per minute)
YOUTUBE_RATE_LIMIT=60
LASTFM_RATE_LIMIT=200
"""
    
    with open(".env", "w") as f:
        f.write(env_template)
    
    print("✅ Created .env file template")
    print("⚠️ Remember to add your actual API keys to .env file")
    return True

def test_database_connection():
    """Test SQLite database connection"""
    try:
        conn = sqlite3.connect("music_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        print("✅ Database connection successful")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_imports():
    """Test if all required modules can be imported"""
    required_modules = [
        'pandas',
        'numpy', 
        'requests',
        'beautifulsoup4',
        'python-dotenv',
        'sqlalchemy'
    ]
    
    failed_imports = []
    
    for module in required_modules:
        try:
            if module == 'beautifulsoup4':
                import bs4
            elif module == 'python-dotenv':
                import dotenv
            else:
                __import__(module)
            print(f"✅ {module}")
        except ImportError:
            print(f"❌ {module}")
            failed_imports.append(module)
    
    if failed_imports:
        print(f"\n❌ Failed to import: {', '.join(failed_imports)}")
        print("Run: pip install -r requirements.txt")
        return False
    
    print("✅ All required modules available")
    return True

def validate_api_setup():
    """Check API key setup"""
    from dotenv import load_dotenv
    load_dotenv()
    
    api_keys = {
        'YOUTUBE_API_KEY': os.getenv('YOUTUBE_API_KEY'),
        'LASTFM_API_KEY': os.getenv('LASTFM_API_KEY')
    }
    
    print("\n🔑 API Key Status:")
    any_configured = False
    
    for key_name, key_value in api_keys.items():
        if key_value and key_value != f"your_{key_name.lower()}_here":
            print(f"✅ {key_name}: Configured")
            any_configured = True
        else:
            print(f"⚠️ {key_name}: Not configured")
    
    if not any_configured:
        print("\n⚠️ No API keys configured. You can still run chart collection without APIs.")
        print("📖 Setup guides:")
        print("   YouTube API: https://console.cloud.google.com/")
        print("   Last.fm API: https://www.last.fm/api/account/create")
    
    return True

def run_basic_test():
    """Run a basic functionality test"""
    print("\n🧪 Running basic functionality test...")
    
    try:
        # Test chart collector (no API key needed)
        from chart_data_collector import ChartDataCollector
        
        collector = ChartDataCollector()
        print("✅ Chart collector initialized")
        
        # Test iTunes collection (should work without API key)
        entries = collector.collect_itunes_top_charts("us", limit=5)
        if entries:
            print(f"✅ iTunes chart collection successful ({len(entries)} entries)")
        else:
            print("⚠️ iTunes chart collection returned no results")
        
        print("✅ Basic test passed")
        return True
        
    except Exception as e:
        print(f"❌ Basic test failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🎵 Music Prediction Platform Setup")
    print("=" * 50)
    
    steps = [
        ("Creating project structure", create_project_structure),
        ("Installing dependencies", install_dependencies),
        ("Creating environment file", create_env_file),
        ("Testing database connection", test_database_connection),
        ("Testing module imports", test_imports),
        ("Validating API setup", validate_api_setup),
        ("Running basic test", run_basic_test)
    ]
    
    failed_steps = []
    
    for step_name, step_function in steps:
        print(f"\n📋 {step_name}...")
        try:
            success = step_function()
            if not success:
                failed_steps.append(step_name)
        except Exception as e:
            print(f"❌ {step_name} failed: {e}")
            failed_steps.append(step_name)
    
    print("\n" + "=" * 50)
    print("🏁 Setup Complete")
    print("=" * 50)
    
    if failed_steps:
        print(f"⚠️ {len(failed_steps)} step(s) failed:")
        for step in failed_steps:
            print(f"   - {step}")
        print("\n📖 Please resolve these issues before running the pipeline")
    else:
        print("✅ All setup steps completed successfully!")
        print("\n🚀 Next steps:")
        print("1. Add your API keys to .env file (optional)")
        print("2. Run: python main_data_pipeline.py")
        print("3. Check the 'reports' folder for results")
    
    return len(failed_steps) == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)