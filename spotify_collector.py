"""
Spotify Data Collector for Music Popularity Prediction Platform
Collects track metadata, audio features, and chart data from Spotify
"""

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time
import json
from music_prediction_database import MusicPredictionDatabase
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDataCollector:
    """Collects music data from Spotify Web API"""S
    
    def __init__(self, client_id: str, client_secret: str, db_path: str = "music_prediction.db"):
        """Initialize Spotify client and database connection"""
        try:
            # Setup Spotify client
            client_credentials_manager = SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
            self.spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            
            # Setup database
            self.db = MusicPredictionDatabase(db_path)
            
            # Test connection
            self._test_connection()
            
            logger.info("Spotify Data Collector initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spotify client: {e}")
            raise
    
    def _test_connection(self):
        """Test Spotify API connection"""
        try:
            # Try a simple search to test credentials
            result = self.spotify.search(q="test", type="track", limit=1)
            logger.info("Spotify API connection successful")
        except Exception as e:
            logger.error(f"Spotify API connection failed: {e}")
            raise
    
    def get_chart_tracks(self, region: str = "US", limit: int = 50) -> List[Dict]:
        """
        Get trending/popular tracks from Spotify charts
        Note: Spotify doesn't have official charts API, so we use featured playlists
        """
        tracks = []
        
        try:
            # Get featured playlists which often contain popular tracks
            featured_playlists = self.spotify.featured_playlists(
                country=region, 
                limit=10
            )
            
            for playlist in featured_playlists['playlists']['items']:
                if any(keyword in playlist['name'].lower() for keyword in 
                       ['top', 'hits', 'popular', 'viral', 'trending', 'charts']):
                    
                    # Get tracks from this playlist
                    playlist_tracks = self.spotify.playlist_tracks(
                        playlist['id'], 
                        limit=min(limit, 50)
                    )
                    
                    for item in playlist_tracks['items']:
                        if item['track'] and item['track']['id']:
                            track_data = self._extract_track_data(item['track'])
                            track_data['chart_source'] = f"Spotify Featured: {playlist['name']}"
                            track_data['region'] = region
                            tracks.append(track_data)
                    
                    if len(tracks) >= limit:
                        break
            
            # Also get tracks from "Today's Top Hits" type searches
            search_results = self.spotify.search(
                q=f"year:2024 year:2025", 
                type="track", 
                market=region,
                limit=min(limit, 50)
            )
            
            for track in search_results['tracks']['items']:
                if track['popularity'] > 70:  # Only high popularity tracks
                    track_data = self._extract_track_data(track)
                    track_data['chart_source'] = "Spotify Search Popular"
                    track_data['region'] = region
                    tracks.append(track_data)
            
            logger.info(f"Collected {len(tracks)} tracks from Spotify for region {region}")
            return tracks[:limit]  # Return only requested number
            
        except Exception as e:
            logger.error(f"Error getting chart tracks for {region}: {e}")
            return []
    
    def search_tracks(self, query: str, limit: int = 20) -> List[Dict]:
        """Search for specific tracks"""
        try:
            results = self.spotify.search(q=query, type="track", limit=limit)
            tracks = []
            
            for track in results['tracks']['items']:
                track_data = self._extract_track_data(track)
                tracks.append(track_data)
            
            return tracks
            
        except Exception as e:
            logger.error(f"Error searching tracks: {e}")
            return []
    
    def get_new_releases(self, country: str = "US", limit: int = 50) -> List[Dict]:
        """Get new music releases"""
        try:
            results = self.spotify.new_releases(country=country, limit=limit)
            tracks = []
            
            for album in results['albums']['items']:
                # Get tracks from each album
                album_tracks = self.spotify.album_tracks(album['id'])
                
                for track in album_tracks['items']:
                    # Get full track info
                    full_track = self.spotify.track(track['id'])
                    track_data = self._extract_track_data(full_track)
                    track_data['chart_source'] = "Spotify New Releases"
                    track_data['region'] = country
                    tracks.append(track_data)
            
            logger.info(f"Collected {len(tracks)} new releases for {country}")
            return tracks
            
        except Exception as e:
            logger.error(f"Error getting new releases: {e}")
            return []
    
    def _extract_track_data(self, track: Dict) -> Dict:
        """Extract relevant data from Spotify track object"""
        try:
            # Basic track info
            track_data = {
                'spotify_id': track['id'],
                'name': track['name'],
                'popularity': track['popularity'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit'],
                'preview_url': track['preview_url'],
                'external_urls': track['external_urls']
            }
            
            # Album info
            if 'album' in track:
                album = track['album']
                track_data.update({
                    'album_name': album['name'],
                    'release_date': album['release_date'],
                    'album_type': album['album_type']
                })
            
            # Artist info
            if 'artists' in track and track['artists']:
                main_artist = track['artists'][0]
                track_data['artist'] = {
                    'spotify_id': main_artist['id'],
                    'name': main_artist['name'],
                    'external_urls': main_artist['external_urls']
                }
                
                # Get detailed artist info
                try:
                    artist_details = self.spotify.artist(main_artist['id'])
                    track_data['artist'].update({
                        'followers': artist_details['followers']['total'],
                        'popularity': artist_details['popularity'],
                        'genres': artist_details['genres'],
                        'images': artist_details['images']
                    })
                except:
                    # If artist details fail, continue with basic info
                    pass
            
            return track_data
            
        except Exception as e:
            logger.error(f"Error extracting track data: {e}")
            return {}
    
    def get_audio_features(self, track_ids: List[str]) -> Dict[str, Dict]:
        """Get audio features for multiple tracks"""
        audio_features = {}
        
        try:
            # Spotify allows up to 100 tracks per request
            for i in range(0, len(track_ids), 100):
                batch_ids = track_ids[i:i+100]
                features = self.spotify.audio_features(batch_ids)
                
                for feature in features:
                    if feature:  # Some tracks might not have audio features
                        audio_features[feature['id']] = {
                            'danceability': feature['danceability'],
                            'energy': feature['energy'],
                            'key': feature['key'],
                            'loudness': feature['loudness'],
                            'mode': feature['mode'],
                            'speechiness': feature['speechiness'],
                            'acousticness': feature['acousticness'],
                            'instrumentalness': feature['instrumentalness'],
                            'liveness': feature['liveness'],
                            'valence': feature['valence'],
                            'tempo': feature['tempo'],
                            'time_signature': feature['time_signature']
                        }
                
                time.sleep(0.1)  # Rate limiting
            
            logger.info(f"Retrieved audio features for {len(audio_features)} tracks")
            return audio_features
            
        except Exception as e:
            logger.error(f"Error getting audio features: {e}")
            return {}
    
    def save_tracks_to_database(self, tracks: List[Dict]) -> List[int]:
        """Save tracks and their data to database"""
        track_ids = []
        
        for track_data in tracks:
            try:
                # Insert artist first
                if 'artist' in track_data:
                    artist_id = self.db.insert_artist(track_data['artist'])
                    
                    # Insert track
                    track_id = self.db.insert_track(track_data, artist_id)
                    track_ids.append(track_id)
                    
                    # Get and insert audio features
                    audio_features = self.get_audio_features([track_data['spotify_id']])
                    if track_data['spotify_id'] in audio_features:
                        self.db.insert_audio_features(
                            track_id, 
                            audio_features[track_data['spotify_id']]
                        )
                    
                    time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error saving track {track_data.get('name', 'Unknown')}: {e}")
                continue
        
        logger.info(f"Saved {len(track_ids)} tracks to database")
        return track_ids
    
    def collect_comprehensive_data(self, regions: List[str] = None) -> Dict:
        """Collect comprehensive data from multiple sources"""
        if not regions:
            regions = config.collection.spotify_regions
        
        collection_summary = {
            'tracks_collected': 0,
            'artists_collected': 0,
            'regions_processed': 0,
            'collection_time': datetime.now().isoformat()
        }
        
        all_tracks = []
        
        for region in regions:
            logger.info(f"Collecting data for region: {region}")
            
            # Get chart tracks
            chart_tracks = self.get_chart_tracks(region, limit=30)
            all_tracks.extend(chart_tracks)
            
            # Get new releases
            new_releases = self.get_new_releases(region, limit=20)
            all_tracks.extend(new_releases)
            
            collection_summary['regions_processed'] += 1
            time.sleep(1)  # Rate limiting between regions
        
        # Remove duplicates based on Spotify ID
        unique_tracks = {}
        for track in all_tracks:
            spotify_id = track.get('spotify_id')
            if spotify_id and spotify_id not in unique_tracks:
                unique_tracks[spotify_id] = track
        
        unique_tracks_list = list(unique_tracks.values())
        
        # Save to database
        if unique_tracks_list:
            track_ids = self.save_tracks_to_database(unique_tracks_list)
            collection_summary['tracks_collected'] = len(track_ids)
            collection_summary['artists_collected'] = len(set(
                track.get('artist', {}).get('spotify_id') 
                for track in unique_tracks_list 
                if track.get('artist', {}).get('spotify_id')
            ))
        
        logger.info(f"Collection complete: {collection_summary}")
        return collection_summary
    
    def get_track_popularity_over_time(self, spotify_id: str, days: int = 30) -> List[Dict]:
        """
        Track popularity changes over time
        Note: This would require storing historical data
        """
        # For now, just return current popularity
        # In production, you'd track this daily
        try:
            track = self.spotify.track(spotify_id)
            return [{
                'date': datetime.now().strftime('%Y-%m-%d'),
                'popularity': track['popularity']
            }]
        except Exception as e:
            logger.error(f"Error getting track popularity: {e}")
            return []

def main():
    """Main function for testing and running collection"""
    
    # Validate configuration
    if not config.validate_all():
        logger.error("Configuration validation failed")
        return
    
    # Initialize collector
    try:
        collector = SpotifyDataCollector(
            client_id=config.api.spotify_client_id,
            client_secret=config.api.spotify_client_secret,
            db_path=config.database.db_path
        )
        
        # Run comprehensive data collection
        summary = collector.collect_comprehensive_data()
        
        print("\n🎵 Spotify Data Collection Summary:")
        print(f"   Tracks collected: {summary['tracks_collected']}")
        print(f"   Artists collected: {summary['artists_collected']}")
        print(f"   Regions processed: {summary['regions_processed']}")
        print(f"   Collection time: {summary['collection_time']}")
        
        # Get database stats
        stats = collector.db.get_database_stats()
        print("\n📊 Database Statistics:")
        for key, value in stats.items():
            print(f"   {key}: {value}")
            
    except Exception as e:
        logger.error(f"Collection failed: {e}")

if __name__ == "__main__":
    main()
