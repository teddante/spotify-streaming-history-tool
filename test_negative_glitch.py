import pytest
from monthly_analysis import SpotifyAnalyzer
from datetime import datetime, timedelta

def test_negative_glitch_correction():
    analyzer = SpotifyAnalyzer()
    
    # Track "A" has high historical completion
    # Pass 1: Setup historical data (10 full listens)
    hist_records = [
        {
            "ts": (datetime(2023, 1, 1) + timedelta(minutes=i*5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "master_metadata_track_name": "Song A",
            "master_metadata_album_artist_name": "Artist X",
            "ms_played": 180000 # 3 mins
        } for i in range(10)
    ]
    
    analyzer.collect_records(hist_records)
    analyzer.finalize_durations()
    # Mode should now be 180000
    
    # Pass 2: The Glitch Scenario
    # 1. Song A (Reported 10s) at 10:03:00 (ts is end time)
    # 2. Song B starts at 10:03:00 and ends at 10:06:05 (3m 5s gap)
    
    glitch_records = [
        {
            "ts": "2023-01-01T10:03:00Z",
            "master_metadata_track_name": "Song A",
            "master_metadata_album_artist_name": "Artist X",
            "ms_played": 10000 # Reported 10s, but gap since prev (10:00:00) is 3m
        },
        {
            "ts": "2023-01-01T10:06:05Z",
            "master_metadata_track_name": "Song B",
            "master_metadata_album_artist_name": "Artist Y",
            "ms_played": 180000 
        }
    ]
    
    analyzer.collect_records(glitch_records)
    analyzer.process_data()
    report = analyzer.get_report(top_n=100)
    
    song_a = [s for s in report["alltime"]["songs"] if s["name"] == "Song A"][0]
    
    # Current logic: 10,000 / 180,000 = 0.05 FLE
    print(f"Song A FLE (Current): {song_a['score']}")
    
    # Desired logic: Gap analysis recognizes the 3m window and repairs it to 1.0
    assert song_a['score'] > 0.8, f"Expected Song A to be repaired to ~1.0 FLE, but got {song_a['score']}"

if __name__ == "__main__":
    test_negative_glitch_correction()
