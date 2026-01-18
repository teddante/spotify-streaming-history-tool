import json
import os
from datetime import datetime, timedelta
from analyzer import SpotifyAnalyzer

def generate_mock_data():
    """Generates two distinct eras of listening history."""
    data = []
    base_time = datetime(2023, 1, 1)
    
    # Era 1: The Beatles (Jan - March)
    for i in range(100):
        ts = base_time + timedelta(days=i/2)
        data.append({
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ms_played": 180000,
            "master_metadata_track_name": f"Song {i}",
            "master_metadata_album_artist_name": "The Beatles",
            "skipped": False
        })
        
    # Era 2: Nirvana (April - June)
    base_time_2 = datetime(2023, 4, 1)
    for i in range(100):
        ts = base_time_2 + timedelta(days=i/2)
        data.append({
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ms_played": 180000,
            "master_metadata_track_name": f"Nirvana Song {i}",
            "master_metadata_album_artist_name": "Nirvana",
            "skipped": False
        })
        
    os.makedirs("test_data", exist_ok=True)
    with open("test_data/Streaming_History_Audio_Mock.json", "w") as f:
        json.dump(data, f)

def test_stage_detection():
    print("Running Adversarial Test: Stage Transition Detection...")
    generate_mock_data()
    
    analyzer = SpotifyAnalyzer("test_data")
    stages = analyzer.run_analysis(window_size_days=30)
    
    # Assertions
    assert len(stages) >= 2, f"Expected at least 2 stages, found {len(stages)}"
    
    beatles_stage = any("The Beatles" in s['top_artists'] for s in stages)
    nirvana_stage = any("Nirvana" in s['top_artists'] for s in stages)
    
    assert beatles_stage, "Failed to identify Beatles era"
    assert nirvana_stage, "Failed to identify Nirvana era"
    
    print("✅ TEST PASSED: Algorithm successfully distinguished between distinct musical eras.")

if __name__ == "__main__":
    try:
        test_stage_detection()
    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        exit(1)
