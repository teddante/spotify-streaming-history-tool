import json
import os
import shutil
import math
import tempfile
from datetime import datetime, timedelta
from analyzer import SpotifyAnalyzer

class TestContext:
    def __init__(self, name):
        self.name = name
        self.path = os.path.join(tempfile.gettempdir(), f"spotify_test_{name}_{os.getpid()}")
    
    def __enter__(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)
        os.makedirs(self.path)
        return self.path
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

def test_stage_detection():
    print("Testing Stage Transition Detection (Emergent)...")
    with TestContext("standard") as path:
        data = []
        base_time = datetime(2023, 1, 1)
        
        # Era 1: The Beatles
        for i in range(150):
            ts = base_time + timedelta(hours=i)
            data.append({
                "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ms_played": 180000,
                "master_metadata_album_artist_name": "The Beatles",
            })
            
        # Era 2: Nirvana
        base_time_2 = base_time + timedelta(days=20)
        for i in range(150):
            ts = base_time_2 + timedelta(hours=i)
            data.append({
                "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ms_played": 180000,
                "master_metadata_album_artist_name": "Nirvana",
            })
            
        with open(os.path.join(path, "Streaming_History_Audio_Mock.json"), "w") as f:
            json.dump(data, f)
            
        analyzer = SpotifyAnalyzer(path)
        stages = analyzer.run_analysis(bin_size=50)
        
        assert len(stages) >= 2, f"Expected at least 2 stages, found {len(stages)}"
        artists = [s['top_artists'][0] for s in stages]
        assert "The Beatles" in artists
        assert "Nirvana" in artists
        print("✅ Passed: Distinguished distinct musical eras.")

def test_drift_detection():
    print("\nTesting Gradual Drift Detection...")
    with TestContext("drift") as path:
        data = []
        base_time = datetime(2023, 1, 1)
        
        # 400 songs shifting from A to B
        for i in range(400):
            ts = base_time + timedelta(hours=i)
            # Probability of A decreases over time
            prob_a = max(0, 1 - (i / 400))
            artist = "Artist A" if (i/400) < prob_a else "Artist B"
            
            data.append({
                "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ms_played": 180000,
                "master_metadata_album_artist_name": artist,
            })

        with open(os.path.join(path, "Streaming_History_Audio_Drift.json"), "w") as f:
            json.dump(data, f)
            
        analyzer = SpotifyAnalyzer(path)
        stages = analyzer.run_analysis(bin_size=50)
        
        assert len(stages) > 1, "Failed to detect gradual drift in musical taste."
        print(f"✅ Passed: Detected transition in drift ({len(stages)} stages).")

def test_short_track_significance():
    print("\nTesting Short Track Significance...")
    with TestContext("short_tracks") as path:
        data = []
        base_time = datetime(2023, 1, 1)
        
        # 200 plays of 10-second tracks (Used to be filtered out)
        for i in range(200):
            data.append({
                "ts": (base_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ms_played": 10000,
                "master_metadata_album_artist_name": "MicroArtist",
            })
        
        with open(os.path.join(path, "Streaming_History_Audio_Short.json"), "w") as f:
            json.dump(data, f)
            
        analyzer = SpotifyAnalyzer(path)
        stages = analyzer.run_analysis(bin_size=50)
        
        assert len(stages) > 0, "Failed to detect significance of short tracks."
        assert "MicroArtist" in stages[0]['top_artists'], "MicroArtist should be in DNA."
        print("✅ Passed: Short tracks correctly contribute to DNA.")

def test_sparse_data_continuity():
    print("\nTesting Sparse Data Continuity...")
    with TestContext("sparse") as path:
        data = []
        base_time = datetime(2023, 1, 1)
        
        # One play every 45 days for 2 years (Volume-invariant binning handles this)
        for i in range(20):
            data.append({
                "ts": (base_time + timedelta(days=i*45)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ms_played": 200000,
                "master_metadata_album_artist_name": "ConsistentArtist",
            })
            
        with open(os.path.join(path, "Streaming_History_Audio_Sparse.json"), "w") as f:
            json.dump(data, f)
            
        analyzer = SpotifyAnalyzer(path)
        stages = analyzer.run_analysis(bin_size=50)
        
        # Should be one stage because bin size is 50 and we only have 20 items
        assert len(stages) == 1, f"Expected 1 stage for sparse but consistent data, found {len(stages)}"
        print("✅ Passed: Volume-invariant binning maintains sparse continuity.")

def test_no_arbitrary_labels():
    print("\nTesting for absence of arbitrary labels...")
    with TestContext("no_labels") as path:
        data = [{"ts": "2023-01-01T12:00:00Z", "ms_played": 100000, "master_metadata_album_artist_name": "A"}]
        with open(os.path.join(path, "Streaming_History_Audio_Test.json"), "w") as f:
            json.dump(data, f)
            
        analyzer = SpotifyAnalyzer(path)
        stages = analyzer.run_analysis()
        
        for stage in stages:
            assert 'archetype' not in stage, "Found arbitrary 'archetype' key."
            for key, value in stage.items():
                if key in ['start', 'end', 'top_artists']: continue
                assert isinstance(value, (int, float)), f"Key {key} has non-numeric value."
        print("✅ Passed: Output is strictly quantitative.")

def test_weighting_cap():
    print("\nTesting Log-Weighting Balance...")
    with TestContext("weighting") as path:
        data = []
        # 1 long podcast (1 hour)
        data.append({
            "ts": "2023-01-01T10:00:00Z",
            "ms_played": 3600000,
            "master_metadata_album_artist_name": "LongPodcaster"
        })
        # 10 short songs (3 mins each) by Different Artist
        for i in range(10):
            data.append({
                "ts": "2023-01-01T11:00:00Z",
                "ms_played": 180000,
                "master_metadata_album_artist_name": "VarietyArtist"
            })
            
        with open(os.path.join(path, "Streaming_History_Audio_Weight.json"), "w") as f:
            json.dump(data, f)
            
        analyzer = SpotifyAnalyzer(path)
        stages = analyzer.run_analysis()
        
        # log10(3600) + 1 ≈ 4.5
        # 10 * (log10(180) + 1) ≈ 10 * 3.2 ≈ 32
        # VarietyArtist should be top artist despite having much less raw duration
        assert stages[0]['top_artists'][0] == "VarietyArtist"
        print("✅ Passed: Log-weighting prevents long tracks from drowning out variety.")

if __name__ == "__main__":
    try:
        test_stage_detection()
        test_drift_detection()
        test_short_track_significance()
        test_sparse_data_continuity()
        test_no_arbitrary_labels()
        test_weighting_cap()
        print("\n✨ ALL TESTS PASSED! WORKSPACE COMPLIANT.")
    except Exception:
        import traceback
        traceback.print_exc()
        exit(1)
