import json
from pathlib import Path
from collections import defaultdict

data_dir = "my_spotify_data/Spotify Extended Streaming History"

# Test cases from report
test_cases = [
    ("2017-04", "Breathe (In The Air) - 2011 Remastered Version", "Pink Floyd", 47.61),
    ("2015-03", "You're On (feat. Kyan)", "Madeon", 31.19),
    ("2014-09", "Morning Glory", "Oasis", 9.11),
]

# Global track stats: track_key -> max_ms across ALL time
global_max = defaultdict(int)
# Monthly totals: (month, track_key) -> total_ms
monthly_totals = defaultdict(int)

print("Loading all JSON files...")
for p in Path(data_dir).glob("Streaming_History_*.json"):
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
        for r in data:
            artist = r.get("master_metadata_album_artist_name", "") or ""
            track = r.get("master_metadata_track_name", "") or ""
            ms = r.get("ms_played", 0) or 0
            ts = r.get("ts", "") or ""
            
            if not track or not artist or not ts:
                continue
            
            track_key = (track.lower().strip(), artist.lower().strip())
            month = ts[:7]
            
            global_max[track_key] = max(global_max[track_key], ms)
            monthly_totals[(month, track_key)] += ms

print(f"Loaded {len(global_max)} unique tracks\n")

# Validate test cases
print("=" * 60)
print("VALIDATION RESULTS")
print("=" * 60)

for month, track_name, artist_name, expected_fle in test_cases:
    track_key = (track_name.lower().strip(), artist_name.lower().strip())
    
    total_ms = monthly_totals.get((month, track_key), 0)
    max_ms = global_max.get(track_key, 1)
    calculated_fle = total_ms / max_ms if max_ms > 0 else 0
    
    match = "✓" if abs(calculated_fle - expected_fle) < 0.1 else "✗"
    
    print(f"\n{match} {track_name} by {artist_name} ({month})")
    print(f"  Total MS:     {total_ms:,}")
    print(f"  Global Max:   {max_ms:,}")
    print(f"  Calculated:   {calculated_fle:.2f} FLE")
    print(f"  Report says:  {expected_fle} FLE")
    print(f"  Difference:   {abs(calculated_fle - expected_fle):.2f}")
