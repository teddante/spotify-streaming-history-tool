import json
from pathlib import Path
from collections import Counter, defaultdict

data_dir = "my_spotify_data/Spotify Extended Streaming History"
target_month = "2026-01"
target_artist = "Death"
target_track_snippet = "Crystal Mountain"

song_data = defaultdict(lambda: {"ms": [], "skips": []})
all_records = []

for p in Path(data_dir).glob("Streaming_History_*.json"):
    with open(p, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for r in data:
            ts = r.get("ts", "")
            if ts.startswith(target_month):
                artist = r.get("master_metadata_album_artist_name") or r.get("episode_show_name")
                track = r.get("master_metadata_track_name") or r.get("episode_name")
                if artist == target_artist:
                    ms = r.get("ms_played", 0)
                    skipped = r.get("skipped", False)
                    song_data[track]["ms"].append(ms)
                    song_data[track]["skips"].append(skipped)
                    all_records.append(r)

print(f"Total records for {target_artist} in {target_month}: {len(all_records)}")
for track, info in song_data.items():
    if target_track_snippet in track:
        total_ms = sum(info["ms"])
        max_ms = max(info["ms"]) if info["ms"] else 0
        verified_ms = max([m for m, s in zip(info["ms"], info["skips"]) if not s], default=0)
        baseline = verified_ms if verified_ms > 0 else max_ms
        fle = total_ms / baseline if baseline > 0 else 0
        print(f"Track: {track}")
        print(f"  Plays: {len(info['ms'])}")
        print(f"  Total MS: {total_ms}")
        print(f"  Max MS: {max_ms}")
        print(f"  Verified Max MS: {verified_ms}")
        print(f"  FLE Calculation: {total_ms} / {baseline} = {fle}")
