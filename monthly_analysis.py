import json
import os
import argparse
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

class SpotifyAnalyzer:
    def __init__(self):
        # monthly_data[month]["songs"][(lower_t, lower_a)] = total_ms_played
        self.monthly_data = defaultdict(lambda: {"songs": Counter(), "artists": Counter()})
        # Track max duration and display names globally
        # stats[(lower_t, lower_a)] = {"max_ms": ms, "track_name": "Name", "artist_name": "Artist"}
        self.global_track_stats = defaultdict(lambda: {"max_ms": 1, "track_name": "", "artist_name": ""})
        self.processed_ids = set()
        self.stats = {"processed": 0, "skipped": 0, "duplicates": 0}

    def _normalize(self, text):
        return str(text).strip() if text else ""

    def process_data(self, records):
        """Processes a list of Spotify history records."""
        if not isinstance(records, list):
            self.stats["skipped"] += 1
            return

        for record in records:
            if not isinstance(record, dict):
                self.stats["skipped"] += 1
                continue

            ts = record.get("ts")
            ms_played = record.get("ms_played", 0)
            if ms_played is None: ms_played = 0
            
            # Primary fields for Music
            track = self._normalize(record.get("master_metadata_track_name"))
            artist = self._normalize(record.get("master_metadata_album_artist_name"))
            
            # Fallback for Podcasts/Videos
            if not track:
                track = self._normalize(record.get("episode_name"))
            if not artist:
                artist = self._normalize(record.get("episode_show_name"))
            
            if not ts or not track or not artist:
                self.stats["skipped"] += 1
                continue

            # De-duplication
            record_id = (ts, ms_played, track.lower(), artist.lower())
            if record_id in self.processed_ids:
                self.stats["duplicates"] += 1
                continue
            self.processed_ids.add(record_id)
                
            try:
                month_key = ts[:7] # YYYY-MM
                datetime.strptime(month_key, "%Y-%m")
            except (ValueError, TypeError):
                self.stats["skipped"] += 1
                continue
            
            t_low, a_low = track.lower(), artist.lower()
            track_id = (t_low, a_low)
            
            # Global tracking (Always use Max MS regardless of skip flag for robustness)
            self.global_track_stats[track_id]["max_ms"] = max(self.global_track_stats[track_id]["max_ms"], ms_played)
            # Store first encountered display name (or could update to keep latest/most frequent)
            if not self.global_track_stats[track_id]["track_name"]:
                self.global_track_stats[track_id]["track_name"] = track
                self.global_track_stats[track_id]["artist_name"] = artist
            
            # Increment monthly totals
            self.monthly_data[month_key]["songs"][track_id] += ms_played
            self.stats["processed"] += 1

    def get_report(self, top_n=10):
        """returns a sorted dictionary of monthly summaries ranked by FLE."""
        report = {}
        for month in sorted(self.monthly_data.keys()):
            data = self.monthly_data[month]
            
            # Calculate FLE for all songs in this month
            song_metrics = [] # List of (track_id, fle_score)
            month_artist_scores = Counter() # lower_a -> sum(fle)
            
            for track_id, total_ms in data["songs"].items():
                max_ms = self.global_track_stats[track_id]["max_ms"]
                fle_score = total_ms / max_ms if max_ms > 0 else 0
                song_metrics.append((track_id, fle_score))
                
                # Assign FLE to the artist
                month_artist_scores[track_id[1]] += fle_score
                
            # Rank Songs
            top_songs = sorted(song_metrics, key=lambda x: x[1], reverse=True)[:top_n]
            # Rank Artists
            top_artists = month_artist_scores.most_common(top_n)
            
            report[month] = {
                "songs": [
                    {
                        "name": self.global_track_stats[tid]["track_name"],
                        "artist": self.global_track_stats[tid]["artist_name"],
                        "score": round(score, 2)
                    } for tid, score in top_songs
                ],
                "artists": [
                    {
                        "name": self.global_track_stats[(None, aid)]["artist_name"] if (None, aid) in self.global_track_stats else self._find_artist_display(aid),
                        "score": round(score, 2)
                    } for aid, score in top_artists
                ]
            }
        return report

    def _find_artist_display(self, lower_artist):
        # Fallback to find a display name for an artist from the global tracks
        for tid, stats in self.global_track_stats.items():
            if tid[1] == lower_artist:
                return stats["artist_name"]
        return lower_artist

def load_and_analyze(data_dir, top_n=10):
    analyzer = SpotifyAnalyzer()
    path = Path(data_dir)
    
    json_files = sorted(list(path.glob("Streaming_History_*.json")))
    if not json_files:
        print(f"No streaming history files found in {data_dir}")
        return {}

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                analyzer.process_data(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
            
    print(f"Processing complete: {analyzer.stats['processed']} records loaded.")
    print(f"Stats: {analyzer.stats['duplicates']} duplicates skipped, {analyzer.stats['skipped']} malformed records skipped.")
    return analyzer.get_report(top_n=top_n)

def print_report(report, output_file=None):
    output_str = ""
    for month, data in report.items():
        month_section = f"\n=== {month} ===\n"
        month_section += "Top Artists (by Full Listen Equivalents):\n"
        for i, artist in enumerate(data["artists"], 1):
            month_section += f"  {i}. {artist['name']} ({artist['score']} FLE)\n"
            
        month_section += "\nTop Songs (by Full Listen Equivalents):\n"
        for i, song in enumerate(data["songs"], 1):
            month_section += f"  {i}. {song['name']} by {song['artist']} ({song['score']} FLE)\n"
        
        output_str += month_section

    print(output_str)
    
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(output_str)
            print(f"\n[v] Report saved to {output_file}")
        except IOError as e:
            print(f"\n[x] Error saving report to {output_file}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spotify Monthly Analysis Tool (FLE Edition)")
    parser.add_argument("--dir", default="my_spotify_data/Spotify Extended Streaming History", help="Directory containing Spotify JSON files")
    parser.add_argument("--top", type=int, default=10, help="Number of top items to show per month")
    parser.add_argument("--output", help="Path to save the report")
    
    args = parser.parse_args()
    
    report = load_and_analyze(args.dir, top_n=args.top)
    print_report(report, output_file=args.output)
