import json
import os
import argparse
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

class SpotifyAnalyzer:
    def __init__(self):
        # Nested dict: data[year_month]["songs"] = Counter(), data[year_month]["artists"] = Counter()
        self.monthly_data = defaultdict(lambda: {"songs": Counter(), "artists": Counter()})

    def process_data(self, records):
        """Processes a list of Spotify history records."""
        for record in records:
            # Basic validation
            ts = record.get("ts")
            track = record.get("master_metadata_track_name")
            artist = record.get("master_metadata_album_artist_name")
            
            if not ts or not track or not artist:
                continue
                
            # Extract YYYY-MM
            try:
                # Format is usually 2022-12-30T07:19:16Z
                dt = datetime.strptime(ts[:10], "%Y-%m-%d")
                month_key = dt.strftime("%Y-%m")
            except ValueError:
                continue
            
            # Increment counts
            # We use (track, artist) tuple for song identification to handle identical track names across artists
            self.monthly_data[month_key]["songs"][(track, artist)] += 1
            self.monthly_data[month_key]["artists"][artist] += 1

    def get_report(self, top_n=10):
        """returns a sorted dictionary of monthly summaries."""
        report = {}
        for month in sorted(self.monthly_data.keys()):
            data = self.monthly_data[month]
            
            top_songs = []
            for (track, artist), count in data["songs"].most_common(top_n):
                top_songs.append({"name": track, "artist": artist, "count": count})
                
            top_artists = []
            for artist, count in data["artists"].most_common(top_n):
                top_artists.append({"name": artist, "count": count})
                
            report[month] = {
                "songs": top_songs,
                "artists": top_artists
            }
        return report

def load_and_analyze(data_dir, top_n=10):
    analyzer = SpotifyAnalyzer()
    path = Path(data_dir)
    
    # Process each JSON file
    json_files = sorted(list(path.glob("Streaming_History_Audio_*.json")))
    
    if not json_files:
        print(f"No audio streaming history files found in {data_dir}")
        return {}

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                analyzer.process_data(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
            
    return analyzer.get_report(top_n=top_n)

def print_report(report, output_file=None):
    output_str = ""
    for month, data in report.items():
        month_section = f"\n=== {month} ===\n"
        month_section += "Top Artists:\n"
        for i, artist in enumerate(data["artists"], 1):
            month_section += f"  {i}. {artist['name']} ({artist['count']} plays)\n"
            
        month_section += "\nTop Songs:\n"
        for i, song in enumerate(data["songs"], 1):
            month_section += f"  {i}. {song['name']} by {song['artist']} ({song['count']} plays)\n"
        
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
    parser = argparse.ArgumentParser(description="Spotify Monthly Analysis Tool")
    parser.add_argument("--dir", default="my_spotify_data/Spotify Extended Streaming History", help="Directory containing Spotify JSON files")
    parser.add_argument("--top", type=int, default=10, help="Number of top items to show per month")
    parser.add_argument("--output", help="Path to save the report (e.g., report.txt)")
    
    args = parser.parse_args()
    
    report = load_and_analyze(args.dir, top_n=args.top)
    print_report(report, output_file=args.output)
