import json
import os
import argparse
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

class SpotifyAnalyzer:
    def __init__(self):
        self.monthly_data = defaultdict(lambda: {"songs": Counter(), "artists": Counter()})
        self.yearly_data = defaultdict(lambda: {"songs": Counter(), "artists": Counter()})
        self.alltime_data = {"songs": Counter(), "artists": Counter()}
        
        self.duration_index = defaultdict(Counter)
        # stats[track_id] = {"ref_ms": ms, "trust": 0.0, "name": "", "artist": ""}
        self.global_track_stats = defaultdict(lambda: {"ref_ms": 1, "trust": 0.0, "track_name": "", "artist_name": ""})
        
        self.all_records = []
        self.processed_ids = set()
        self.stats = {"processed": 0, "skipped": 0, "duplicates": 0, "repaired": 0, "fused": 0}

    def _normalize(self, text):
        return str(text).strip() if text else ""

    def _print_progress(self, current, total, prefix="Progress"):
        percent = (current / total) * 100
        bar = "#" * int(percent // 2)
        print(f"\r{prefix}: [{bar:<50}] {percent:.1f}%", end="", flush=True)
        if current >= total:
            print()

    def collect_records(self, records):
        """Pass 1 Pre-step: Just collect all records for global analysis."""
        if not isinstance(records, list): return
        for record in records:
            if not isinstance(record, dict): continue
            
            ts_str = record.get("ts")
            if not ts_str: continue
            
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
                track = self._normalize(record.get("master_metadata_track_name") or record.get("episode_name"))
                artist = self._normalize(record.get("master_metadata_album_artist_name") or record.get("episode_show_name"))
                ms_played = record.get("ms_played", 0) or 0
                
                if not track or not artist: continue
                
                track_id = (track.lower(), artist.lower())
                
                # Internal de-duplication
                record_id = (ts_str, ms_played, track_id)
                if record_id in self.processed_ids:
                    self.stats["duplicates"] += 1
                    continue
                self.processed_ids.add(record_id)
                
                # Store display name on first encounter (O(1) later)
                if not self.global_track_stats[track_id]["track_name"]:
                    self.global_track_stats[track_id]["track_name"] = track
                    self.global_track_stats[track_id]["artist_name"] = artist

                self.all_records.append({
                    "ts": ts,
                    "ts_str": ts_str,
                    "ms_played": ms_played,
                    "track_id": track_id
                })
                
                # Early index build for durations
                if ms_played > 5000:
                    self.duration_index[track_id][ms_played] += 1
            except (ValueError, TypeError):
                continue

    def finalize_durations(self):
        """Pass 1 Finish: Calculate reference duration and Bayesian Trust Score."""
        items = list(self.duration_index.items())
        total = len(items)
        for i, (track_id, counts) in enumerate(items):
            if i % 100 == 0:
                self._print_progress(i, total, prefix="Analyzing Stats")
            
            if not counts: continue
            
            most_common = counts.most_common()
            max_freq = most_common[0][1]
            modes = [val for val, freq in most_common if freq == max_freq]
            ref_ms = max(modes)
            
            total_plays = sum(counts.values())
            full_listens = sum(freq for val, freq in counts.items() if val >= 0.8 * ref_ms)
            trust = full_listens / total_plays if total_plays > 0 else 0.0
            
            self.global_track_stats[track_id].update({
                "ref_ms": ref_ms,
                "trust": trust
            })
        self._print_progress(total, total, prefix="Analyzing Stats")

    def process_data(self):
        """Pass 2: Global Chronological Processing with Fragment Fusion and Gap Repair."""
        if not self.all_records: return
        
        print("Sorting records chronologically...")
        self.all_records.sort(key=lambda x: x["ts"])
        
        prev_record = None
        total = len(self.all_records)
        
        for i, curr in enumerate(self.all_records):
            if i % 500 == 0:
                self._print_progress(i, total, prefix="Applying Physics")
            
            track_id = curr["track_id"]
            stats = self.global_track_stats[track_id]
            ref_ms = stats["ref_ms"]
            reported_ms = curr["ms_played"]
            
            # --- PHASE A: FRAGMENT FUSION ---
            if prev_record and prev_record["track_id"] == track_id:
                gap_ms = (curr["ts"] - prev_record["ts"]).total_seconds() * 1000
                if gap_ms < 1000:
                    self.stats["fused"] += 1
                    continue 

            # --- PHASE B: PHYSICAL EVIDENCE (GAP REPAIR) ---
            effective_ms = reported_ms
            
            if i + 1 < len(self.all_records):
                next_rec = self.all_records[i+1]
                gap_ms = (next_rec["ts"] - curr["ts"]).total_seconds() * 1000
                
                if reported_ms < 0.15 * ref_ms and gap_ms > 0.8 * ref_ms and gap_ms < 1.2 * ref_ms:
                    if stats["trust"] > 0.5:
                        effective_ms = ref_ms
                        self.stats["repaired"] += 1

            # --- PHASE C: SCORE CALCULATION ---
            if effective_ms < 5000: continue
            
            record_fle = min(effective_ms / ref_ms, 2.0) if ref_ms > 0 else 0
            
            ts_str = curr["ts_str"]
            month_key, year_key = ts_str[:7], ts_str[:4]
            artist_low = track_id[1]
            
            self.monthly_data[month_key]["songs"][track_id] += record_fle
            self.monthly_data[month_key]["artists"][artist_low] += record_fle
            self.yearly_data[year_key]["songs"][track_id] += record_fle
            self.yearly_data[year_key]["artists"][artist_low] += record_fle
            self.alltime_data["songs"][track_id] += record_fle
            self.alltime_data["artists"][artist_low] += record_fle
            
            self.stats["processed"] += 1
            prev_record = curr
        
        self._print_progress(total, total, prefix="Applying Physics")

    def _calculate_fle_rankings(self, context_data, top_n):
        """Helper to format pre-calculated FLE scores."""
        top_songs = context_data["songs"].most_common(top_n)
        top_artists = context_data["artists"].most_common(top_n)
        
        return {
            "songs": [
                {
                    "name": self.global_track_stats[tid]["track_name"],
                    "artist": self.global_track_stats[tid]["artist_name"],
                    "score": round(score, 2)
                } for tid, score in top_songs
            ],
            "artists": [
                {
                    "name": self._find_artist_display(aid),
                    "score": round(score, 2)
                } for aid, score in top_artists
            ]
        }

    def get_report(self, top_n=10):
        """Returns a dictionary with monthly, yearly, and all-time summaries ranked by FLE."""
        # Monthly report
        monthly_report = {}
        for month in sorted(self.monthly_data.keys()):
            monthly_report[month] = self._calculate_fle_rankings(
                self.monthly_data[month], top_n
            )
        
        # Yearly report
        yearly_report = {}
        for year in sorted(self.yearly_data.keys()):
            yearly_report[year] = self._calculate_fle_rankings(
                self.yearly_data[year], top_n
            )
        
        # All-time report
        alltime_report = self._calculate_fle_rankings(
            self.alltime_data, top_n
        )
        
        return {
            "monthly": monthly_report,
            "yearly": yearly_report,
            "alltime": alltime_report
        }

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

    # Pass 1: Collect all records and build duration count
    print(f"Pass 1: Collecting records and analyzing durations...")
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                analyzer.collect_records(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
    
    analyzer.finalize_durations()

    # Pass 2: Global Chronological Processing
    print(f"Pass 2: Chronological processing (Gap analysis & Repair)...")
    analyzer.process_data()
            
    print(f"Processing complete: {analyzer.stats['processed']} valid listens processed.")
    print(f"Stats: {analyzer.stats['duplicates']} duplicates skipped, {analyzer.stats['fused']} fragments fused.")
    print(f"Heuristics: {analyzer.stats['repaired']} negative glitches repaired via Wall-Clock Evidence.")
    return analyzer.get_report(top_n=top_n)

def _format_section(title, data):
    """Format a single section (artists + songs) for output."""
    section = f"\n=== {title} ===\n"
    section += "Top Artists (by Full Listen Equivalents):\n"
    for i, artist in enumerate(data["artists"], 1):
        section += f"  {i}. {artist['name']} ({artist['score']} FLE)\n"
    
    section += "\nTop Songs (by Full Listen Equivalents):\n"
    for i, song in enumerate(data["songs"], 1):
        section += f"  {i}. {song['name']} by {song['artist']} ({song['score']} FLE)\n"
    
    return section


def print_report(report, output_file=None):
    output_str = ""
    
    # Monthly section
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                    MONTHLY TOP LISTS\n"
    output_str += "=" * 60 + "\n"
    for month, data in report["monthly"].items():
        output_str += _format_section(month, data)
    
    # Yearly section
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                    YEARLY TOP LISTS\n"
    output_str += "=" * 60 + "\n"
    for year, data in report["yearly"].items():
        output_str += _format_section(year, data)
    
    # All-time section
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                   ALL-TIME TOP LISTS\n"
    output_str += "=" * 60 + "\n"
    output_str += _format_section("All-Time", report["alltime"])

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
    parser.add_argument("--output", help="Path to save the report (default: auto-generated timestamp)")
    parser.add_argument("--no-save", action="store_true", help="Don't save to file, only print to console")
    
    args = parser.parse_args()
    
    # Generate default output filename with timestamp if not specified
    if args.no_save:
        output_file = None
    elif args.output:
        output_file = args.output
    else:
        # Create output folder if it doesn't exist
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"report_{timestamp}.txt"
    
    report = load_and_analyze(args.dir, top_n=args.top)
    print_report(report, output_file=output_file)

