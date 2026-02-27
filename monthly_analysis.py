import json
import os
import argparse
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime
import math
import re

class SpotifyAnalyzer:
    def __init__(self):
        self.monthly_data = defaultdict(lambda: {"songs": Counter(), "artists": Counter()})
        self.yearly_data = defaultdict(lambda: {"songs": Counter(), "artists": Counter()})
        self.alltime_data = {"songs": Counter(), "artists": Counter()}
        
        self.duration_index = defaultdict(Counter)
        # stats[track_id] = {"ref_ms": ms, "trust": 0.0, "name": "", "artist": ""}
        self.global_track_stats = defaultdict(lambda: {"ref_ms": 1, "trust": 0.0, "track_name": "", "artist_name": ""})
        
        # New Intelligence Structures
        self.canon_track_map = {} # (low_t, low_a) -> (canonical_low_t, low_a)
        self.monthly_entropy = {} # month -> shannon_entropy
        self.circadian_stats = defaultdict(lambda: Counter()) # lower_artist -> hour (0-23)
        self.artist_monthly_presence = defaultdict(set) # lower_artist -> set of months
        
        self.all_records = []
        self.processed_ids = set()
        self.stats = {"processed": 0, "skipped": 0, "duplicates": 0, "repaired": 0, "fused": 0, "canonized": 0}

    def _normalize(self, text):
        return str(text).strip() if text else ""
    
    def _canonicalize_track(self, track_name):
        """Removes common suffixes like - Remaster, (Live), etc. to group variants."""
        if not track_name: return ""
        # 1. Remove everything after " - " if it looks like a version suffix
        clean = re.split(r' - \d{4} Remaster| - Remaster| - Live| - Mono| - Deluxe', track_name, flags=re.IGNORECASE)[0]
        # 2. Remove parenthetical versions
        clean = re.split(r' \((Remastered|Live|Mono|Deluxe|Radio Edit|Bonus Track)\)', clean, flags=re.IGNORECASE)[0]
        return clean.strip()

    def _print_progress(self, current, total, prefix="Progress"):
        percent = (current / total) * 100
        bar = "#" * int(percent // 2)
        print(f"\r{prefix}: [{bar:<50}] {percent:.1f}%", end="", flush=True)
        if current >= total:
            print()

    def collect_records(self, records):
        """Pass 1 Pre-step: Just collect all records for global analysis with canonical mapping."""
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
                
                raw_track_id = (track.lower(), artist.lower())
                
                # Semantic Consolidation: Map variant to canonical version
                canon_track = self._canonicalize_track(track)
                canon_track_id = (canon_track.lower(), artist.lower())
                
                if canon_track_id != raw_track_id:
                    self.stats["canonized"] += 1
                
                self.canon_track_map[raw_track_id] = canon_track_id
                
                # Internal de-duplication
                record_id = (ts_str, ms_played, raw_track_id)
                if record_id in self.processed_ids:
                    self.stats["duplicates"] += 1
                    continue
                self.processed_ids.add(record_id)
                
                # Store display name for canonical ID on first encounter
                if not self.global_track_stats[canon_track_id]["track_name"]:
                    self.global_track_stats[canon_track_id]["track_name"] = canon_track
                    self.global_track_stats[canon_track_id]["artist_name"] = artist

                self.all_records.append({
                    "ts": ts,
                    "ts_str": ts_str,
                    "ms_played": ms_played,
                    "raw_track_id": raw_track_id, 
                    "track_id": canon_track_id, # We Use Canonical ID for everything now
                    "hour": ts.hour
                })
                
                # Early index build for durations (uses canonical ID!)
                if ms_played > 5000:
                    self.duration_index[canon_track_id][ms_played] += 1
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

    def _calculate_entropy(self, data_counter):
        """Calculates Shannon Entropy (Diversity) of a artist/song distribution."""
        total = sum(data_counter.values())
        if total == 0: return 0
        entropy = 0
        for count in data_counter.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log2(p)
        return entropy

    def process_data(self):
        """Pass 2: Global Chronological Processing with Intelligence Metrics."""
        if not self.all_records: return
        
        print("Sorting records chronologically...")
        self.all_records.sort(key=lambda x: x["ts"])
        
        prev_record = None
        total = len(self.all_records)
        
        for i, curr in enumerate(self.all_records):
            if i % 1000 == 0:
                self._print_progress(i, total, prefix="Applying Physics & IQ")
            
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

            # --- PHASE C: SCORE CALCULATION & INTELLIGENCE ---
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
            
            # Intelligence: Circadian & Presence
            self.circadian_stats[artist_low][curr["hour"]] += record_fle
            self.artist_monthly_presence[artist_low].add(month_key)
            
            self.stats["processed"] += 1
            prev_record = curr
        
        self._print_progress(total, total, prefix="Applying Physics & IQ")
        self._finalize_intelligence()

    def _calculate_kl_divergence(self, p_counts, q_counts):
        """Relative Entropy between two multinomial distributions (artists)."""
        p_total = sum(p_counts.values())
        q_total = sum(q_counts.values())
        if p_total == 0 or q_total == 0: return 0
        
        # Merge all keys
        all_keys = set(p_counts.keys()) | set(q_counts.keys())
        divergence = 0
        for k in all_keys:
            # Use small epsilon for smoothing
            p = (p_counts.get(k, 0) + 0.01) / p_total
            q = (q_counts.get(k, 0) + 0.01) / q_total
            divergence += p * math.log2(p / q)
        return divergence

    def _finalize_intelligence(self):
        """Post-processing: Entropy and Life Eras."""
        # 1. Diversity (Entropy)
        for month, data in self.monthly_data.items():
            self.monthly_entropy[month] = self._calculate_entropy(data["songs"])
            
        # 2. Automated Era Detection
        sorted_months = sorted(self.monthly_data.keys())
        if not sorted_months: return
        
        self.eras = []
        current_era_start = sorted_months[0]
        current_era_distribution = Counter()
        
        # Sensitivity threshold for Era change
        # Higher = fewer, longer eras. Lower = many small eras.
        THRESHOLD = 1.3 
        
        for i in range(len(sorted_months)):
            m = sorted_months[i]
            m_dist = self.monthly_data[m]["artists"]
            
            if i == 0:
                current_era_distribution.update(m_dist)
                continue
                
            # If shift from previous month is too high, start a new era
            # We compare the current month vs the average of the PREVIOUS era
            shift = self._calculate_kl_divergence(m_dist, current_era_distribution)
            
            if shift > THRESHOLD:
                # Close current era
                self.eras.append({
                    "start": current_era_start,
                    "end": sorted_months[i-1],
                    "top_artist": current_era_distribution.most_common(1)[0][0] if current_era_distribution else "Unknown"
                })
                current_era_start = m
                current_era_distribution = Counter(m_dist)
            else:
                # Merge into existing era
                current_era_distribution.update(m_dist)
        
        # Close the final era
        self.eras.append({
            "start": current_era_start,
            "end": sorted_months[-1],
            "top_artist": current_era_distribution.most_common(1)[0][0] if current_era_distribution else "Unknown"
        })

    def get_report(self, top_n=10):
        """Build the final hierarchical report including Intelligence Layer."""
        report = {
            "stats": self.stats,
            "intelligence": {
                "eras": self.eras,
                "entropy": self.monthly_entropy
            },
            "monthly": {},
            "yearly": {},
            "alltime": self._calculate_fle_rankings(self.alltime_data, top_n)
        }
        
        # Add Behavioral Tags to All-Time Artists
        total_months = len(self.monthly_data)
        for artist in report["alltime"]["artists"]:
            low_name = artist["name"].lower()
            presence = len(self.artist_monthly_presence.get(low_name, []))
            artist["loyalty_score"] = round(presence / total_months, 2) if total_months > 0 else 0
            
            # Peak month FLE / Total FLE
            peak_fle = 0
            for m in self.monthly_data.values():
                peak_fle = max(peak_fle, m["artists"].get(low_name, 0))
            artist["binge_index"] = round(peak_fle / artist["score"], 2) if artist["score"] > 0 else 0

        for month, data in sorted(self.monthly_data.items()):
            report["monthly"][month] = self._calculate_fle_rankings(data, top_n)
            report["monthly"][month]["entropy"] = round(self.monthly_entropy.get(month, 0), 2)
            
        for year, data in sorted(self.yearly_data.items()):
            report["yearly"][year] = self._calculate_fle_rankings(data, top_n)
            
        return report

    def _calculate_fle_rankings(self, context_data, top_n):
        """Helper to format pre-calculated FLE scores using canonical IDs."""
        artists = []
        for low_artist, score in context_data["artists"].items():
            display_name = self._find_artist_display(low_artist)
            artists.append({"name": display_name, "score": round(score, 2)})
            
        songs = []
        for track_id, score in context_data["songs"].items():
            s = self.global_track_stats[track_id]
            songs.append({
                "name": s["track_name"],
                "artist": s["artist_name"],
                "score": round(score, 2)
            })
            
        return {
            "artists": sorted(artists, key=lambda x: x["score"], reverse=True)[:top_n],
            "songs": sorted(songs, key=lambda x: x["score"], reverse=True)[:top_n]
        }

    def _find_artist_display(self, lower_artist):
        # Fallback to find a display name for an artist from the global tracks
        for tid, stats in self.global_track_stats.items():
            if tid[1] == lower_artist:
                return stats["artist_name"]
        return lower_artist

def generate_dashboard(report, output_file):
    """Generates a premium, interactive HTML dashboard using Vanilla CSS/JS."""
    era_html = "".join([
        f'<div class="era"><strong>{era["start"]} - {era["end"]}</strong><br><span class="era-tagline">The {era["top_artist"]} Era</span></div>'
        for era in report["intelligence"]["eras"]
    ])
    
    # All-time artists with behavioral tags
    artist_rows = "".join([
        f'<tr><td>{a["name"]}</td><td>{a["score"]}</td><td>{int(a["loyalty_score"]*100)}%</td><td class="binge-col">{int(a["binge_index"]*100)}%</td></tr>'
        for a in report["alltime"]["artists"]
    ])

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Spotify Intelligence Dashboard</title>
        <style>
            :root {{ --bg: #0f172a; --card: rgba(30, 41, 59, 0.7); --text: #f1f5f9; --accent: #38bdf8; }}
            body {{ font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); line-height: 1.6; margin: 0; padding: 40px; }}
            h1, h2 {{ font-family: 'Georgia', serif; color: var(--accent); }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
            .card {{ background: var(--card); backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.1); padding: 20px; border-radius: 12px; }}
            .era {{ border-left: 4px solid var(--accent); padding-left: 15px; margin-bottom: 20px; }}
            .era-tagline {{ font-size: 0.9em; opacity: 0.7; font-style: italic; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th, td {{ text-align: left; padding: 12px; border-bottom: 1px solid rgba(255,255,255,0.05); }}
            th {{ font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.1em; opacity: 0.6; }}
            .binge-col {{ color: #fb7185; }}
            .entropy-bar {{ height: 8px; background: #334155; border-radius: 4px; overflow: hidden; margin-top: 5px; }}
            .entropy-fill {{ height: 100%; background: var(--accent); }}
        </style>
    </head>
    <body>
        <header>
            <h1>Musical Intelligence Engine</h1>
            <p>A narrative history of listening intent & life stages.</p>
        </header>

        <section>
            <h2>Chronological Eras</h2>
            <div class="grid">
                <div class="card">{era_html}</div>
            </div>
        </section>

        <section style="margin-top: 40px;">
            <h2>Top Artists & Behavioral DNA</h2>
            <div class="card">
                <table>
                    <thead>
                        <tr><th>Artist</th><th>FLE Score</th><th>Loyalty</th><th>Binge Factor</th></tr>
                    </thead>
                    <tbody>
                        {artist_rows}
                    </tbody>
                </table>
            </div>
        </section>
        
        <script>
            console.log("Intelligence Dashboard Loaded.");
        </script>
    </body>
    </html>
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"[v] HTML Dashboard generated at {output_file}")
    except Exception as e:
        print(f"[x] Failed to generate dashboard: {e}")

def load_and_analyze(data_dir, top_n=10):
    analyzer = SpotifyAnalyzer()
    path = Path(data_dir)
    
    json_files = sorted(list(path.glob("Streaming_History_*.json")))
    if not json_files:
        print(f"No streaming history files found in {data_dir}")
        return {}

    print(f"Pass 1: Collecting records and analyzing durations...")
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                analyzer.collect_records(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading {file_path}: {e}")
    
    analyzer.finalize_durations()

    print(f"Pass 2: Chronological processing (Gap analysis & Repair)...")
    analyzer.process_data()
            
    print(f"Processing complete: {analyzer.stats['processed']} valid listens processed.")
    print(f"Stats: {analyzer.stats['duplicates']} duplicates skipped, {analyzer.stats['fused']} fragments fused.")
    print(f"Heuristics: {analyzer.stats['repaired']} negative glitches repaired via Wall-Clock Evidence.")
    print(f"Semantic: {analyzer.stats['canonized']} variants grouped via Semantic Consolidation.")
    return analyzer.get_report(top_n=top_n)

def _format_section(title, data):
    """Format a single section with Intelligence metadata."""
    section = f"\n=== {title} ===\n"
    if "entropy" in data:
        div_label = "Exploration" if data["entropy"] > 6 else "Obsessed"
        section += f"Diversity Score: {data['entropy']} bits ({div_label})\n"
        
    section += "Top Artists:\n"
    for i, artist in enumerate(data["artists"], 1):
        tags = []
        if artist.get("loyalty_score", 0) > 0.7: tags.append("Old Friend")
        if artist.get("binge_index", 0) > 0.4: tags.append("The Binge")
        tag_str = f" [{', '.join(tags)}]" if tags else ""
        section += f"  {i}. {artist['name']} ({artist['score']} FLE){tag_str}\n"
    
    section += "\nTop Songs:\n"
    for i, song in enumerate(data["songs"], 1):
        section += f"  {i}. {song['name']} by {song['artist']} ({song['score']} FLE)\n"
    
    return section

def print_report(report, output_file=None):
    output_str = ""
    
    # 1. Narrative Eras
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                    LIFE ERAS (Narrative History)\n"
    output_str += "=" * 60 + "\n"
    for i, era in enumerate(report["intelligence"]["eras"], 1):
        output_str += f"Era {i}: {era['start']} to {era['end']}\n"
        output_str += f"  Defined by: {era['top_artist']}\n\n"

    # 2. Monthly section
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                    MONTHLY TOP LISTS\n"
    output_str += "=" * 60 + "\n"
    for month, data in sorted(report["monthly"].items(), reverse=True):
        output_str += _format_section(month, data)
    
    # 3. Yearly section
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                    YEARLY TOP LISTS\n"
    output_str += "=" * 60 + "\n"
    for year, data in sorted(report["yearly"].items(), reverse=True):
        output_str += _format_section(year, data)
    
    # 4. All-time section
    output_str += "\n" + "=" * 60 + "\n"
    output_str += "                   ALL-TIME TOP LISTS\n"
    output_str += "=" * 60 + "\n"
    output_str += _format_section("All-Time", report["alltime"])

    print(output_str)
    
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(output_str)
            print(f"\n[v] Text report saved to {output_file}")
        except IOError as e:
            print(f"\n[x] Error saving report: {e}")

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
    
    # Generate HTML Dashboard automatically if we have an output path
    if output_file:
        dashboard_file = output_file.with_suffix(".html")
        generate_dashboard(report, dashboard_file)

