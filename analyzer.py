import json
import os
import glob
from datetime import datetime, timedelta
from collections import defaultdict
import math

class SpotifyAnalyzer:
    def __init__(self, data_path):
        self.data_path = data_path
        self.raw_data = []

    def load_data(self):
        """Streams JSON files from the directory and yields cleaned records."""
        # Look for both Extended and Standard history formats
        patterns = [
            os.path.join(self.data_path, "Streaming_History_Audio_*.json"),
            os.path.join(self.data_path, "StreamingHistory*.json"),
            os.path.join(self.data_path, "Spotify Extended Streaming History", "Streaming_History_Audio_*.json")
        ]
        
        files = []
        for pattern in patterns:
            files.extend(glob.glob(pattern))
        
        if not files:
            # Fallback for nested subdir
            files = glob.glob(os.path.join(self.data_path, "**", "Streaming_History_Audio_*.json"), recursive=True)

        for file_path in sorted(files):
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    batch = json.load(f)
                    for entry in batch:
                        # Normalize fields between Extended and Standard formats
                        ts_str = entry.get('ts') or entry.get('endTime')
                        if not ts_str:
                            continue
                        
                        # Filter by duration and skip status
                        ms_played = entry.get('ms_played') or entry.get('msPlayed', 0)
                        skipped = entry.get('skipped', False)
                        
                        if ms_played < 30000 or skipped:
                            continue
                            
                        artist = entry.get('master_metadata_album_artist_name') or entry.get('artistName')
                        if not artist:
                            continue
                            
                        yield {
                            'ts': datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S") if 'T' in ts_str else datetime.strptime(ts_str, "%Y-%m-%d %H:%M"),
                            'artist': artist,
                            'duration': ms_played
                        }
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"Skipping malformed file {file_path}: {e}")

    def get_cosine_similarity(self, vec1, vec2):
        """Computes cosine similarity between two frequency dictionaries."""
        all_keys = set(vec1.keys()) | set(vec2.keys())
        dot_product = sum(vec1.get(k, 0) * vec2.get(k, 0) for k in all_keys)
        mag1 = math.sqrt(sum(v**2 for v in vec1.values()))
        mag2 = math.sqrt(sum(v**2 for v in vec2.values()))
        
        if mag1 == 0 or mag2 == 0:
            return 0
        return dot_product / (mag1 * mag2)

    def run_analysis(self, window_size_days=30, similarity_threshold=0.3):
        """Identifies transitions between musical stages."""
        records = sorted(list(self.load_data()), key=lambda x: x['ts'])
        if not records:
            return []

        # 1. Binned Artist Vectors
        start_date = records[0]['ts']
        end_date = records[-1]['ts']
        
        bins = []
        current_date = start_date
        while current_date <= end_date:
            bin_end = current_date + timedelta(days=window_size_days)
            bin_data = defaultdict(float)
            
            # Find records in this range
            for r in records:
                if current_date <= r['ts'] < bin_end:
                    # Weight by duration to favor 'deep listens'
                    bin_data[r['artist']] += r['duration']
            
            if bin_data:
                bins.append({
                    'start': current_date,
                    'end': bin_end,
                    'vector': bin_data
                })
            current_date = bin_end

        # 2. Detect Transitions
        stages = []
        if not bins:
            return []

        current_stage_start = bins[0]['start']
        current_stage_vectors = [bins[0]['vector']]
        
        for i in range(1, len(bins)):
            prev_bin = bins[i-1]['vector']
            curr_bin = bins[i]['vector']
            
            similarity = self.get_cosine_similarity(prev_bin, curr_bin)
            
            # If similarity drops significantly, start a new stage
            if similarity < similarity_threshold:
                # Close current stage
                stage_data = self._summarize_stage(current_stage_start, bins[i-1]['end'], current_stage_vectors)
                stages.append(stage_data)
                
                # Start new stage
                current_stage_start = bins[i]['start']
                current_stage_vectors = [bins[i]['vector']]
            else:
                current_stage_vectors.append(bins[i]['vector'])

        # Add the final stage
        if current_stage_vectors:
            stages.append(self._summarize_stage(current_stage_start, bins[-1]['end'], current_stage_vectors))

        return stages

    def _summarize_stage(self, start, end, vectors):
        """Aggregates multiple bins into a single stage summary."""
        aggregate = defaultdict(float)
        for v in vectors:
            for artist, weight in v.items():
                aggregate[artist] += weight
        
        sorted_artists = sorted(aggregate.items(), key=lambda x: x[1], reverse=True)
        top_artists = [a[0] for a in sorted_artists[:5]]
        
        return {
            'start': start.strftime("%Y-%m-%d"),
            'end': end.strftime("%Y-%m-%d"),
            'top_artists': top_artists,
            'duration_days': (end - start).days
        }

if __name__ == "__main__":
    # Example usage on real data
    analyzer = SpotifyAnalyzer("my_spotify_data")
    results = analyzer.run_analysis()
    
    print("\n# Your Life in Music Stages\n")
    with open("analysis_report.md", "w", encoding="utf-8") as f:
        f.write("# Spotify Life Stages: Analysis Report\n\n")
        f.write(f"Analyzed data from **{results[0]['start']}** to **{results[-1]['end']}**.\n\n")
        
        for i, stage in enumerate(results):
            output = f"## Stage {i+1}: {stage['start']} to {stage['end']}\n"
            output += f"*   **Key Artists**: {', '.join(stage['top_artists'])}\n"
            output += f"*   **Duration**: {stage['duration_days']} days\n\n"
            print(output.strip())
            f.write(output)
    print(f"\nReport saved to analysis_report.md")
