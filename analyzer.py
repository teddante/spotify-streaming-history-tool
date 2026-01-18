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
                        
                        artist = entry.get('master_metadata_album_artist_name') or entry.get('artistName')
                        if not artist:
                            continue
                        
                        ms_played = entry.get('ms_played') or entry.get('msPlayed', 0)
                            
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

    def run_analysis(self, bin_size=100):
        """Identifies transitions between musical stages using adaptive similarity and volume-invariant binning."""
        records = sorted(list(self.load_data()), key=lambda x: x['ts'])
        if not records:
            return []

        # 1. Volume-Invariant Binning with Log-Weighting
        bins = []
        for i in range(0, len(records), bin_size):
            batch = records[i:i + bin_size]
            bin_data = defaultdict(float)
            for r in batch:
                # Log-weighting: captures intent without letting long tracks drown out variety
                # 30s -> ~1.5, 3m -> ~2.3, 1h -> ~4.5
                weight = 1 + math.log10(max(0, r['duration']) / 1000 + 1)
                bin_data[r['artist']] += weight
            
            bins.append({
                'start': batch[0]['ts'],
                'end': batch[-1]['ts'],
                'vector': bin_data
            })

        if not bins:
            return []

        # 2. Detect Transitions with Adaptive Threshold
        stages = []
        current_stage_start = bins[0]['start']
        current_stage_bins = [bins[0]]
        
        # Track similarities to establish a rolling baseline
        similarities = []
        
        for i in range(1, len(bins)):
            # Compare current bin to the mean of the current stage
            stage_mean = self._get_mean_vector([b['vector'] for b in current_stage_bins])
            curr_vector = bins[i]['vector']
            
            sim = self.get_cosine_similarity(stage_mean, curr_vector)
            similarities.append(sim)
            
            # Adaptive threshold: if similarity is significantly lower than recent baseline
            # If we don't have enough history, use a conservative starting threshold
            recent_sims = similarities[-5:] if len(similarities) > 5 else similarities
            baseline = sum(recent_sims) / len(recent_sims) if recent_sims else 1.0
            
            # Transition triggered by deviation from rolling similarity baseline
            # This is emergent rather than a hard 0.3 threshold
            is_transition = sim < (baseline * 0.7) and sim < 0.5
            
            if is_transition:
                # Lookahead to verify it's not a momentary outlier
                is_real = True
                if i + 1 < len(bins):
                    next_sim = self.get_cosine_similarity(stage_mean, bins[i+1]['vector'])
                    if next_sim > baseline * 0.8:
                        is_real = False
                
                if is_real:
                    stages.append(self._summarize_stage(current_stage_start, current_stage_bins[-1]['end'], [b['vector'] for b in current_stage_bins]))
                    current_stage_start = bins[i]['start']
                    current_stage_bins = [bins[i]]
                    # Reset similarity tracking for the new stage
                    similarities = []
                else:
                    current_stage_bins.append(bins[i])
            else:
                current_stage_bins.append(bins[i])

        if current_stage_bins:
            stages.append(self._summarize_stage(current_stage_start, current_stage_bins[-1]['end'], [b['vector'] for b in current_stage_bins]))

        return stages

    def _get_mean_vector(self, vectors):
        """Computes the average frequency vector."""
        if not vectors:
            return {}
        mean = defaultdict(float)
        for v in vectors:
            for artist, weight in v.items():
                mean[artist] += weight / len(vectors)
        return mean

    def _summarize_stage(self, start, end, vectors):
        """Aggregates multiple bins into a single stage summary with structural metrics."""
        aggregate = defaultdict(float)
        for v in vectors:
            for artist, weight in v.items():
                aggregate[artist] += weight
        
        sorted_artists = sorted(aggregate.items(), key=lambda x: x[1], reverse=True)
        top_artists = [a[0] for a in sorted_artists[:5]]
        
        # Structural Metrics (Purely Quantitative)
        total_weight = sum(aggregate.values())
        
        # 1. Artist Concentration (Weight of Top 1 artist)
        concentration = (sorted_artists[0][1] / total_weight) if total_weight > 0 else 0
        
        # 2. Shannon Entropy (Diversity Index)
        # Low entropy = hyper-focused on few artists, High entropy = diverse mix
        entropy = 0
        if total_weight > 0:
            for artist, weight in aggregate.items():
                p = weight / total_weight
                if p > 0:
                    entropy -= p * math.log2(p)

        return {
            'start': start.strftime("%Y-%m-%d"),
            'end': end.strftime("%Y-%m-%d"),
            'top_artists': top_artists,
            'duration_days': (end - start).days,
            'concentration': round(concentration, 3),
            'diversity_index': round(entropy, 3)
        }

if __name__ == "__main__":
    # Example usage on real data
    analyzer = SpotifyAnalyzer("my_spotify_data")
    results = analyzer.run_analysis()
    
    if not results:
        print("No data found to analyze.")
        exit(0)

    print("\n# Your Life in Music Stages\n")
    with open("analysis_report.md", "w", encoding="utf-8") as f:
        f.write("# 🎵 Spotify Life Stages: Analysis Report\n\n")
        f.write(f"> Analyzed data from **{results[0]['start']}** to **{results[-1]['end']}**.\n\n")
        f.write("---\n\n")
        
        for i, stage in enumerate(results):
            # Aesthetic ASCII bar for duration (max 20 chars)
            bar_len = min(20, max(1, stage['duration_days'] // 30))
            bar = "█" * bar_len + "░" * (20 - bar_len)
            
            output = f"### 📅 Stage {i+1}: {stage['start']} to {stage['end']}\n"
            output += f"**Entropy (Diversity)**: `{stage['diversity_index']}` | **Focus**: `{stage['concentration']}`\n\n"
            output += f"```\n|{bar}| {stage['duration_days']} days\n```\n"
            output += f"*   **Musical DNA**: {', '.join(stage['top_artists'])}\n\n"
            
            print(output.strip())
            f.write(output + "\n---\n\n")
            
    print(f"\n✨ Report saved to analysis_report.md")
