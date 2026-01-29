import unittest
import json
import os
from monthly_analysis import SpotifyAnalyzer

class TestMonthlyAnalysis(unittest.TestCase):
    def setUp(self):
        self.analyzer = SpotifyAnalyzer()

    def _run_analysis(self, data):
        self.analyzer.collect_records(data)
        self.analyzer.finalize_durations()
        self.analyzer.process_data()

    def test_basic_aggregation(self):
        data = [
            {"ts": "2023-01-01T10:00:00Z", "master_metadata_track_name": "Song A", "master_metadata_album_artist_name": "Artist X", "ms_played": 180000},
            {"ts": "2023-01-02T10:00:00Z", "master_metadata_track_name": "Song A", "master_metadata_album_artist_name": "Artist X", "ms_played": 180000},
            {"ts": "2023-01-03T10:00:00Z", "master_metadata_track_name": "Song B", "master_metadata_album_artist_name": "Artist Y", "ms_played": 180000},
        ]
        self._run_analysis(data)
        report = self.analyzer.get_report(top_n=10)
        
        jan = report["monthly"]["2023-01"]
        self.assertEqual(jan["songs"][0]["name"], "Song A")
        self.assertEqual(jan["songs"][0]["score"], 2.0)  # FLE: 2 full listens = 2.0
        self.assertEqual(jan["artists"][0]["name"], "Artist X")
        self.assertEqual(jan["artists"][0]["score"], 2.0)
        
        # Test yearly aggregation
        year_2023 = report["yearly"]["2023"]
        self.assertEqual(year_2023["songs"][0]["name"], "Song A")
        self.assertEqual(year_2023["songs"][0]["score"], 2.0)
        
        # Test all-time aggregation
        alltime = report["alltime"]
        self.assertEqual(alltime["songs"][0]["name"], "Song A")
        self.assertEqual(alltime["songs"][0]["score"], 2.0)

    def test_null_handling(self):
        data = [
            {"ts": "2023-01-01T10:00:00Z", "master_metadata_track_name": None, "master_metadata_album_artist_name": None, "ms_played": 10000},
            {"ts": "2023-01-02T10:00:00Z", "master_metadata_track_name": "Real Song", "master_metadata_album_artist_name": "Real Artist", "ms_played": 180000},
        ]
        self._run_analysis(data)
        report = self.analyzer.get_report()
        jan = report["monthly"]["2023-01"]
        # Should ignore the null entry
        self.assertEqual(len(jan["songs"]), 1)
        self.assertEqual(jan["songs"][0]["name"], "Real Song")

    def test_out_of_order_months(self):
        data = [
            {"ts": "2023-02-01T10:00:00Z", "master_metadata_track_name": "Feb Song", "master_metadata_album_artist_name": "Artist", "ms_played": 180000},
            {"ts": "2023-01-01T10:00:00Z", "master_metadata_track_name": "Jan Song", "master_metadata_album_artist_name": "Artist", "ms_played": 180000},
        ]
        self._run_analysis(data)
        report = self.analyzer.get_report()
        # Months should be sorted
        months = list(report["monthly"].keys())
        self.assertEqual(months, ["2023-01", "2023-02"])

    def test_configurable_top_n(self):
        data = []
        for i in range(20):
            data.append({"ts": "2023-01-01T10:00:00Z", "master_metadata_track_name": f"Song {i}", "master_metadata_album_artist_name": "Artist", "ms_played": 180000})
        
        self._run_analysis(data)
        report = self.analyzer.get_report(top_n=5)
        self.assertEqual(len(report["monthly"]["2023-01"]["songs"]), 5)
        self.assertEqual(len(report["yearly"]["2023"]["songs"]), 5)
        self.assertEqual(len(report["alltime"]["songs"]), 5)

if __name__ == "__main__":
    unittest.main()
