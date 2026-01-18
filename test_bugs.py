import unittest
from monthly_analysis import SpotifyAnalyzer

class TestSpotifyGlitches(unittest.TestCase):
    def setUp(self):
        self.analyzer = SpotifyAnalyzer()

    def test_duplicate_records(self):
        """Test that identical records in overlapping files are not double-counted."""
        record = {
            "ts": "2023-01-01T12:00:00Z",
            "master_metadata_track_name": "Song",
            "master_metadata_album_artist_name": "Artist",
            "ms_played": 200000
        }
        self.analyzer.process_data([record, record])
        report = self.analyzer.get_report()
        self.assertEqual(report["2023-01"]["songs"][0]["score"], 1.0, "Double-counting detected!")

    def test_fle_intent_correction(self):
        """
        Test that a short skip is corrected once a full listen is found.
        1. 5s skip (skipped=True) -> score 1.0 (since it's the only data)
        2. 180s full listen (skipped=False) -> baseline becomes 180s.
        3. Total score should be (5+180)/180 = 1.03
        """
        data_skip = [{
            "ts": "2023-01-01T10:00:00Z",
            "master_metadata_track_name": "S",
            "master_metadata_album_artist_name": "A",
            "ms_played": 5000,
            "skipped": True
        }]
        self.analyzer.process_data(data_skip)
        rep1 = self.analyzer.get_report()
        self.assertEqual(rep1["2023-01"]["songs"][0]["score"], 1.0)

        data_full = [{
            "ts": "2023-01-01T11:00:00Z",
            "master_metadata_track_name": "S",
            "master_metadata_album_artist_name": "A",
            "ms_played": 180000,
            "skipped": False
        }]
        self.analyzer.process_data(data_full)
        rep2 = self.analyzer.get_report()
        # Score = (5000 + 180000) / 180000 = 1.027...
        self.assertEqual(rep2["2023-01"]["songs"][0]["score"], 1.03)

    def test_fle_never_finished(self):
        """
        Test that content never finished (all skips) uses inferred maximum.
        1. 10s skip
        2. 20s skip
        Score = (10+20)/20 = 1.5 FLE
        """
        data = [
            {"ts": "2023-01-01T10:00:00Z", "master_metadata_track_name": "S", "master_metadata_album_artist_name": "A", "ms_played": 10000, "skipped": True},
            {"ts": "2023-01-01T10:05:00Z", "master_metadata_track_name": "S", "master_metadata_album_artist_name": "A", "ms_played": 20000, "skipped": True},
        ]
        self.analyzer.process_data(data)
        report = self.analyzer.get_report()
        self.assertEqual(report["2023-01"]["songs"][0]["score"], 1.5)

    def test_case_insensitivity(self):
        """Test that artist/song names are grouped case-insensitively."""
        data = [
            {"ts": "2023-01-01T12:00:00Z", "master_metadata_track_name": "song", "master_metadata_album_artist_name": "ARTIST", "ms_played": 180000},
            {"ts": "2023-01-01T12:05:00Z", "master_metadata_track_name": "SONG", "master_metadata_album_artist_name": "artist", "ms_played": 180000},
        ]
        self.analyzer.process_data(data)
        report = self.analyzer.get_report()
        self.assertEqual(len(report["2023-01"]["artists"]), 1, "Case-sensitive grouping detected!")

if __name__ == "__main__":
    unittest.main()
