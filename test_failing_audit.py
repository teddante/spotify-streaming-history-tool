import unittest
from monthly_analysis import SpotifyAnalyzer

class TestSpotifyBugs(unittest.TestCase):

    # --- CONFIRMED BUGS ---

    def test_bug1_attribute_error_numeric_metadata(self):
        """
        BUG #1: _normalize() crashes if metadata fields are numeric.
        Expected: Should gracefully convert to string instead of crashing.
        """
        analyzer = SpotifyAnalyzer()
        records = [
            {
                "ts": "2026-01-01T12:00:00Z",
                "ms_played": 1000,
                "master_metadata_track_name": 12345,  # Numeric track name
                "master_metadata_album_artist_name": "Artist A"
            }
        ]
        # This should NOT raise AttributeError
        try:
            analyzer.process_data(records)
        except AttributeError as e:
            self.fail(f"process_data crashed with AttributeError on numeric track name: {e}")

    # --- EDGE CASE TESTS ---

    def test_none_ms_played(self):
        """
        Verify handling of None in ms_played.
        """
        analyzer = SpotifyAnalyzer()
        records = [
            {
                "ts": "2026-01-01T12:00:00Z",
                "ms_played": None,
                "master_metadata_track_name": "Song A",
                "master_metadata_album_artist_name": "Artist A"
            }
        ]
        analyzer.process_data(records)
        self.assertEqual(analyzer.stats["processed"], 1)

    def test_fle_division_by_zero(self):
        """
        Verify FLE calculation doesn't crash when max_ms is 0.
        This can happen if all plays have ms_played=0.
        """
        analyzer = SpotifyAnalyzer()
        records = [
            {
                "ts": "2026-01-01T12:00:00Z",
                "ms_played": 0,
                "master_metadata_track_name": "Silent Track",
                "master_metadata_album_artist_name": "Artist B"
            }
        ]
        analyzer.process_data(records)
        # Should not crash
        report = analyzer.get_report()
        self.assertIn("2026-01", report)

    def test_duplicate_timestamp_different_songs(self):
        """
        Verify that two different songs with the same timestamp are both processed.
        The current de-duplication uses (ts, ms_played, track, artist).
        """
        analyzer = SpotifyAnalyzer()
        records = [
            {
                "ts": "2026-01-01T12:00:00Z",
                "ms_played": 1000,
                "master_metadata_track_name": "Song A",
                "master_metadata_album_artist_name": "Artist A"
            },
            {
                "ts": "2026-01-01T12:00:00Z",  # Same timestamp
                "ms_played": 1000,              # Same ms_played
                "master_metadata_track_name": "Song B",  # Different song
                "master_metadata_album_artist_name": "Artist A"
            }
        ]
        analyzer.process_data(records)
        self.assertEqual(analyzer.stats["processed"], 2, 
                         "Both songs should be processed despite same timestamp")

    def test_malformed_timestamp(self):
        """
        Verify that malformed timestamps (e.g., "not-a-date") are skipped.
        """
        analyzer = SpotifyAnalyzer()
        records = [
            {
                "ts": "not-a-date",
                "ms_played": 1000,
                "master_metadata_track_name": "Song A",
                "master_metadata_album_artist_name": "Artist A"
            }
        ]
        analyzer.process_data(records)
        self.assertEqual(analyzer.stats["skipped"], 1)
        self.assertEqual(analyzer.stats["processed"], 0)

    def test_empty_track_name_skipped(self):
        """
        Verify that records with empty track names are skipped.
        """
        analyzer = SpotifyAnalyzer()
        records = [
            {
                "ts": "2026-01-01T12:00:00Z",
                "ms_played": 1000,
                "master_metadata_track_name": "",
                "master_metadata_album_artist_name": "Artist A"
            }
        ]
        analyzer.process_data(records)
        self.assertEqual(analyzer.stats["skipped"], 1)

if __name__ == "__main__":
    unittest.main()
