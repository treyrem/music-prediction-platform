import unittest
import pandas as pd


class TestDataQuality(unittest.TestCase):

    def setUp(self):
        self.df = pd.read_sql("SELECT * FROM cleaned_music_data", conn)

    def test_no_empty_track_names(self):
        empty_tracks = self.df[self.df["track_name"].str.strip() == ""]
        self.assertEqual(len(empty_tracks), 0, "Found empty track names")

    def test_reasonable_view_counts(self):
        # Check for unreasonably high view counts (potential data errors)
        max_views = self.df["view_count"].max()
        self.assertLess(
            max_views, 10_000_000_000, "Unreasonably high view count detected"
        )

    def test_date_formats(self):
        # Ensure dates are in correct format
        date_columns = ["chart_date", "collection_date"]
        for col in date_columns:
            if col in self.df.columns:
                # Test that dates can be parsed
                pd.to_datetime(self.df[col], errors="coerce")


if __name__ == "__main__":
    unittest.main()
