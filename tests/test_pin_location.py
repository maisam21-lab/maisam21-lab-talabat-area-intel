"""Location + radius classification must change when the scrape pin moves."""

from __future__ import annotations

import unittest

import pandas as pd

from scrape_engine import compute_radius_stats


class TestPinLocation(unittest.TestCase):
    def test_two_distant_pins_change_inside_radius_rows(self) -> None:
        """One branch near Dubai must be inside under a Dubai pin and outside under an Abu Dhabi pin."""
        df = pd.DataFrame(
            {
                "lat": [25.21],
                "lng": [55.28],
                "restaurant_name": ["Dubai-ish branch"],
            }
        )
        dubai_pin = (25.2048, 55.2708)
        abu_pin = (24.4539, 54.3773)
        radius_km = 12.0
        _, dubai_stats = compute_radius_stats(df, dubai_pin[0], dubai_pin[1], radius_km)
        _, abu_stats = compute_radius_stats(df, abu_pin[0], abu_pin[1], radius_km)

        self.assertEqual(int(dubai_stats["inside_radius_row_count"]), 1)
        self.assertEqual(int(abu_stats["inside_radius_row_count"]), 0)
        self.assertEqual(int(abu_stats["outside_radius_row_count"]), 1)


if __name__ == "__main__":
    unittest.main()
