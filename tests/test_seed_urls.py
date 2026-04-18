"""Seed vendor URL normalization for scrape_engine (no Playwright)."""

from __future__ import annotations

import unittest

from scrape_engine import build_restaurant_records_from_seed_urls, normalize_seed_vendor_urls


class TestSeedUrls(unittest.TestCase):
    def test_normalize_dedupe_and_uae_canonical(self) -> None:
        raw = [
            "https://www.talabat.com/en/uae/foo-bar?x=1",
            "https://www.talabat.com/uae/foo-bar",
            "# comment",
            "https://www.talabat.com/egypt/other-vendor",
        ]
        out = normalize_seed_vendor_urls(raw, max_urls=50)
        self.assertEqual(
            out,
            [
                "https://www.talabat.com/uae/foo-bar",
                "https://www.talabat.com/egypt/other-vendor",
            ],
        )

    def test_csv_first_column(self) -> None:
        out = normalize_seed_vendor_urls(
            ["https://www.talabat.com/uae/x,ignored,ignored"],
            max_urls=10,
        )
        self.assertEqual(out, ["https://www.talabat.com/uae/x"])

    def test_build_records_shape(self) -> None:
        recs = build_restaurant_records_from_seed_urls(
            ["https://www.talabat.com/uae/demo-slug"],
            pin_lat=25.2,
            pin_lng=55.27,
            radius_km=10.0,
        )
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].restaurant_url, "https://www.talabat.com/uae/demo-slug")
        self.assertEqual(recs[0].talabat_listing_slug, "demo-slug")


if __name__ == "__main__":
    unittest.main()
