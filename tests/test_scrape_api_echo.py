"""API /scrape must always echo location debug fields in scrape_run_meta."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd
from starlette.requests import Request

import scraper_api


class TestScrapeApiEcho(unittest.IsolatedAsyncioTestCase):
    async def test_scrape_echo_fields_present(self) -> None:
        async def fake_run_area_scrape(**kwargs):
            meta_out = kwargs.get("meta_out")
            if isinstance(meta_out, dict):
                meta_out["raw_listing_row_count"] = 3
                meta_out["rows_with_coordinates"] = 2
                meta_out["inside_radius_row_count"] = 1
                meta_out["outside_radius_row_count"] = 1
            return pd.DataFrame(
                [
                    {"restaurant_name": "A", "lat": 25.205, "lng": 55.271},
                    {"restaurant_name": "B", "lat": 25.315, "lng": 55.381},
                ]
            )

        req = scraper_api.ScrapeRequest(
            pin_lat=25.2048,
            pin_lng=55.2708,
            radius_km=9.0,
            status_filter="live",
            city="dubai",
            client_asserted_pin_lat=25.2048,
            client_asserted_pin_lng=55.2708,
        )
        request = Request({"type": "http", "headers": []})
        request.state.request_id = "test-rid-123"
        with patch.object(scraper_api, "run_area_scrape", side_effect=fake_run_area_scrape):
            out = await scraper_api.scrape(req, request=request, x_api_key=None)

        meta = out.get("scrape_run_meta") or {}
        required = {
            "frontend_pin_lat",
            "frontend_pin_lng",
            "api_received_pin_lat",
            "api_received_pin_lng",
            "effective_scrape_pin_lat",
            "effective_scrape_pin_lng",
            "effective_radius_km",
            "raw_listing_row_count",
            "rows_with_coordinates",
            "inside_radius_row_count",
            "outside_radius_row_count",
        }
        self.assertTrue(required.issubset(meta.keys()), f"Missing keys: {sorted(required - set(meta.keys()))}")
        self.assertEqual(meta["frontend_pin_lat"], req.client_asserted_pin_lat)
        self.assertEqual(meta["api_received_pin_lat"], req.pin_lat)
        self.assertEqual(meta["effective_scrape_pin_lat"], req.pin_lat)
        self.assertEqual(meta["effective_radius_km"], req.radius_km)
        self.assertEqual(out["request_id"], "test-rid-123")
        self.assertEqual(out["count"], 2)


if __name__ == "__main__":
    unittest.main()
