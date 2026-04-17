"""GET /health/scrape-config returns non-secret deployment guardrails."""

from __future__ import annotations

import os
import unittest

from fastapi.testclient import TestClient

import scraper_api


class TestHealthScrapeConfig(unittest.TestCase):
    def test_scrape_config_requires_key_when_configured(self) -> None:
        prev = os.environ.get("SCRAPER_API_KEY")
        try:
            os.environ["SCRAPER_API_KEY"] = "test-secret-key"
            client = TestClient(scraper_api.app)
            r = client.get("/health/scrape-config")
            self.assertEqual(r.status_code, 401)
            r2 = client.get("/health/scrape-config", headers={"X-API-Key": "test-secret-key"})
            self.assertEqual(r2.status_code, 200)
            data = r2.json()
            self.assertTrue(data.get("ok"))
            self.assertIn("scraper_wall_clock_sec_default", data)
            self.assertIn("google_maps_key_configured", data)
        finally:
            if prev is None:
                os.environ.pop("SCRAPER_API_KEY", None)
            else:
                os.environ["SCRAPER_API_KEY"] = prev


if __name__ == "__main__":
    unittest.main()
