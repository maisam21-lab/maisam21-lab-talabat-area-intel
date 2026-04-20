from __future__ import annotations

import os

import pytest
import requests

from google_coverage import fetch_google_nearby_restaurants, google_coverage_enabled


def test_google_coverage_enabled_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "x")
    monkeypatch.setenv("GOOGLE_COVERAGE_ENABLED", "1")
    assert google_coverage_enabled() is True
    monkeypatch.setenv("GOOGLE_COVERAGE_ENABLED", "0")
    assert google_coverage_enabled() is False


def test_fetch_google_nearby_restaurants_parses_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "x")

    class FakeResp:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "status": "OK",
                "results": [
                    {
                        "name": "Place A",
                        "place_id": "p1",
                        "rating": 4.3,
                        "user_ratings_total": 41,
                        "vicinity": "Dubai",
                        "types": ["restaurant", "food"],
                        "geometry": {"location": {"lat": 25.2, "lng": 55.3}},
                    }
                ],
            }

    class FakeSession:
        def get(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            return FakeResp()

    monkeypatch.setattr(requests, "Session", lambda: FakeSession())
    rows = fetch_google_nearby_restaurants(25.2, 55.3, 8.0)
    assert len(rows) == 1
    assert rows[0]["name"] == "Place A"
    assert rows[0]["source"] == "google_only"

