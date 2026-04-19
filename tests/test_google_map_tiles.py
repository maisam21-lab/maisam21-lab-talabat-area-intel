"""Unit tests for Google Map Tiles session helper."""

from __future__ import annotations

import time

import pytest
import requests

from google_map_tiles import (
    ensure_google_map_tile_sessions,
    google_2d_tile_url_template,
    google_maps_tile_attribution,
)


def test_google_2d_tile_url_template_encodes_params() -> None:
    url = google_2d_tile_url_template("k=key&x", "sess/ion+")
    assert "2dtiles/{z}/{x}/{y}" in url
    assert "key=" in url
    assert "{z}" in url
    assert "sess%2Fion%2B" in url or "sess" in url


def test_google_maps_tile_attribution_contains_google() -> None:
    assert "Google" in google_maps_tile_attribution()


def test_ensure_sessions_requests_roadmap_and_satellite(monkeypatch: pytest.MonkeyPatch) -> None:
    bodies: list[dict] = []

    class FakeResp:
        def __init__(self, session: str, expiry: str) -> None:
            self._session = session
            self._expiry = expiry

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"session": self._session, "expiry": self._expiry}

    def fake_post(url: str, json=None, headers=None, timeout=None):  # type: ignore[no-untyped-def]
        bodies.append(dict(json or {}))
        mt = (json or {}).get("mapType")
        if mt == "roadmap":
            return FakeResp("rm-token", "2000000000")
        if mt == "satellite":
            return FakeResp("sat-token", "2000000000")
        raise AssertionError(f"unexpected mapType {mt}")

    monkeypatch.setattr(requests, "post", fake_post)
    cache: dict = {}
    r1, s1 = ensure_google_map_tile_sessions("test-key", cache)
    assert r1 == "rm-token"
    assert s1 == "sat-token"
    assert len(bodies) == 2
    assert bodies[0]["mapType"] == "roadmap"
    assert bodies[1]["mapType"] == "satellite"
    assert bodies[1]["layerTypes"] == ["layerRoadmap"]
    assert bodies[1]["overlay"] is False

    # Cached: no new HTTP
    bodies.clear()
    r2, s2 = ensure_google_map_tile_sessions("test-key", cache)
    assert (r2, s2) == (r1, s1)
    assert bodies == []


def test_ensure_sessions_clears_cache_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise requests.ConnectionError("down")

    monkeypatch.setattr(requests, "post", fake_post)
    cache = {"roadmap": {"session": "x", "expiry": time.time() + 99999}}
    r, s = ensure_google_map_tile_sessions("k", cache)
    assert r is None and s is None
    assert "roadmap" not in cache
