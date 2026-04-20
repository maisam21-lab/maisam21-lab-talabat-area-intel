from __future__ import annotations

import pytest
from pydantic import ValidationError

from scraper_api import GoogleCoverageRequest, ScrapeRequest


def test_scrape_radius_bounds() -> None:
    ScrapeRequest(radius_km=5.0)
    ScrapeRequest(radius_km=10.0)
    with pytest.raises(ValidationError):
        ScrapeRequest(radius_km=4.9)
    with pytest.raises(ValidationError):
        ScrapeRequest(radius_km=10.1)


def test_google_coverage_radius_bounds() -> None:
    GoogleCoverageRequest(radius_km=6.0)
    with pytest.raises(ValidationError):
        GoogleCoverageRequest(radius_km=4.0)
