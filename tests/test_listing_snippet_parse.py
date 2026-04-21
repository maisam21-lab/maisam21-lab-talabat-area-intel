from __future__ import annotations

from scrape_engine import parse_listing_snippet_fields


def test_parse_listing_snippet_fields_extracts_rating_and_reviews() -> None:
    s = "Burgers • 4.3 • 1,250 ratings • 25-35 min • Delivery AED 7.5"
    out = parse_listing_snippet_fields(s)
    assert out["cuisines"]
    assert out["rating"] == "4.3"
    assert out["reviews_count"] == "1250"
    assert "min" in out["eta"]
    assert "AED" in out["delivery_fee"]

