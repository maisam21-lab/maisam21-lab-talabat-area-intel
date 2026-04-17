"""Validate scrape pins for the public API (fail loudly on missing / absurd coordinates)."""

from __future__ import annotations

from fastapi import HTTPException


def parse_scrape_pin_or_raise_value_error(pin_lat: float | None, pin_lng: float | None) -> tuple[float, float]:
    """Return (lat, lng) or raise ValueError (shared by API and Streamlit)."""
    if pin_lat is None or pin_lng is None:
        raise ValueError("pin_lat and pin_lng are required")
    try:
        lat = float(pin_lat)
        lng = float(pin_lng)
    except (TypeError, ValueError) as exc:
        raise ValueError("pin_lat and pin_lng must be numbers") from exc
    if abs(lat) < 1e-9 and abs(lng) < 1e-9:
        raise ValueError("pin (0,0) is not valid — set a real UAE location")
    if not (20.5 <= lat <= 28.5 and 50.5 <= lng <= 58.5):
        raise ValueError(f"pin ({lat:.5f},{lng:.5f}) is outside allowed UAE scrape bounds")
    return lat, lng


def validate_scrape_pin(pin_lat: float | None, pin_lng: float | None) -> tuple[float, float]:
    """Return (lat, lng) or raise 400. UAE-centric bounds (includes maritime / border padding)."""
    try:
        return parse_scrape_pin_or_raise_value_error(pin_lat, pin_lng)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def assert_client_pin_matches_body(
    body_lat: float,
    body_lng: float,
    asserted_lat: float | None,
    asserted_lng: float | None,
) -> None:
    """Streamlit sends asserted pin identical to body; mismatch means split UI state — hard fail."""
    if asserted_lat is None and asserted_lng is None:
        return
    if asserted_lat is None or asserted_lng is None:
        raise HTTPException(
            status_code=400,
            detail="Send both client_asserted_pin_lat and client_asserted_pin_lng or omit both",
        )
    if abs(float(asserted_lat) - float(body_lat)) > 1e-5 or abs(float(asserted_lng) - float(body_lng)) > 1e-5:
        raise HTTPException(
            status_code=400,
            detail=(
                "client_asserted pin does not match pin_lat/pin_lng — frontend location state is split. "
                f"body=({body_lat:.6f},{body_lng:.6f}) asserted=({float(asserted_lat):.6f},{float(asserted_lng):.6f})"
            ),
        )
