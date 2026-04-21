"""Google-only nearby restaurant coverage (not Talabat-sourced)."""

from __future__ import annotations

import os
import time

import requests

NEARBY_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def google_coverage_enabled() -> bool:
    if not (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip():
        return False
    return _truthy(os.getenv("GOOGLE_COVERAGE_ENABLED", "1"))


def fetch_google_nearby_restaurants(pin_lat: float, pin_lng: float, radius_km: float) -> list[dict]:
    """Return Google Places Nearby Search rows around the pin."""
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key or radius_km <= 0:
        return []

    radius_m = int(max(300, min(50_000, float(radius_km) * 1000.0)))
    max_pages = int(os.getenv("GOOGLE_COVERAGE_MAX_PAGES", "3"))
    max_rows = int(os.getenv("GOOGLE_COVERAGE_MAX_ROWS", "300"))
    max_pages = max(1, min(3, max_pages))
    max_rows = max(10, min(300, max_rows))

    out: list[dict] = []
    next_page_token: str | None = None
    session = requests.Session()

    for page_idx in range(max_pages):
        params: dict[str, str] = {
            "key": key,
            "language": "en",
        }
        if next_page_token:
            # Google documents a brief token activation delay.
            time.sleep(2.0)
            params["pagetoken"] = next_page_token
        else:
            params["location"] = f"{float(pin_lat):.7f},{float(pin_lng):.7f}"
            params["radius"] = str(radius_m)
            params["type"] = "restaurant"

        try:
            r = session.get(NEARBY_SEARCH_URL, params=params, timeout=20)
            r.raise_for_status()
            payload = r.json()
        except (requests.RequestException, ValueError):
            break

        status = str(payload.get("status") or "")
        if status not in ("OK", "ZERO_RESULTS"):
            break
        for item in (payload.get("results") or []):
            geom = (item.get("geometry") or {}).get("location") or {}
            lat = geom.get("lat")
            lng = geom.get("lng")
            if lat is None or lng is None:
                continue
            try:
                la, ln = float(lat), float(lng)
            except (TypeError, ValueError):
                continue
            if not (-90 <= la <= 90 and -180 <= ln <= 180):
                continue
            name = str(item.get("name") or "").strip()
            place_id = str(item.get("place_id") or "").strip()
            business_status = str(item.get("business_status") or "").strip()
            rating = item.get("rating")
            ratings_total = item.get("user_ratings_total")
            vicinity = str(item.get("vicinity") or "").strip()
            types = item.get("types") or []
            out.append(
                {
                    "source": "google_only",
                    "google_place_id": place_id,
                    "name": name,
                    "business_status": business_status,
                    "rating": rating if isinstance(rating, (int, float)) else None,
                    "user_ratings_total": int(ratings_total) if isinstance(ratings_total, (int, float)) else None,
                    "vicinity": vicinity[:280],
                    "types": ", ".join(str(t) for t in types if t)[:400] if isinstance(types, list) else "",
                    "lat": la,
                    "lng": ln,
                    "google_maps_link": f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else "",
                    "page": page_idx + 1,
                }
            )
            if len(out) >= max_rows:
                break
        if len(out) >= max_rows:
            break
        next_page_token = payload.get("next_page_token")
        if not next_page_token:
            break

    # Deduplicate by place_id; if missing, fallback to rounded coordinate + name.
    dedup: dict[str, dict] = {}
    for row in out:
        pid = str(row.get("google_place_id") or "").strip().lower()
        if pid:
            key = pid
        else:
            key = f"{str(row.get('name') or '').strip().lower()}|{float(row['lat']):.5f}|{float(row['lng']):.5f}"
        if key not in dedup:
            dedup[key] = row
    return list(dedup.values())
