"""Foursquare nearby restaurant coverage."""

from __future__ import annotations

import os

import requests

FSQ_SEARCH_URL = "https://api.foursquare.com/v3/places/search"


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def foursquare_coverage_enabled() -> bool:
    if not (os.getenv("FOURSQUARE_API_KEY") or "").strip():
        return False
    return _truthy(os.getenv("FOURSQUARE_COVERAGE_ENABLED", "1"))


def fetch_foursquare_nearby_restaurants(pin_lat: float, pin_lng: float, radius_km: float) -> list[dict]:
    key = (os.getenv("FOURSQUARE_API_KEY") or "").strip()
    if not key or radius_km <= 0:
        return []

    radius_m = int(max(300, min(100_000, float(radius_km) * 1000.0)))
    max_rows = int(os.getenv("FOURSQUARE_COVERAGE_MAX_ROWS", "300"))
    max_rows = max(10, min(500, max_rows))

    headers = {
        "Authorization": key,
        "Accept": "application/json",
    }
    params = {
        "ll": f"{float(pin_lat):.7f},{float(pin_lng):.7f}",
        "radius": str(radius_m),
        "categories": "13065",
        "limit": "50",
        "sort": "DISTANCE",
    }

    out: list[dict] = []
    offset = 0
    session = requests.Session()
    while len(out) < max_rows:
        params["offset"] = str(offset)
        try:
            resp = session.get(FSQ_SEARCH_URL, headers=headers, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, ValueError):
            break
        rows = payload.get("results") or []
        if not isinstance(rows, list) or not rows:
            break
        for item in rows:
            geocodes = item.get("geocodes") or {}
            main = geocodes.get("main") or {}
            lat = main.get("latitude")
            lng = main.get("longitude")
            if lat is None or lng is None:
                continue
            try:
                la, ln = float(lat), float(lng)
            except (TypeError, ValueError):
                continue
            fsq_id = str(item.get("fsq_id") or "").strip()
            name = str(item.get("name") or "").strip()
            location = item.get("location") or {}
            formatted_address = str(location.get("formatted_address") or "").strip()
            locality = str(location.get("locality") or "").strip()
            region = str(location.get("region") or "").strip()
            country = str(location.get("country") or "").strip()
            categories = item.get("categories") or []
            category_names = ", ".join(
                str(c.get("name") or "").strip() for c in categories if isinstance(c, dict) and c.get("name")
            )[:400]
            out.append(
                {
                    "source": "foursquare_only",
                    "foursquare_id": fsq_id,
                    "name": name,
                    "lat": la,
                    "lng": ln,
                    "foursquare_categories": category_names,
                    "foursquare_link": f"https://foursquare.com/v/{fsq_id}" if fsq_id else "",
                    "foursquare_formatted_address": formatted_address,
                    "foursquare_locality": locality,
                    "foursquare_region": region,
                    "foursquare_country": country,
                }
            )
            if len(out) >= max_rows:
                break
        if len(rows) < 50:
            break
        offset += 50

    dedup: dict[str, dict] = {}
    for row in out:
        fsq_id = str(row.get("foursquare_id") or "").strip().lower()
        if fsq_id:
            key = fsq_id
        else:
            key = f"{str(row.get('name') or '').strip().lower()}|{float(row['lat']):.5f}|{float(row['lng']):.5f}"
        if key not in dedup:
            dedup[key] = row
    return list(dedup.values())
