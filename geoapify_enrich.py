"""
Enrich restaurant records with phone numbers from Geoapify Places API.
Uses OpenStreetMap data — free tier: 3,000 requests/day.
Searches by restaurant name + coordinates, picks closest match within 500m.
Results cached to disk.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import requests

from geo_utils import haversine_km

_CACHE_PATH = Path(os.getenv("GEOAPIFY_CACHE_PATH", "/app/analyze_jobs/geoapify_cache.json"))
_API_KEY_ENV = "GEOAPIFY_API_KEY"
_PLACES_URL = "https://api.geoapify.com/v2/places"
_DETAILS_URL = "https://api.geoapify.com/v2/place-details"

_UAE_MOBILE_RE = re.compile(
    r'(?<!\d)(?:'
    r'\+971\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|00971\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|0\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r')(?!\d)'
)


def _load_cache() -> dict:
    try:
        if _CACHE_PATH.exists():
            return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_cache(cache: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass


def _extract_phone(props: dict) -> str:
    """Extract phone from Geoapify place properties."""
    raw = (props.get("datasource") or {}).get("raw") or {}
    for field in ("contact:phone", "phone", "contact:mobile", "mobile"):
        val = (raw.get(field) or "").strip()
        if val:
            return val
    return ""


def _is_service_number(phone: str) -> bool:
    """Return True for 600/800 UAE service numbers — not useful for outreach."""
    p = re.sub(r"[\s\-\.\(\)]", "", phone)
    return bool(
        p.startswith("+971600") or p.startswith("971600") or p.startswith("600")
        or p.startswith("+971800") or p.startswith("971800") or p.startswith("800")
    )


def search_geoapify_phone(
    name: str,
    lat: float,
    lng: float,
    *,
    api_key: str,
    session: requests.Session,
    radius_m: int = 500,
    max_km: float = 0.5,
) -> dict:
    """
    Search Geoapify for a restaurant by name near lat/lng.
    Returns dict: {geoapify_phone, geoapify_address, geoapify_place_id}
    """
    empty = {"geoapify_phone": "", "geoapify_address": "", "geoapify_place_id": ""}

    params = {
        "categories": "catering",
        "filter": f"circle:{lng},{lat},{radius_m}",
        "bias": f"proximity:{lng},{lat}",
        "name": name,
        "limit": 5,
        "apiKey": api_key,
    }
    try:
        r = session.get(_PLACES_URL, params=params, timeout=12)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return empty

    features = data.get("features") or []
    if not features:
        return empty

    # Pick closest feature by actual distance
    best = None
    best_d = 1e9
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        flng, flat = float(coords[0]), float(coords[1])
        d = haversine_km(lat, lng, flat, flng)
        if d < best_d:
            best_d, best = d, feat

    if best is None or best_d > max_km:
        return empty

    props = best.get("properties") or {}
    phone = _extract_phone(props)

    # Filter out service numbers
    if phone and _is_service_number(phone):
        phone = ""

    place_id = props.get("place_id", "")
    address = props.get("formatted", "")

    return {
        "geoapify_phone": phone,
        "geoapify_address": address[:300] if address else "",
        "geoapify_place_id": place_id,
    }


def enrich_df_with_geoapify(df, *, max_brands: int = 3000) -> None:
    """
    Add geoapify_phone column to df in-place.
    Only enriches brands that still have no contact_phone after Google Places.
    Deduplicates at restaurant_id level — one API call per brand.
    """
    api_key = (os.getenv(_API_KEY_ENV) or "").strip()
    if not api_key:
        return

    NEW_COLS = ["geoapify_phone", "geoapify_address"]
    for col in NEW_COLS:
        if col not in df.columns:
            df[col] = ""

    cache = _load_cache()
    session = requests.Session()
    done = 0
    seen_rids: dict = {}

    for idx in df.index:
        rid = df.at[idx, "restaurant_id"] if "restaurant_id" in df.columns else None
        name = str(df.at[idx, "name"] if "name" in df.columns else "").strip()
        if not name or len(name) < 2:
            continue

        # Skip if already has a phone from Google Places
        existing_phone = str(df.at[idx, "contact_phone"] if "contact_phone" in df.columns else "").strip()
        if existing_phone:
            continue

        # Dedup same brand
        cache_key = str(rid) if rid is not None else name
        if cache_key in seen_rids:
            result = seen_rids[cache_key]
            _apply(df, idx, result, NEW_COLS)
            continue

        if cache_key in cache:
            result = cache[cache_key]
            seen_rids[cache_key] = result
            _apply(df, idx, result, NEW_COLS)
            continue

        if done >= max_brands:
            continue

        try:
            lat = float(df.at[idx, "latitude"] if "latitude" in df.columns else 0)
            lng = float(df.at[idx, "longitude"] if "longitude" in df.columns else 0)
        except (TypeError, ValueError):
            lat = lng = 0.0
        if lat == 0 or lng == 0:
            continue

        time.sleep(0.25)
        result = search_geoapify_phone(name, lat, lng, api_key=api_key, session=session)
        seen_rids[cache_key] = result
        cache[cache_key] = result
        done += 1
        if done % 50 == 0:
            _save_cache(cache)

        _apply(df, idx, result, NEW_COLS)

    _save_cache(cache)


def _apply(df, idx, result: dict, cols: list) -> None:
    for col in cols:
        val = result.get(col, "")
        if val and not str(df.at[idx, col]).strip():
            df.at[idx, col] = val
