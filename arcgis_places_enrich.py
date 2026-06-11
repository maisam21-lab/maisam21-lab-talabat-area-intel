"""
Enrich restaurant records with phone numbers from ArcGIS Places API.
Uses Esri's global POI database — searches by name + coordinates, fetches ContactInfo.
Results cached to disk. Slots in before Geoapify as the primary phone enrichment source.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_CACHE_PATH = Path(os.getenv("ARCGIS_PLACES_CACHE_PATH", "/app/analyze_jobs/arcgis_places_cache.json"))
_SEARCH_URL  = "https://places-api.arcgis.com/arcgis/rest/services/places-service/v1/places/near-point"
_DETAILS_URL = "https://places-api.arcgis.com/arcgis/rest/services/places-service/v1/places/{place_id}"

# ArcGIS Places food & beverage category IDs
_FOOD_CATEGORIES = "13000,13065,13064,13035,13034,13338"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_api_key() -> str:
    key = os.getenv("ARCGIS_API_KEY", "").strip()
    if not key:
        # Fallback: read from .env file
        env_path = Path(__file__).parent / ".env"
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("ARCGIS_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
        except Exception:
            pass
    return key


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


def _normalise_name(name: str) -> str:
    """Lowercase, strip punctuation for name matching."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _name_score(a: str, b: str) -> float:
    """Simple token overlap score 0-1."""
    na, nb = _normalise_name(a), _normalise_name(b)
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.8
    # token overlap
    ta = set(re.findall(r"[a-z0-9]+", na))
    tb = set(re.findall(r"[a-z0-9]+", nb))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))


def _fetch_phone(place_id: str, api_key: str) -> str:
    """Fetch ContactInfo for a place and return the telephone number."""
    try:
        r = requests.get(
            _DETAILS_URL.format(place_id=place_id),
            params={
                "requestedFields": "ContactInfo",
                "token": api_key,
                "f": "json",
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        contact = (data.get("result") or data).get("contactInfo") or {}
        phone = (contact.get("telephone") or "").strip()
        return phone
    except Exception:
        return ""


def _search_place(name: str, lat: float, lng: float, api_key: str) -> str | None:
    """Search ArcGIS Places near coordinates, return best-matching place_id."""
    try:
        r = requests.get(
            _SEARCH_URL,
            params={
                "x": lng,
                "y": lat,
                "radius": 400,
                "categoryIds": _FOOD_CATEGORIES,
                "pageSize": 10,
                "token": api_key,
                "f": "json",
            },
            timeout=10,
        )
        r.raise_for_status()
        results = r.json().get("results") or []
        best_id, best_score = None, 0.4  # minimum threshold
        for place in results:
            score = _name_score(name, place.get("name", ""))
            if score > best_score:
                best_score = score
                best_id = place.get("placeId")
        return best_id
    except Exception:
        return None


# ── Main enrichment function ──────────────────────────────────────────────────

def enrich_df_with_arcgis_places(df: "pd.DataFrame", max_brands: int = 3000) -> None:
    """
    Enrich `df` in-place with phone numbers from ArcGIS Places.
    Only processes rows where contact_phone is empty.
    """
    import pandas as pd  # noqa: F401

    api_key = _get_api_key()
    if not api_key:
        logger.info("ArcGIS Places enrichment skipped — ARCGIS_API_KEY not configured")
        return

    if "contact_phone" not in df.columns:
        df["contact_phone"] = ""
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return

    cache = _load_cache()
    enriched = 0

    # Work brand-by-brand, skip those already enriched
    brand_col = "restaurant_id" if "restaurant_id" in df.columns else None
    processed_brands: set = set()

    for idx, row in df.iterrows():
        if enriched >= max_brands:
            break

        existing_phone = str(row.get("contact_phone") or "").strip()
        if existing_phone:
            continue  # already has a phone

        lat = row.get("latitude")
        lng = row.get("longitude")
        name = str(row.get("name") or "").strip()
        if not name or not lat or not lng:
            continue

        brand_key = str(row.get(brand_col)) if brand_col else name
        if brand_key in processed_brands:
            # Propagate phone found for this brand to other rows
            cached_phone = cache.get(f"brand:{brand_key}", {}).get("phone", "")
            if cached_phone:
                df.at[idx, "contact_phone"] = cached_phone
            continue
        processed_brands.add(brand_key)

        cache_key = f"{_normalise_name(name)}:{round(float(lat),3)}:{round(float(lng),3)}"
        if cache_key in cache:
            phone = cache[cache_key].get("phone", "")
        else:
            place_id = _search_place(name, float(lat), float(lng), api_key)
            phone = _fetch_phone(place_id, api_key) if place_id else ""
            cache[cache_key] = {"phone": phone, "ts": time.time()}
            # Also cache by brand key for propagation
            if brand_col:
                cache[f"brand:{brand_key}"] = {"phone": phone, "ts": time.time()}
            time.sleep(0.05)  # ~20 req/s — well within ArcGIS rate limits

        if phone:
            df.at[idx, "contact_phone"] = phone
            enriched += 1

    _save_cache(cache)
    logger.info("ArcGIS Places enrichment: %d phones added", enriched)
