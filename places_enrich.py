"""Optional backfill of phone, business name, and place_id on Talabat rows via Google Places (HTTP).

Requires GOOGLE_MAPS_API_KEY with Places API enabled. Runs when the scrape request sets
``google_places_enrich: true`` (then env GOOGLE_PLACES_ENRICH is ignored), or when
``GOOGLE_PLACES_ENRICH=1`` and the request does not force false.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
import requests

from geo_utils import haversine_km
from models import RestaurantRecord

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Persistent cache — survives container restarts (stored on the host-mounted volume).
_CACHE_PATH = Path(os.getenv("GOOGLE_PLACES_CACHE_PATH", "/app/analyze_jobs/google_places_cache.json"))


def _load_disk_cache() -> dict:
    try:
        if _CACHE_PATH.exists():
            return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_disk_cache(cache: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def google_places_enrich_effective(force: bool | None) -> bool:
    """Whether Places enrichment should run for this scrape (per-request override or env)."""
    if force is False:
        return False
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        return False
    if force is True:
        return True
    return _truthy(os.getenv("GOOGLE_PLACES_ENRICH"))


def _pick_closest_result(
    results: list[dict],
    ref_lat: float,
    ref_lng: float,
    max_km: float,
) -> dict | None:
    if not results:
        return None
    best: dict | None = None
    best_d = 1e9
    for pl in results[:12]:
        loc = (pl.get("geometry") or {}).get("location") or {}
        plat, plng = loc.get("lat"), loc.get("lng")
        if plat is None or plng is None:
            continue
        d = haversine_km(ref_lat, ref_lng, float(plat), float(plng))
        if d < best_d:
            best_d, best = d, pl
    if best is not None and best_d <= max_km:
        return best
    return None


def enrich_records_with_google_places(records: list[RestaurantRecord], *, force: bool | None = None) -> None:
    if not records:
        return
    if not google_places_enrich_effective(force):
        return
    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        return

    max_rows = int(os.getenv("GOOGLE_PLACES_ENRICH_MAX", "180"))
    radius_m = int(os.getenv("GOOGLE_PLACES_SEARCH_RADIUS_M", "5000"))
    max_km = float(os.getenv("GOOGLE_PLACES_MAX_DISTANCE_KM", "3.0"))
    include_ids = _truthy(os.getenv("GOOGLE_PLACES_ENRICH_INCLUDE_IDS"))

    session = requests.Session()
    done = 0
    for row in records:
        if done >= max_rows:
            break
        has_phone = bool((row.contact_phone or "").strip())
        has_legal = bool((row.legal_name or "").strip())
        has_pid = bool((row.google_place_id or "").strip())
        need = (not has_phone) or (not has_legal) or (include_ids and not has_pid)
        if not need:
            continue

        name = (row.restaurant_name or "").strip()
        if len(name) < 2:
            continue
        parts = [name]
        if (row.branch_name or "").strip():
            parts.append(str(row.branch_name).strip())
        if (row.area_label or "").strip():
            parts.append(str(row.area_label).strip())
        parts.append("United Arab Emirates")
        query = " ".join(parts)

        params: dict[str, str] = {"query": query, "key": key, "language": "en"}
        try:
            ref_lat = float(row.lat)
            ref_lng = float(row.lng)
        except (TypeError, ValueError):
            continue
        params["location"] = f"{ref_lat},{ref_lng}"
        params["radius"] = str(radius_m)

        try:
            r = session.get(TEXT_SEARCH_URL, params=params, timeout=14)
            r.raise_for_status()
            payload = r.json()
        except (requests.RequestException, ValueError):
            time.sleep(0.15)
            continue

        status = payload.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            time.sleep(0.25)
            continue
        results = payload.get("results") or []
        if not results:
            time.sleep(0.1)
            continue

        picked = _pick_closest_result(results, ref_lat, ref_lng, max_km)
        if picked is None:
            time.sleep(0.1)
            continue
        place_id = (picked.get("place_id") or "").strip()
        if not place_id:
            time.sleep(0.1)
            continue

        fields = ",".join(
            [
                "place_id",
                "name",
                "formatted_phone_number",
                "international_phone_number",
                "business_status",
                "formatted_address",
                "website",
                "url",
                "types",
                "geometry",
                "editorial_summary",
                "opening_hours",
            ]
        )
        try:
            dr = session.get(
                DETAILS_URL,
                params={
                    "place_id": place_id,
                    "fields": fields,
                    "key": key,
                    "language": "en",
                },
                timeout=14,
            )
            dr.raise_for_status()
            detail = dr.json()
        except (requests.RequestException, ValueError):
            done += 1
            time.sleep(0.15)
            continue

        if detail.get("status") != "OK":
            done += 1
            time.sleep(0.15)
            continue
        res = detail.get("result") or {}
        if (res.get("business_status") or "OPERATIONAL") == "CLOSED_PERMANENTLY":
            time.sleep(0.1)
            continue

        phone = (res.get("international_phone_number") or res.get("formatted_phone_number") or "").strip()
        gname = (res.get("name") or "").strip()
        faddr = (res.get("formatted_address") or "").strip()
        gweb = (res.get("website") or "").strip()
        maps_url = (res.get("url") or "").strip()
        types = res.get("types") or []
        type_str = ""
        if isinstance(types, list) and types:
            type_str = ", ".join(str(t) for t in types if t)[:400]
        ed = res.get("editorial_summary") or {}
        ed_text = ""
        if isinstance(ed, dict):
            ed_text = (ed.get("overview") or "").strip()
        elif isinstance(ed, str):
            ed_text = ed.strip()
        oh = res.get("opening_hours") or {}
        oh_snip = ""
        if isinstance(oh, dict):
            wt = oh.get("weekday_text")
            if isinstance(wt, list) and wt:
                oh_snip = " | ".join(str(x) for x in wt if x)[:800]

        row.google_place_id = place_id
        if gname:
            row.google_maps_name = gname
            if not has_legal:
                row.legal_name = gname
        if phone and not has_phone:
            row.contact_phone = phone
        if faddr:
            row.google_formatted_address = faddr[:500]
        if maps_url:
            row.google_maps_link = maps_url[:500]
        if type_str:
            row.google_primary_type = type_str[:400]
        loc = (res.get("geometry") or {}).get("location") or {}
        try:
            gla, gln = loc.get("lat"), loc.get("lng")
            if gla is not None and gln is not None:
                row.lat = float(gla)
                row.lng = float(gln)
        except (TypeError, ValueError):
            pass
        # Listing-only rows often have empty Talabat ``cuisines``; Places types are a useful fallback for analytics.
        if not (row.cuisines or "").strip() and isinstance(types, list) and types:
            skip = {"point_of_interest", "establishment"}
            labels = [
                str(t).replace("_", " ").strip().title()
                for t in types
                if t and str(t).lower() not in skip
            ]
            if labels:
                row.cuisines = ", ".join(labels[:6])[:400]
        if gweb:
            row.google_business_website = gweb[:500]
            if not (row.vendor_website or "").strip():
                row.vendor_website = gweb[:500]
        if ed_text and len(ed_text) > len((row.vendor_description or "").strip()):
            row.vendor_description = ed_text[:1500]
        if oh_snip and len(oh_snip) > len((row.opening_hours_snippet or "").strip()):
            row.opening_hours_snippet = oh_snip

        done += 1
        time.sleep(0.12)


def enrich_df_with_google_places(
    df: "pd.DataFrame",
    fallback_lat: float,
    fallback_lng: float,
    *,
    max_brands: int = 300,
) -> "pd.DataFrame":
    """
    Enrich a raw-records DataFrame with Google Places data (phone, address, maps link, legal name).

    Works at brand level (one API call per restaurant_id) to minimise cost.
    Adds/fills columns: contact_phone, legal_name, google_address, google_maps_link, data_source.
    Records without enrichment get data_source='Talabat'.
    Enriched records get data_source='Talabat + Google Maps'.
    """
    import pandas as pd

    key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

    # Always stamp the source column first
    df = df.copy()
    for col in ["contact_phone", "legal_name", "google_address", "google_maps_link", "data_source"]:
        if col not in df.columns:
            df[col] = ""
    df["data_source"] = "Talabat"

    if not key:
        return df

    session = requests.Session()
    brand_cache: dict = {}  # restaurant_id → enrichment dict or None (in-memory this run)
    disk_cache: dict = _load_disk_cache()  # persisted across all runs
    done = 0

    for idx in df.index:
        row = df.loc[idx]
        rid = row.get("restaurant_id")
        name = str(row.get("name") or "").strip()
        if not name or len(name) < 2:
            continue

        # 1. Check in-memory cache (same brand seen earlier this run)
        if rid is not None and rid in brand_cache:
            result = brand_cache[rid]
            if result:
                for col, val in result.items():
                    if val and not str(df.at[idx, col]).strip():
                        df.at[idx, col] = val
                df.at[idx, "data_source"] = "Talabat + Google Maps"
            continue

        # 2. Check persistent disk cache (enriched in a previous run — free)
        _cache_key = str(rid) if rid is not None else name
        if _cache_key in disk_cache:
            result = disk_cache[_cache_key]
            brand_cache[rid] = result
            if result:
                for col, val in result.items():
                    if val and not str(df.at[idx, col]).strip():
                        df.at[idx, col] = val
                df.at[idx, "data_source"] = "Talabat + Google Maps"
            continue

        if done >= max_brands:
            continue

        # Reference coordinates — use vendor coords if available, else pin fallback
        try:
            vlat = float(row.get("latitude") or 0)
            vlng = float(row.get("longitude") or 0)
        except (TypeError, ValueError):
            vlat = vlng = 0.0
        ref_lat = vlat if vlat != 0.0 else fallback_lat
        ref_lng = vlng if vlng != 0.0 else fallback_lng

        # Google Places Text Search
        area_hint = str(row.get("area") or "").strip()
        query = f"{name} {area_hint} UAE".strip()
        params: dict = {
            "query": query,
            "key": key,
            "language": "en",
            "location": f"{ref_lat},{ref_lng}",
            "radius": "6000",
        }
        try:
            r = session.get(TEXT_SEARCH_URL, params=params, timeout=14)
            r.raise_for_status()
            payload = r.json()
        except (requests.RequestException, ValueError):
            time.sleep(0.2)
            continue

        if payload.get("status") not in ("OK", "ZERO_RESULTS"):
            time.sleep(0.3)
            continue

        results = payload.get("results") or []
        picked = _pick_closest_result(results, ref_lat, ref_lng, max_km=5.0)
        if not picked:
            brand_cache[rid] = None
            time.sleep(0.1)
            continue

        place_id = (picked.get("place_id") or "").strip()
        if not place_id:
            brand_cache[rid] = None
            time.sleep(0.1)
            continue

        # Google Places Details
        fields = "place_id,name,international_phone_number,formatted_phone_number,formatted_address,website,url,types"
        try:
            dr = session.get(
                DETAILS_URL,
                params={"place_id": place_id, "fields": fields, "key": key, "language": "en"},
                timeout=14,
            )
            dr.raise_for_status()
            detail = dr.json()
        except (requests.RequestException, ValueError):
            done += 1
            time.sleep(0.2)
            continue

        if detail.get("status") != "OK":
            done += 1
            brand_cache[rid] = None
            time.sleep(0.2)
            continue

        res = detail.get("result") or {}
        phone = (res.get("international_phone_number") or res.get("formatted_phone_number") or "").strip()
        gname = (res.get("name") or "").strip()
        faddr = (res.get("formatted_address") or "").strip()
        maps_url = (res.get("url") or "").strip()

        enrichment = {
            "contact_phone": phone,
            "legal_name": gname,
            "google_address": faddr,
            "google_maps_link": maps_url,
        }
        brand_cache[rid] = enrichment
        disk_cache[_cache_key] = enrichment  # persist for future runs
        if done % 20 == 0:
            _save_disk_cache(disk_cache)  # save every 20 new enrichments

        for col, val in enrichment.items():
            if val and not str(df.at[idx, col]).strip():
                df.at[idx, col] = val
        df.at[idx, "data_source"] = "Talabat + Google Maps"

        done += 1
        time.sleep(0.12)

    _save_disk_cache(disk_cache)  # final save
    return df
