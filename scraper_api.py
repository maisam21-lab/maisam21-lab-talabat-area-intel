from __future__ import annotations

import asyncio
import os

import requests
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from scrape_engine import run_area_scrape

app = FastAPI(title="Talabat Area Scraper API", version="1.0.0")

# Wall clock for one /scrape (Playwright + enrichment). Render free tier often ~100s HTTP limit; set env per plan.
_SCRAPE_WALL_SEC = float(os.getenv("SCRAPER_WALL_CLOCK_SEC", "120"))


def verify_api_key(x_api_key: str | None) -> None:
    expected = os.getenv("SCRAPER_API_KEY", "").strip()
    # If no key is configured, auth is disabled (useful for local dev).
    if not expected:
        return
    if not x_api_key or x_api_key.strip() != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


class ScrapeRequest(BaseModel):
    pin_lat: float
    pin_lng: float
    radius_km: float = Field(default=5.0, ge=1.0, le=30.0)
    # Wider spacing + low concurrency defaults reduce Render 502/timeouts on long runs.
    spacing_km: float = Field(default=1.5, ge=0.5, le=3.0)
    concurrency: int = Field(default=1, ge=1, le=6)
    # "live" = exclude closed (includes unknown/open); "all" = no filter; "closed" = closed only.
    status_filter: str = Field(default="live")
    just_landed_only: bool = False
    scroll_rounds: int = Field(default=18, ge=2, le=60)
    scroll_wait_ms: int = Field(default=900, ge=400, le=3000)
    # More points = longer runs; omit to use MAX_SCRAPE_SAMPLE_POINTS env (default 1).
    max_sample_points: int | None = Field(default=None, ge=1, le=200)


class GeocodeRequest(BaseModel):
    query: str


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Nominatim requires a valid User-Agent (https://operations.osmfoundation.org/policies/nominatim/).
_DEFAULT_NOMINATIM_UA = "TalabatAreaIntel/1.0 (+https://github.com/maisam21-lab/maisam21-lab-talabat-area-intel)"


def _nominatim_enabled() -> bool:
    return os.getenv("GEOCODE_FALLBACK_NOMINATIM", "1").strip().lower() not in ("0", "false", "no", "off")


def _geocode_nominatim(query: str) -> dict | None:
    """OpenStreetMap Nominatim search, UAE-only. English-friendly display names."""
    q = query.strip()
    if not q:
        return None
    ua = (os.getenv("NOMINATIM_USER_AGENT") or "").strip() or _DEFAULT_NOMINATIM_UA
    headers = {"User-Agent": ua, "Accept-Language": "en"}
    for qstr in (f"{q}, United Arab Emirates", f"{q}, UAE", q):
        try:
            resp = requests.get(
                NOMINATIM_URL,
                params={
                    "q": qstr,
                    "format": "json",
                    "limit": 1,
                    "countrycodes": "ae",
                    "accept-language": "en",
                },
                headers=headers,
                timeout=18,
            )
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError):
            continue
        if not data:
            continue
        top = data[0]
        lat_s, lon_s = top.get("lat"), top.get("lon")
        if lat_s is None or lon_s is None:
            continue
        try:
            lat, lng = float(lat_s), float(lon_s)
        except (TypeError, ValueError):
            continue
        if not (-90 < lat < 90 and -180 < lng < 180):
            continue
        label = str(top.get("display_name") or qstr).strip()
        return {
            "lat": lat,
            "lng": lng,
            "formatted_address": label,
        }
    return None


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/geocode")
def geocode(payload: GeocodeRequest, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    if not payload.query.strip():
        raise HTTPException(status_code=400, detail="query is required")

    query = payload.query.strip()
    attempts = [query, f"{query}, UAE"]
    google_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    google_last_error: str | None = None

    try:
        if google_key:
            for q in attempts:
                zero_for_this_q = False
                for params in (
                    {
                        "address": q,
                        "key": google_key,
                        "region": "ae",
                        "components": "country:AE",
                        "language": "en",
                    },
                    {"address": q, "key": google_key, "region": "ae", "language": "en"},
                ):
                    g_resp = requests.get(
                        "https://maps.googleapis.com/maps/api/geocode/json",
                        params=params,
                        timeout=20,
                    )
                    g_resp.raise_for_status()
                    g_data = g_resp.json()
                    status = g_data.get("status")
                    results = g_data.get("results") or []
                    if status == "OK" and results:
                        top = results[0]
                        loc = (top.get("geometry") or {}).get("location") or {}
                        if loc.get("lat") is not None and loc.get("lng") is not None:
                            return {
                                "ok": True,
                                "provider": "google",
                                "result": {
                                    "lat": float(loc["lat"]),
                                    "lng": float(loc["lng"]),
                                    "formatted_address": str(top.get("formatted_address") or q).strip(),
                                },
                            }
                    if status == "ZERO_RESULTS":
                        zero_for_this_q = True
                        break
                    if status == "INVALID_REQUEST" and "components" in params:
                        continue
                    google_last_error = str(g_data.get("error_message") or status or "google_geocode_error")
                    break
                if zero_for_this_q:
                    continue
                if google_last_error is not None:
                    break

        if _nominatim_enabled():
            nom = _geocode_nominatim(query)
            if nom:
                out: dict = {
                    "ok": True,
                    "provider": "nominatim",
                    "result": nom,
                }
                if google_last_error:
                    out["note"] = (
                        "Google Geocoding was not used successfully; coordinates from OpenStreetMap Nominatim. "
                        "Enable Geocoding API on your Google project for primary results."
                    )
                elif not google_key:
                    out["note"] = (
                        "No GOOGLE_MAPS_API_KEY; using OpenStreetMap Nominatim. "
                        "Set a Google key + enable Geocoding API for Google results."
                    )
                return out

        hint_parts = [
            "No geocoding result. ",
        ]
        if google_last_error:
            hint_parts.append(
                f"Google: {google_last_error} — In Google Cloud Console open APIs & Services → Library, "
                "search **Geocoding API**, click **Enable**, and ensure billing is active if required. "
                "Your key must allow the Geocoding API (API key restrictions). "
            )
        elif google_key:
            hint_parts.append("Google returned no results for this query. ")
        else:
            hint_parts.append("GOOGLE_MAPS_API_KEY is not set on the API service. ")
        hint_parts.append(
            "You can also rely on the OSM fallback: keep GEOCODE_FALLBACK_NOMINATIM=1 (default) on the API."
        )

        err_payload: dict = {
            "ok": False,
            "provider": "none",
            "result": None,
            "error": "No candidates from providers",
            "hint": "".join(hint_parts),
        }
        if google_last_error is not None:
            err_payload["google_error"] = google_last_error
        return err_payload
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Geocode failed: {exc}") from exc


@app.post("/scrape")
async def scrape(payload: ScrapeRequest, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    if payload.status_filter not in {"all", "live", "closed"}:
        raise HTTPException(status_code=400, detail="status_filter must be one of: all, live, closed")
    try:
        df = await asyncio.wait_for(
            run_area_scrape(
                pin_lat=payload.pin_lat,
                pin_lng=payload.pin_lng,
                radius_km=payload.radius_km,
                spacing_km=payload.spacing_km,
                concurrency=payload.concurrency,
                status_filter=payload.status_filter,
                just_landed_only=payload.just_landed_only,
                scroll_rounds=payload.scroll_rounds,
                scroll_wait_ms=payload.scroll_wait_ms,
                progress_cb=None,
                max_sample_points=payload.max_sample_points,
            ),
            timeout=_SCRAPE_WALL_SEC,
        )
        records = df.to_dict(orient="records")
        return {"count": len(records), "records": records}
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=(
                f"Scrape exceeded {_SCRAPE_WALL_SEC:.0f}s wall clock (SCRAPER_WALL_CLOCK_SEC). "
                "Lower Grid sample points in the app, set RESTAURANT_DETAIL_ENRICH_MAX smaller on the API, "
                "or raise SCRAPER_WALL_CLOCK_SEC / upgrade Render if your tier allows longer HTTP requests."
            ),
        ) from None
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Scrape failed: {exc}") from exc
