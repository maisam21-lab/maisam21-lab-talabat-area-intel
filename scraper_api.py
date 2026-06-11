from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time as _time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import requests
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from google_coverage import fetch_google_nearby_restaurants, google_coverage_enabled
from foursquare_coverage import fetch_foursquare_nearby_restaurants, foursquare_coverage_enabled
from pin_validation import assert_client_pin_matches_body, validate_scrape_pin
from listing_harvest import country_path_slug, default_listing_url_for_slug, harvest_vendor_urls
from scrape_network import outbound_proxy_source
from scrape_job_store import job_store_dir, load_job_record, persist_job_record
from scrape_engine import run_area_scrape
from uae_cities import resolve_city
from area_page_scraper import (
    scrape_vendors_near_pin as _area_scrape_vendors_near_pin,
    scrape_area_vendors as _scrape_area_vendors,
    vendor_to_row,
    UAE_AREA_REGISTRY,
    find_nearest_registry_area,
)

_AREA_INTEL_LOG_HANDLER_ATTR = "_talabat_area_intel_stderr"


@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    """Ensure ``talabat_area_intel.*`` INFO lines reach Docker logs."""
    talabat = logging.getLogger("talabat_area_intel")
    talabat.setLevel(logging.INFO)
    if not any(getattr(h, _AREA_INTEL_LOG_HANDLER_ATTR, False) for h in talabat.handlers):
        h = logging.StreamHandler(sys.stderr)
        setattr(h, _AREA_INTEL_LOG_HANDLER_ATTR, True)
        h.setLevel(logging.INFO)
        h.setFormatter(logging.Formatter("%(message)s"))
        talabat.addHandler(h)
        talabat.propagate = False
    yield


app = FastAPI(title="Talabat Area Scraper API", version="1.0.0", lifespan=_app_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_STATIC_DIR = Path(__file__).parent / "static"
_STATIC_DIR.mkdir(exist_ok=True)
app.mount("/ui", StaticFiles(directory=str(_STATIC_DIR), html=True), name="static")

_ANALYZE_JOBS: dict[str, dict] = {}
_ANALYZE_JOBS_LOCK = threading.Lock()
_ANALYZE_JOBS_DIR = Path(__file__).parent / "analyze_jobs"
_ANALYZE_JOBS_DIR.mkdir(exist_ok=True)


def _persist_job(job_id: str, job: dict) -> None:
    """Write job state to disk so it survives container restarts."""
    try:
        import json
        data = {k: v for k, v in job.items()}
        (_ANALYZE_JOBS_DIR / f"job_{job_id}.json").write_text(
            json.dumps(data, default=str), encoding="utf-8"
        )
    except Exception:
        pass


def _load_persisted_jobs() -> None:
    """On startup reload completed/failed jobs; recover interrupted ones whose Excel was already written."""
    import json
    for p in _ANALYZE_JOBS_DIR.glob("job_*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            job_id = data.get("job_id")
            if not job_id:
                continue
            if data.get("status") in ("running", "queued"):
                # If Excel was already written before the restart, the job is actually complete.
                excel_path = data.get("output_file") or ""
                if excel_path and Path(excel_path).exists():
                    data["status"] = "complete"
                    if not data.get("result_summary"):
                        data["result_summary"] = {
                            "brands": 0, "raw_rows": 0, "pins": len(data.get("pins", [])),
                            "just_landed_count": 0, "vendor_coords": [], "vendor_points": [],
                            "pin_errors": {},
                            "note": "Recovered after server restart — Excel report is ready to download.",
                        }
                else:
                    data["status"] = "failed"
                    data["error"] = "Server restarted while job was running — please submit again."
                p.write_text(json.dumps(data, default=str), encoding="utf-8")
            with _ANALYZE_JOBS_LOCK:
                _ANALYZE_JOBS[job_id] = data
        except Exception:
            pass


_load_persisted_jobs()
logger = logging.getLogger("talabat_area_intel.api")
_JOB_RESULTS: dict[str, dict] = {}
_JOB_LOCK = asyncio.Lock()
# One scrape at a time by default: parallel Playwright jobs on a small VM cause OOM, zero rows, and “frozen” APIs.
_SCRAPE_MAX_CONCURRENT_SCRAPES = max(1, int(os.getenv("SCRAPER_MAX_CONCURRENT_SCRAPES", "1")))
_scrape_execution_semaphore = asyncio.Semaphore(_SCRAPE_MAX_CONCURRENT_SCRAPES)


@app.middleware("http")
async def add_request_id_middleware(request: Request, call_next):
    rid = (request.headers.get("X-Request-ID") or request.headers.get("X-Correlation-ID") or "").strip()
    if not rid:
        rid = uuid.uuid4().hex
    request.state.request_id = rid
    try:
        response = await call_next(request)
    except Exception:
        logger.error("unhandled_exception request_id=%s\n%s", rid, traceback.format_exc())
        raise
    response.headers["X-Request-ID"] = rid
    return response


@app.exception_handler(HTTPException)
async def http_exception_json(request: Request, exc: HTTPException):
    rid = getattr(request.state, "request_id", "")
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"ok": False, "error": detail, "request_id": rid},
        headers={"X-Request-ID": rid} if rid else None,
    )


@app.exception_handler(Exception)
async def unhandled_exception_json(request: Request, exc: Exception):
    rid = getattr(request.state, "request_id", "")
    logger.error("unhandled_api_error request_id=%s error=%s\n%s", rid, exc, traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": "Internal server error", "request_id": rid},
        headers={"X-Request-ID": rid} if rid else None,
    )

# Geocoding: Google is optional. Without GCP, omit GOOGLE_MAPS_API_KEY or set GEOCODE_USE_GOOGLE=0 and use
# OpenStreetMap Nominatim (GEOCODE_FALLBACK_NOMINATIM=1 default).

# Wall clock for one /scrape: see _effective_scrape_timeout_sec() (env + optional JSON override per request).


def verify_api_key(x_api_key: str | None) -> None:
    expected = os.getenv("SCRAPER_API_KEY", "").strip()
    # If no key is configured, auth is disabled (useful for local dev).
    if not expected:
        return
    if not x_api_key or x_api_key.strip() != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


class ScrapeRequest(BaseModel):
    pin_lat: float = Field(default=25.2048, description="Search center latitude (geometry always follows this pin)")
    pin_lng: float = Field(default=55.2708, description="Search center longitude (geometry always follows this pin)")
    radius_km: float = Field(default=10.0, ge=5.0, le=10.0)
    # Hex grid spacing; lower values increase coverage and runtime.
    spacing_km: float = Field(default=1.8, ge=0.35, le=3.0)
    concurrency: int = Field(default=3, ge=1, le=6)
    # "live" = drop closed rows (keeps open + unset/empty status); "all" = no filter; "closed" = closed only.
    status_filter: str = Field(
        default="live",
        description="live | all | closed — see API docs; not the same as Talabat Pro 'live' badge.",
    )
    just_landed_only: bool = Field(
        default=False,
        description="Attempts Talabat 'Just Landed' listing filter; result rows also restricted to new signals when True.",
    )
    scroll_rounds: int = Field(default=6, ge=2, le=60)
    scroll_wait_ms: int = Field(default=500, ge=200, le=3000)
    # More points = longer runs; omit to use MAX_SCRAPE_SAMPLE_POINTS env (default 6).
    max_sample_points: int | None = Field(default=20, ge=1, le=400)
    high_volume: bool = Field(
        default=False,
        description="Dense geo grid + cuisine listing pages for large unique-vendor counts; slow — raise SCRAPER_WALL_CLOCK_SEC.",
    )
    # KitchenPark / multi-city expansion: dubai | sharjah | abudhabi | alain | ajman — overrides pin to city center.
    city: str | None = Field(
        default=None,
        description="Optional UAE city label (scrape_city). Does NOT override pin_lat/pin_lng — geometry follows the pin.",
    )
    # False = keep duplicate vendor URLs from different grid samples (branches / areas for acquisition analysis).
    dedupe_by_vendor_url: bool = Field(
        default=False,
        description="If true, collapse to one row per vendor URL. If false, keep all listing rows.",
    )
    scrape_target_label: str | None = Field(
        default=None,
        description="Optional micro-market label (e.g. 'Dubai Marina') stored on each row; use with custom pin + radius.",
    )
    scrape_wall_clock_sec: int | None = Field(
        default=None,
        ge=60,
        le=3600,
        description="Max seconds for this /scrape call (overrides SCRAPER_WALL_CLOCK_SEC when set). Use 900 for heavy runs.",
    )
    client_asserted_pin_lat: float | None = Field(
        default=None,
        description="Optional: UI 'run pin' lat — must equal pin_lat when sent (catches split session state).",
    )
    client_asserted_pin_lng: float | None = Field(
        default=None,
        description="Optional: UI 'run pin' lng — must equal pin_lng when sent.",
    )
    google_places_enrich: bool | None = Field(
        default=None,
        description="If true, run Google Places enrichment when GOOGLE_MAPS_API_KEY is set. If false, skip. If null, use env GOOGLE_PLACES_ENRICH.",
    )
    enrich: bool = Field(
        default=False,
        description="If false, skip vendor/google/reverse enrichment and return listing rows only.",
    )


def _effective_scrape_timeout_sec(payload: ScrapeRequest) -> float:
    """Env SCRAPER_WALL_CLOCK_SEC (default 600s) unless the client sends scrape_wall_clock_sec (60–3600)."""
    cap = 3600.0
    env_v = float(os.getenv("SCRAPER_WALL_CLOCK_SEC", "600"))
    env_v = max(60.0, min(env_v, cap))
    if payload.scrape_wall_clock_sec is not None:
        return max(60.0, min(float(payload.scrape_wall_clock_sec), cap))
    return env_v


class ListingHarvestRequest(BaseModel):
    country: str = Field(default="uae", description="uae, egypt, or Talabat country path slug")
    listing_url: str | None = Field(default=None, description="Override restaurants listing URL")
    max_next: int = Field(default=40, ge=0, le=120, description="Max pagination 'next' clicks")
    harvest_wall_clock_sec: int | None = Field(
        default=300,
        ge=60,
        le=1200,
        description="Hard cap for the Playwright harvest (seconds)",
    )


class GeocodeRequest(BaseModel):
    query: str


class GoogleCoverageRequest(BaseModel):
    pin_lat: float = Field(default=25.2048)
    pin_lng: float = Field(default=55.2708)
    radius_km: float = Field(default=10.0, ge=5.0, le=10.0)


class FoursquareCoverageRequest(BaseModel):
    pin_lat: float = Field(default=25.2048)
    pin_lng: float = Field(default=55.2708)
    radius_km: float = Field(default=10.0, ge=5.0, le=10.0)


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Nominatim requires a valid User-Agent (https://operations.osmfoundation.org/policies/nominatim/).
_DEFAULT_NOMINATIM_UA = "TalabatAreaIntel/1.0 (+https://github.com/maisam21-lab/maisam21-lab-talabat-area-intel)"


def _env_truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _env_nonempty(key: str, default: str) -> str:
    """Treat unset or empty Docker env (KEY=) as missing so defaults apply."""
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip()


def _int_env_nonempty(key: str, default: int) -> int:
    s = _env_nonempty(key, str(default))
    try:
        return int(s)
    except ValueError:
        return default


def _nominatim_enabled() -> bool:
    return os.getenv("GEOCODE_FALLBACK_NOMINATIM", "1").strip().lower() not in ("0", "false", "no", "off")


def _google_geocode_enabled() -> bool:
    """Set GEOCODE_USE_GOOGLE=0 to skip Google entirely (OpenStreetMap Nominatim only; no GCP needed)."""
    return os.getenv("GEOCODE_USE_GOOGLE", "1").strip().lower() not in ("0", "false", "no", "off")


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


@app.get("/config")
async def ui_config():
    """Public endpoint — returns bootstrap config for the NAMAA frontend (no auth required)."""
    key = os.getenv("SCRAPER_API_KEY", "").strip()
    return JSONResponse({"has_key": bool(key), "api_key": key})


@app.get("/health/scrape-config")
def scrape_config(x_api_key: str | None = Header(default=None)) -> dict:
    """Non-secret guardrails and geocode toggles — use to verify deployed API settings (requires same API key as /scrape)."""
    verify_api_key(x_api_key)
    return {
        "ok": True,
        "google_maps_key_configured": bool(os.getenv("GOOGLE_MAPS_API_KEY", "").strip()),
        "foursquare_key_configured": bool(os.getenv("FOURSQUARE_API_KEY", "").strip()),
        "geocode_use_google": _google_geocode_enabled(),
        "geocode_fallback_nominatim": _nominatim_enabled(),
        "scraper_wall_clock_sec_default": float(os.getenv("SCRAPER_WALL_CLOCK_SEC", "600")),
        "scraper_min_radius_km": float(os.getenv("SCRAPER_MIN_RADIUS_KM", "5")),
        "scraper_max_radius_km": float(os.getenv("SCRAPER_MAX_RADIUS_KM", "10")),
        "scraper_max_sample_points_cap_api": int(os.getenv("SCRAPER_MAX_SAMPLE_POINTS_CAP_API", "400")),
        "max_scrape_sample_points_default": int(os.getenv("MAX_SCRAPE_SAMPLE_POINTS", "6")),
        "restaurant_detail_enrich_max_default": _int_env_nonempty("RESTAURANT_DETAIL_ENRICH_MAX", 12),
        "scraper_per_point_timeout_sec": float(os.getenv("SCRAPER_PER_POINT_TIMEOUT_SEC", "120")),
        "scraper_listing_fast_path": os.getenv("SCRAPER_LISTING_FAST_PATH", "0").strip(),
        "scraper_humanize": os.getenv("SCRAPER_HUMANIZE", "0").strip(),
        "google_places_enrich_env": os.getenv("GOOGLE_PLACES_ENRICH", "0").strip(),
        "scraper_listing_page_pagination": _env_nonempty("SCRAPER_LISTING_PAGE_PAGINATION", "1"),
        "scraper_listing_max_pages": _int_env_nonempty("SCRAPER_LISTING_MAX_PAGES", 35),
        "listing_harvest_response_max_urls": int(os.getenv("LISTING_HARVEST_RESPONSE_MAX_URLS", "2500")),
        "scraper_vendor_page_enrich": _env_truthy(os.getenv("SCRAPER_VENDOR_PAGE_ENRICH", "0")),
        "scraper_max_concurrent_scrapes": _SCRAPE_MAX_CONCURRENT_SCRAPES,
        "outbound_proxy_configured": bool(outbound_proxy_source()),
        "outbound_proxy_source": outbound_proxy_source() or None,
        "scrape_job_store_dir": str(job_store_dir()),
    }


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
        if google_key and _google_geocode_enabled():
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
                        "Coordinates from OpenStreetMap Nominatim (Google Geocoding did not return a result). "
                        "No Google Cloud account is required. Remove GOOGLE_MAPS_API_KEY or set GEOCODE_USE_GOOGLE=0 "
                        "to use OSM only."
                    )
                elif not google_key or not _google_geocode_enabled():
                    out["note"] = (
                        "Geocoding uses OpenStreetMap Nominatim only — no Google Cloud account or API key needed."
                    )
                return out

        hint_parts = [
            "No geocoding result for this query. Try a more specific place name (e.g. \"Dubai Marina, UAE\"). ",
        ]
        if google_last_error and _google_geocode_enabled():
            hint_parts.append(
                f"Google error: {google_last_error}. "
                "If you do not use Google Cloud, remove GOOGLE_MAPS_API_KEY from the API service env or set "
                "GEOCODE_USE_GOOGLE=0 so only OpenStreetMap is used. "
            )
        elif google_key and _google_geocode_enabled():
            hint_parts.append("Google returned no results for this query. ")
        elif not google_key:
            hint_parts.append("Using OSM Nominatim (no Google key). ")
        hint_parts.append(
            "Ensure GEOCODE_FALLBACK_NOMINATIM=1 (default) on the API. "
            "Geocoding does not require Google Cloud when that fallback is on."
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


@app.post("/listing-harvest")
async def listing_harvest_endpoint(
    payload: ListingHarvestRequest,
    request: Request,
    x_api_key: str | None = Header(default=None),
) -> dict:
    """Discover vendor URLs from a country restaurants listing (Playwright; same idea as the Egypt notebook)."""
    request_id = getattr(request.state, "request_id", "")
    verify_api_key(x_api_key)
    slug = country_path_slug(payload.country)
    listing = (payload.listing_url or "").strip() or default_listing_url_for_slug(slug)
    wall = float(payload.harvest_wall_clock_sec or 300)
    wall = max(60.0, min(wall, 1200.0))
    try:
        urls = await asyncio.wait_for(
            harvest_vendor_urls(
                listing,
                country_slug=slug,
                max_next=int(payload.max_next),
                headless=True,
            ),
            timeout=wall,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=(
                f"Listing harvest exceeded {wall:.0f}s — lower max_next or raise harvest_wall_clock_sec (max 1200). "
                "Hosted proxies may still cut the connection earlier."
            ),
        ) from None
    except Exception as exc:
        logger.error("listing_harvest_failed request_id=%s error=%s\n%s", request_id, exc, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Listing harvest failed: {exc}") from exc

    max_return = int(os.getenv("LISTING_HARVEST_RESPONSE_MAX_URLS", "2500"))
    truncated = len(urls) > max_return
    out_urls = urls[:max_return] if truncated else urls
    return {
        "ok": True,
        "request_id": request_id,
        "country_slug": slug,
        "listing_url": listing,
        "count_total": len(urls),
        "urls": out_urls,
        "urls_returned": len(out_urls),
        "truncated": truncated,
    }


@app.post("/google-coverage")
def google_coverage(payload: GoogleCoverageRequest, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    if not google_coverage_enabled():
        return {
            "ok": True,
            "count": 0,
            "records": [],
            "note": "Google coverage disabled or GOOGLE_MAPS_API_KEY missing on API service.",
        }
    pin_lat, pin_lng = validate_scrape_pin(payload.pin_lat, payload.pin_lng)
    rows = fetch_google_nearby_restaurants(pin_lat=pin_lat, pin_lng=pin_lng, radius_km=float(payload.radius_km))
    return {
        "ok": True,
        "count": len(rows),
        "records": rows,
        "pin_lat": pin_lat,
        "pin_lng": pin_lng,
        "radius_km": float(payload.radius_km),
    }


@app.post("/foursquare-coverage")
def foursquare_coverage(payload: FoursquareCoverageRequest, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    if not foursquare_coverage_enabled():
        return {
            "ok": True,
            "count": 0,
            "records": [],
            "note": "Foursquare coverage disabled or FOURSQUARE_API_KEY missing on API service.",
        }
    pin_lat, pin_lng = validate_scrape_pin(payload.pin_lat, payload.pin_lng)
    rows = fetch_foursquare_nearby_restaurants(pin_lat=pin_lat, pin_lng=pin_lng, radius_km=float(payload.radius_km))
    return {
        "ok": True,
        "count": len(rows),
        "records": rows,
        "pin_lat": pin_lat,
        "pin_lng": pin_lng,
        "radius_km": float(payload.radius_km),
    }


class AreaScrapeRequest(BaseModel):
    pin_lat: float = Field(default=25.1865, description="Search center latitude")
    pin_lng: float = Field(default=55.2642, description="Search center longitude")
    radius_km: float = Field(default=10.0, ge=1.0, le=30.0)
    area_id: int | None = Field(default=None, description="Talabat area ID (auto-resolved from registry if omitted)")
    area_slug: str | None = Field(default=None, description="Talabat area slug e.g. 'business-bay' (required if area_id set)")
    country: str = Field(default="uae", description="Talabat country path: uae, egypt, etc.")
    page_delay: float = Field(default=0.4, ge=0.0, le=5.0, description="Delay (seconds) between page fetches")
    max_pages: int | None = Field(default=None, ge=1, description="Cap page count (default: scrape all pages)")
    status_filter: str = Field(default="live", description="live | all | closed")
    scrape_do_token: str | None = Field(default=None, description="Optional scrape.do / proxy token")


@app.post("/scrape-area")
async def scrape_area(
    payload: AreaScrapeRequest,
    request: Request,
    x_api_key: str | None = Header(default=None),
) -> dict:
    """Scrape Talabat vendor listings for an area using __NEXT_DATA__ pagination (no Playwright)."""
    request_id = getattr(request.state, "request_id", "") or uuid.uuid4().hex
    verify_api_key(x_api_key)

    if payload.status_filter not in {"all", "live", "closed"}:
        raise HTTPException(status_code=400, detail="status_filter must be one of: all, live, closed")

    pin_lat, pin_lng = validate_scrape_pin(payload.pin_lat, payload.pin_lng)
    area_id = payload.area_id
    area_slug = payload.area_slug

    if (area_id is None) != (area_slug is None):
        raise HTTPException(status_code=400, detail="Provide both area_id and area_slug, or neither (auto-resolve).")

    if area_id is None:
        resolved = find_nearest_registry_area(pin_lat, pin_lng)
        if resolved is None:
            raise HTTPException(status_code=400, detail="area_id/area_slug required — area registry is empty.")
        _key, area_id, area_slug, dist_km = resolved
        logger.info("scrape_area resolved area_id=%d slug=%s dist_to_pin=%.2fkm request_id=%s",
                    area_id, area_slug, dist_km, request_id)

    scrape_do = (payload.scrape_do_token or os.getenv("SCRAPE_DO_TOKEN", "")).strip() or None
    wall_sec = float(os.getenv("SCRAPER_WALL_CLOCK_SEC", "600"))

    try:
        loop = asyncio.get_running_loop()
        vendors, meta = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: _area_scrape_vendors_near_pin(
                    pin_lat, pin_lng, float(payload.radius_km),
                    area_id=area_id, area_slug=area_slug,
                    country=payload.country,
                    page_delay=float(payload.page_delay),
                    max_pages=payload.max_pages,
                    scrape_do_token=scrape_do,
                ),
            ),
            timeout=wall_sec,
        )
    except TimeoutError:
        raise HTTPException(status_code=504,
            detail=f"Area scrape exceeded {wall_sec:.0f}s — lower max_pages or raise SCRAPER_WALL_CLOCK_SEC.") from None
    except Exception as exc:
        logger.error("scrape_area_failed request_id=%s error=%s\n%s", request_id, exc, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Area scrape failed: {exc}") from exc

    rows = [vendor_to_row(v, pin_lat=pin_lat, pin_lng=pin_lng) for v in vendors]
    if payload.status_filter == "live":
        rows = [r for r in rows if str(r.get("status") or "").lower() not in ("closed", "offline")]
    elif payload.status_filter == "closed":
        rows = [r for r in rows if str(r.get("status") or "").lower() in ("closed", "offline")]

    return {
        "ok": True,
        "request_id": request_id,
        "count": len(rows),
        "records": rows,
        "pin_lat": pin_lat,
        "pin_lng": pin_lng,
        "radius_km": float(payload.radius_km),
        "area_id": area_id,
        "area_slug": area_slug,
        "status_filter": payload.status_filter,
        "meta": meta,
    }


@app.post("/scrape")
async def scrape(payload: ScrapeRequest, request: Request, x_api_key: str | None = Header(default=None)) -> dict:
    request_id = getattr(request.state, "request_id", "")
    verify_api_key(x_api_key)
    if not request_id:
        request_id = uuid.uuid4().hex
    if payload.status_filter not in {"all", "live", "closed"}:
        raise HTTPException(status_code=400, detail="status_filter must be one of: all, live, closed")

    async with _JOB_LOCK:
        _JOB_RESULTS[request_id] = {
            "status": "queued",
            "request_id": request_id,
            "submitted_at": asyncio.get_running_loop().time(),
        }

    async def _run_job() -> None:
        await _scrape_execution_semaphore.acquire()
        logger.info("scrape_worker_begin request_id=%s", request_id)
        submitted_at: float | None = None
        try:
            async with _JOB_LOCK:
                cur = _JOB_RESULTS.get(request_id) or {}
                submitted_at = cur.get("submitted_at")
                cur["status"] = "running"
                _JOB_RESULTS[request_id] = cur
            try:
                result = await _execute_scrape(payload, request_id=request_id)
                done: dict = {
                    "status": "complete",
                    "request_id": request_id,
                    "result": result,
                    "submitted_at": submitted_at,
                }
                async with _JOB_LOCK:
                    _JOB_RESULTS[request_id] = done
                persist_job_record(request_id, done)
            except HTTPException as exc:
                failed: dict = {
                    "status": "failed",
                    "request_id": request_id,
                    "error": str(exc.detail),
                    "status_code": int(exc.status_code),
                    "submitted_at": submitted_at,
                }
                async with _JOB_LOCK:
                    _JOB_RESULTS[request_id] = failed
                persist_job_record(request_id, failed)
            except Exception as exc:
                logger.error("scrape_job_failed request_id=%s error=%s\n%s", request_id, exc, traceback.format_exc())
                failed_exc: dict = {
                    "status": "failed",
                    "request_id": request_id,
                    "error": f"Scrape failed: {exc}",
                    "status_code": 500,
                    "submitted_at": submitted_at,
                }
                async with _JOB_LOCK:
                    _JOB_RESULTS[request_id] = failed_exc
                persist_job_record(request_id, failed_exc)
        finally:
            _scrape_execution_semaphore.release()

    asyncio.create_task(_run_job())
    return {
        "ok": True,
        "request_id": request_id,
        "status": "queued",
    }


@app.get("/result/{request_id}")
async def get_scrape_result(request_id: str, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    async with _JOB_LOCK:
        job = _JOB_RESULTS.get(request_id)
    if not job:
        loaded = load_job_record(request_id)
        if loaded:
            async with _JOB_LOCK:
                _JOB_RESULTS.setdefault(request_id, loaded)
            job = loaded
    if not job:
        raise HTTPException(status_code=404, detail="request_id not found")
    status = str(job.get("status") or "unknown")
    if status == "complete":
        out = dict(job.get("result") or {})
        out["status"] = "complete"
        out["ok"] = True
        out["request_id"] = request_id
        return out
    if status == "failed":
        return {
            "ok": False,
            "status": "failed",
            "request_id": request_id,
            "error": str(job.get("error") or "Scrape job failed"),
            "status_code": int(job.get("status_code") or 500),
        }
    return {
        "ok": True,
        "status": status,
        "request_id": request_id,
    }


async def _execute_scrape(payload: ScrapeRequest, *, request_id: str) -> dict:
    if payload.status_filter not in {"all", "live", "closed"}:
        raise HTTPException(status_code=400, detail="status_filter must be one of: all, live, closed")

    api_received_lat = float(payload.pin_lat)
    api_received_lng = float(payload.pin_lng)
    pin_lat, pin_lng = validate_scrape_pin(payload.pin_lat, payload.pin_lng)
    assert_client_pin_matches_body(
        pin_lat,
        pin_lng,
        payload.client_asserted_pin_lat,
        payload.client_asserted_pin_lng,
    )
    scrape_city_label = ""
    if payload.city and str(payload.city).strip():
        resolved = resolve_city(payload.city)
        if not resolved:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unknown city. Use one of: dubai, sharjah, abudhabi, alain, ajman "
                    "(or aliases such as 'Abu Dhabi', 'Al Ain')."
                ),
            )
        # City preset is for scrape_city / reporting only — geometry MUST follow pin_lat/pin_lng from the client.
        _city_center_lat, _city_center_lng, _default_radius, scrape_city_label = resolved

    # Guardrails to reduce OOM/timeouts on hosted infra when users request very heavy runs.
    min_r = float(os.getenv("SCRAPER_MIN_RADIUS_KM", "5"))
    max_r = float(os.getenv("SCRAPER_MAX_RADIUS_KM", "10"))
    if float(payload.radius_km) < min_r or float(payload.radius_km) > max_r:
        raise HTTPException(status_code=400, detail=f"radius_km must be between {min_r:g} and {max_r:g}")
    max_sample_cap = int(os.getenv("SCRAPER_MAX_SAMPLE_POINTS_CAP_API", "400"))
    effective_max_samples = payload.max_sample_points
    if effective_max_samples is not None:
        effective_max_samples = min(int(effective_max_samples), max_sample_cap)

    wall_sec = _effective_scrape_timeout_sec(payload)
    step = "init"
    try:
        meta: dict = {
            "request_id": request_id,
            "frontend_pin_lat": (
                round(float(payload.client_asserted_pin_lat), 6) if payload.client_asserted_pin_lat is not None else None
            ),
            "frontend_pin_lng": (
                round(float(payload.client_asserted_pin_lng), 6) if payload.client_asserted_pin_lng is not None else None
            ),
            "api_received_pin_lat": round(api_received_lat, 6),
            "api_received_pin_lng": round(api_received_lng, 6),
            "effective_scrape_pin_lat": round(pin_lat, 6),
            "effective_scrape_pin_lng": round(pin_lng, 6),
            "effective_radius_km": float(payload.radius_km),
        }
        step = "run_area_scrape"
        logger.info(
            "scrape_start request_id=%s pin=(%.5f,%.5f) radius=%.2f city=%r status=%s hv=%s sample_points=%s wall=%ss",
            request_id,
            pin_lat,
            pin_lng,
            float(payload.radius_km),
            (scrape_city_label or ""),
            payload.status_filter,
            bool(payload.high_volume),
            effective_max_samples,
            int(wall_sec),
        )
        progress_state = {"done": 0, "total": 0, "rows_last_point": 0}

        def _progress_cb(done: int, total: int, lat: float, lng: float, rows_from_point: int) -> None:
            progress_state["done"] = int(done)
            progress_state["total"] = int(total)
            progress_state["rows_last_point"] = int(rows_from_point)
            if done == 1 or done == total or done % max(1, total // 8) == 0:
                logger.info(
                    "scrape_progress request_id=%s done=%s/%s sample=(%.5f,%.5f) rows_from_point=%s",
                    request_id,
                    done,
                    total,
                    lat,
                    lng,
                    rows_from_point,
                )

        # Safety override for Render stability: Playwright vendor-page enrichment only when allowed.
        # Google Places backfill (phone / place_id / Google fields) is NOT gated by SCRAPER_ALLOW_ENRICH;
        # it runs from ``google_places_enrich`` + GOOGLE_MAPS_API_KEY (see scrape_engine).
        allow_enrich = os.getenv("SCRAPER_ALLOW_ENRICH", "0").strip().lower() in ("1", "true", "yes", "on")
        effective_enrich = bool(payload.enrich) and allow_enrich
        df = await asyncio.wait_for(
            run_area_scrape(
                pin_lat=pin_lat,
                pin_lng=pin_lng,
                radius_km=payload.radius_km,
                spacing_km=payload.spacing_km,
                concurrency=payload.concurrency,
                status_filter=payload.status_filter,
                just_landed_only=payload.just_landed_only,
                scroll_rounds=payload.scroll_rounds,
                scroll_wait_ms=payload.scroll_wait_ms,
                progress_cb=_progress_cb,
                max_sample_points=effective_max_samples,
                dedupe_by_vendor_url=payload.dedupe_by_vendor_url,
                scrape_city=scrape_city_label,
                high_volume=payload.high_volume,
                scrape_target_label=(payload.scrape_target_label or "").strip(),
                meta_out=meta,
                google_places_enrich=payload.google_places_enrich,
                enrich=effective_enrich,
            ),
            timeout=wall_sec,
        )
        step = "serialize_response"
        meta["grid_points_completed"] = int(progress_state["done"])
        meta["grid_points_total"] = int(progress_state["total"])
        meta["last_point_rows"] = int(progress_state["rows_last_point"])
        records = df.to_dict(orient="records")
        out: dict = {
            "ok": True,
            "request_id": request_id,
            "count": len(records),
            "records": records,
            "dedupe_by_vendor_url": payload.dedupe_by_vendor_url,
            "high_volume": payload.high_volume,
            "status_filter": payload.status_filter,
            "just_landed_only": payload.just_landed_only,
            "enrich_requested": bool(payload.enrich),
            "enrich_effective": bool(effective_enrich),
            "scrape_target_label": (payload.scrape_target_label or "").strip(),
            "scrape_wall_clock_sec_applied": int(wall_sec),
            "pin_lat": pin_lat,
            "pin_lng": pin_lng,
            "scrape_run_meta": meta,
            "total_points": int(meta.get("grid_size") or 0),
            "completed_points": int(meta.get("grid_points_completed") or 0),
            "partial": bool(int(meta.get("grid_points_completed") or 0) < int(meta.get("grid_size") or 0)),
        }
        if scrape_city_label:
            out["city"] = scrape_city_label
        logger.info(
            "scrape_done request_id=%s count=%s raw=%s inside=%s outside=%s last_step=%s",
            request_id,
            len(records),
            meta.get("raw_listing_row_count"),
            meta.get("inside_radius_row_count"),
            meta.get("outside_radius_row_count"),
            step,
        )
        return out
    except TimeoutError:
        logger.error(
            "scrape_timeout request_id=%s pin=(%.5f,%.5f) radius=%.2f last_step=%s wall=%ss\n%s",
            request_id,
            pin_lat,
            pin_lng,
            float(payload.radius_km),
            step,
            int(wall_sec),
            traceback.format_exc(),
        )
        raise HTTPException(
            status_code=504,
            detail=(
                f"Scrape exceeded {wall_sec:.0f}s (send JSON scrape_wall_clock_sec: 900, "
                "and/or set env SCRAPER_WALL_CLOCK_SEC on the API). "
                "Lower max_sample_points or turn off high_volume if you cannot raise limits. "
                "If this still happens at 900s+, your host may be closing HTTP before the app (e.g. Render proxy limits)."
            ),
        ) from None
    except Exception as exc:
        logger.error(
            "scrape_failed request_id=%s pin=(%.5f,%.5f) radius=%.2f last_step=%s error=%s\n%s",
            request_id,
            pin_lat,
            pin_lng,
            float(payload.radius_km),
            step,
            exc,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=f"Scrape failed: {exc}") from exc


# ---------------------------------------------------------------------------
# /analyze — multi-pin whitespace analysis with background job + Excel export
# ---------------------------------------------------------------------------

class AnalyzePinRequest(BaseModel):
    name: str = Field(description="Label for this pin (e.g. 'Business Bay')")
    lat: float
    lng: float
    radius_km: float = Field(default=10.0, ge=1.0, le=30.0)


class AnalyzeRequest(BaseModel):
    pins: list[AnalyzePinRequest]
    just_landed_only: bool = Field(default=False, description="Restrict output to vendors tagged as new/just-landed by Talabat")


@app.post("/analyze")
def submit_analyze(
    payload: AnalyzeRequest,
    x_api_key: str | None = Header(default=None),
    x_session_id: str | None = Header(default=None, alias="X-Session-ID"),
) -> dict:
    verify_api_key(x_api_key)
    if not payload.pins:
        raise HTTPException(status_code=400, detail="pins list cannot be empty")

    # Per-session job limit: reject if this session already has a running job
    session_id = x_session_id or "global"
    if x_session_id:
        with _ANALYZE_JOBS_LOCK:
            running = [
                j for j in _ANALYZE_JOBS.values()
                if j.get("session_id") == session_id and j.get("status") in ("queued", "running")
            ]
        if running:
            raise HTTPException(status_code=429, detail="You already have a job running. Wait for it to finish.")

    job_id = uuid.uuid4().hex
    job: dict = {
        "job_id": job_id,
        "status": "queued",
        "session_id": session_id,
        "pins": [p.dict() for p in payload.pins],
        "just_landed_only": payload.just_landed_only,
        "progress": {"current": 0, "total": len(payload.pins), "current_pin": None},
        "created_at": datetime.now().isoformat(),
        "output_file": None,
        "result_summary": None,
        "error": None,
    }
    with _ANALYZE_JOBS_LOCK:
        _ANALYZE_JOBS[job_id] = job
    _persist_job(job_id, job)

    t = threading.Thread(target=_run_analyze_job, args=(job_id,), daemon=True)
    t.start()

    return {"ok": True, "job_id": job_id, "total_pins": len(payload.pins)}


@app.get("/analyze")
def list_analyze_jobs(
    x_api_key: str | None = Header(default=None),
    x_session_id: str | None = Header(default=None, alias="X-Session-ID"),
) -> dict:
    verify_api_key(x_api_key)
    with _ANALYZE_JOBS_LOCK:
        jobs = [
            {
                "job_id": jid,
                "status": j["status"],
                "created_at": j.get("created_at"),
                "progress": j.get("progress"),
                "result_summary": j.get("result_summary"),
                "pins": j.get("pins", []),
            }
            for jid, j in sorted(
                _ANALYZE_JOBS.items(),
                key=lambda x: x[1].get("created_at", ""),
                reverse=True,
            )
            if not x_session_id or j.get("session_id") in (x_session_id, "global", None)
        ]
    return {"ok": True, "jobs": jobs}


@app.get("/analyze/{job_id}")
def get_analyze_job(
    job_id: str,
    x_api_key: str | None = Header(default=None),
) -> dict:
    verify_api_key(x_api_key)
    with _ANALYZE_JOBS_LOCK:
        job = dict(_ANALYZE_JOBS.get(job_id) or {})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "ok": True,
        "job_id": job_id,
        "status": job["status"],
        "progress": job["progress"],
        "created_at": job.get("created_at"),
        "result_summary": job.get("result_summary"),
        "error": job.get("error"),
    }


@app.get("/analyze/{job_id}/download")
def download_analyze(
    job_id: str,
    x_api_key: str | None = Header(default=None),
) -> FileResponse:
    verify_api_key(x_api_key)
    with _ANALYZE_JOBS_LOCK:
        job = dict(_ANALYZE_JOBS.get(job_id) or {})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "complete":
        raise HTTPException(status_code=400, detail=f"Job not complete yet: {job['status']}")
    output_file = job.get("output_file") or ""
    if not output_file or not Path(output_file).exists():
        raise HTTPException(status_code=404, detail="Output file not found")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    return FileResponse(
        path=output_file,
        filename=f"kp_whitespace_{timestamp}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


KP_TENANT_RADIUS_KM = 1.5  # vendor within this distance from a KP facility = KP tenant


def _compute_lead_scores(matrix_df: "pd.DataFrame", raw_df: "pd.DataFrame") -> "pd.DataFrame":
    """Add lead_score (0-100) and lead_priority columns to matrix_df."""
    import pandas as pd

    # Aggregate avg delivery time per brand from raw records
    delivery_agg: dict = {}
    if not raw_df.empty and "restaurant_id" in raw_df.columns and "avg_delivery_min" in raw_df.columns:
        def _parse_delivery(v):
            try:
                m = str(v or "")
                import re as _re
                nums = _re.findall(r'\d+', m)
                return float(nums[0]) if nums else None
            except Exception:
                return None
        raw_copy = raw_df.copy()
        raw_copy["_del"] = raw_copy["avg_delivery_min"].apply(_parse_delivery)
        delivery_agg = (
            raw_copy.dropna(subset=["_del"])
            .groupby("restaurant_id")["_del"]
            .mean()
            .round(1)
            .to_dict()
        )

    scores, priorities = [], []
    for _, row in matrix_df.iterrows():
        if row.get("kp_tenant") == "Yes":
            scores.append(0)
            priorities.append("")
            continue

        score = 0.0

        # Volume: Talabat reviews (0-35)
        reviews = float(row.get("total_reviews") or 0)
        score += min(reviews / 500, 1.0) * 35

        # Quality: Talabat rating (0-30)
        rating = float(row.get("avg_rating") or 0)
        if rating > 0:
            score += max(0.0, (rating - 1.0) / 4.0) * 30

        # Operations: delivery time (0-20) — lower is better
        rid = row.get("restaurant_id")
        delivery = float(delivery_agg.get(rid, 0) or 0)
        if delivery > 0:
            score += max(0.0, (70.0 - delivery) / 50.0) * 20
        else:
            score += 10  # unknown = neutral

        # Google corroboration (0-15)
        try:
            g_rev = float(str(row.get("google_reviews") or "").strip() or 0)
        except (ValueError, TypeError):
            g_rev = 0
        if g_rev >= 200:
            score += 15
        elif g_rev >= 50:
            score += 8

        score = min(100, round(score))
        scores.append(score)
        priorities.append("High Priority" if score >= 60 else "Medium Priority" if score >= 30 else "Low Priority")

    matrix_df = matrix_df.copy()
    matrix_df["lead_score"] = scores
    matrix_df["lead_priority"] = priorities
    return matrix_df


def _enrich_kp_proximity(
    matrix_df: "pd.DataFrame",
    raw_df: "pd.DataFrame",
    facilities: list[dict],
) -> tuple["pd.DataFrame", "pd.DataFrame"]:
    """
    Add KP-presence columns to both dataframes.

    raw_df  → kp_nearest_facility, kp_nearest_km, kp_tenant (Yes/No)
    matrix_df → kp_tenant (Yes/No), kp_facilities (comma-sep list), opportunity (Yes = not a KP tenant)
    """
    import pandas as pd
    from geo_utils import haversine_series_km_from_pin

    live_facs = [f for f in facilities if str(f.get("go_live", "")).lower() == "live"]
    if raw_df.empty or not live_facs:
        return matrix_df, raw_df

    lat_col = pd.to_numeric(raw_df.get("latitude", pd.Series()), errors="coerce")
    lng_col = pd.to_numeric(raw_df.get("longitude", pd.Series()), errors="coerce")

    # Distance matrix: one column per KP facility
    dist_cols = {}
    for fac in live_facs:
        d = haversine_series_km_from_pin(fac["lat"], fac["lng"], lat_col, lng_col)
        dist_cols[fac["name"]] = d

    dist_df = pd.DataFrame(dist_cols, index=raw_df.index)
    raw_df = raw_df.copy()
    raw_df["kp_nearest_facility"] = dist_df.idxmin(axis=1)
    raw_df["kp_nearest_km"]       = dist_df.min(axis=1).round(2)
    raw_df["kp_tenant"]           = (dist_df.min(axis=1) <= KP_TENANT_RADIUS_KM).map({True: "Yes", False: "No"})

    # Aggregate to matrix level using restaurant_id
    if matrix_df.empty:
        return matrix_df, raw_df

    rid_col = "restaurant_id"
    if rid_col not in raw_df.columns:
        return matrix_df, raw_df

    # For each brand (restaurant_id), collect KP facilities where at least one branch is a tenant
    tenant_rows = raw_df[raw_df["kp_tenant"] == "Yes"]
    brand_kp: dict = {}
    for rid, grp in tenant_rows.groupby(rid_col, dropna=False):
        facs_for_brand = sorted(set(grp["kp_nearest_facility"].dropna().tolist()))
        brand_kp[rid] = ", ".join(facs_for_brand)

    matrix_df = matrix_df.copy()
    matrix_df["kp_tenant"]     = matrix_df[rid_col].map(lambda r: "Yes" if r in brand_kp else "No")
    matrix_df["kp_facilities"] = matrix_df[rid_col].map(lambda r: brand_kp.get(r, ""))
    matrix_df["opportunity"]   = matrix_df["kp_tenant"].map({"No": "⭐ Opportunity", "Yes": ""})

    return matrix_df, raw_df


def _run_analyze_job(job_id: str) -> None:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from whitespace_analysis import build_matrix, export_excel, FACILITIES
    from places_enrich import enrich_df_with_google_places

    with _ANALYZE_JOBS_LOCK:
        job = _ANALYZE_JOBS[job_id]
        job["status"] = "running"
    _persist_job(job_id, job)

    pins = job["pins"]
    facility_vendors: dict[str, list[dict]] = {}
    facility_meta: dict[str, dict] = {}

    # One shared session with proxy for the whole job — avoids per-area rate-limit resets
    import requests as _requests
    from scrape_network import requests_proxies_from_env as _proxies_from_env
    _job_session = _requests.Session()
    _job_session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    _job_session.verify = False
    _proxies = _proxies_from_env()
    if _proxies:
        _job_session.proxies.update(_proxies)
        logger.info("analyze_job using proxy: %s", list(_proxies.keys()))

    # ── Smoke-test build_matrix + export pipeline before wasting hours scraping ──
    try:
        _smoke_vendors = [{
            "restaurantId": 1, "name": "Test Brand", "cuisineString": "Burgers",
            "rating": "4.5", "reviewsCount": "100", "latitude": pins[0]["lat"],
            "longitude": pins[0]["lng"],
        }]
        _smoke_fv = {"_smoke_pin": _smoke_vendors}
        _smoke_matrix, _smoke_raw = build_matrix(_smoke_fv)
        if not _smoke_matrix.empty:
            _fac_cols = [c for c in _smoke_matrix.columns if c not in
                         ("restaurant_id", "brand_name", "cuisine", "avg_rating", "total_reviews")]
            _smoke_matrix["_tb"] = _smoke_matrix[_fac_cols].sum(axis=1)
            _smoke_matrix["_tf"] = (_smoke_matrix[_fac_cols] > 0).sum(axis=1)
        import tempfile, os as _os
        _tmp = tempfile.mktemp(suffix=".xlsx")
        try:
            export_excel(_smoke_matrix, _smoke_raw, [], {}, _tmp, radius_km=10.0)
        finally:
            if _os.path.exists(_tmp):
                _os.remove(_tmp)
        logger.info("analyze_job smoke_test passed job_id=%s", job_id)
    except Exception as _smoke_exc:
        tb = traceback.format_exc()
        logger.error("analyze_job smoke_test FAILED job_id=%s: %s\n%s", job_id, _smoke_exc, tb)
        with _ANALYZE_JOBS_LOCK:
            job["status"] = "failed"
            job["error"] = f"[Pre-run check failed — bug detected before scraping started] {_smoke_exc}"
            job["traceback"] = tb
        _persist_job(job_id, job)
        return

    try:
        for i, pin in enumerate(pins):
            name = pin["name"]
            lat = float(pin["lat"])
            lng = float(pin["lng"])
            radius_km = float(pin.get("radius_km", 10.0))

            with _ANALYZE_JOBS_LOCK:
                job["progress"]["current"] = i
                job["progress"]["current_pin"] = name

            logger.info("analyze_job job_id=%s pin=%d/%d name=%r lat=%.5f lng=%.5f",
                        job_id, i + 1, len(pins), name, lat, lng)

            try:
                from geo_utils import haversine_km as _hav

                # Find ALL Talabat areas whose centre is within radius_km of the pin
                areas_in_radius = []
                for _akey, (_aid, _aslug, _alat, _alng) in UAE_AREA_REGISTRY.items():
                    if _hav(lat, lng, _alat, _alng) <= radius_km:
                        areas_in_radius.append((_aid, _aslug))

                if not areas_in_radius:
                    # Fallback: use nearest area, but only if it's reasonably close
                    resolved = find_nearest_registry_area(lat, lng)
                    if resolved is None:
                        raise ValueError("No areas in registry")
                    _, _aid, _aslug, nearest_dist = resolved
                    # If the nearest Talabat area is more than 25 km away, the pin
                    # is in a desert / uncovered zone — fail clearly instead of
                    # silently returning 0 vendors.
                    if nearest_dist > 25.0:
                        raise ValueError(
                            f"No Talabat coverage within {radius_km:.0f} km of '{name}'. "
                            f"The nearest covered area ({_aslug.replace('-', ' ').title()}) "
                            f"is {nearest_dist:.0f} km away. "
                            f"Move the pin to an urban area or increase the radius."
                        )
                    areas_in_radius = [(_aid, _aslug)]

                logger.info("analyze_job pin=%r scraping %d areas: %s",
                            name, len(areas_in_radius),
                            [s for _, s in areas_in_radius])

                seen_branch_ids: set = set()
                vendors: list = []
                total_area_vendors = 0
                last_meta: dict = {}

                # Parallel area scraping — each area gets its own session clone
                from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

                _areas_done = [0]
                # Per-area progress: aslug -> (current_page, total_pages)
                _area_progress: dict = {}

                def _scrape_one_area(area_args):
                    _aid, _aslug = area_args

                    def _cb(page, total_pages, vendors_so_far, _s=_aslug):
                        with _ANALYZE_JOBS_LOCK:
                            _area_progress[_s] = (page, total_pages)
                            pages_done = sum(p for p, _ in _area_progress.values())
                            known_totals = [t for _, t in _area_progress.values()]
                            avg = sum(known_totals) // len(known_totals)
                            remaining = len(areas_in_radius) - len(_area_progress)
                            grand_total = sum(known_totals) + remaining * avg
                            job["progress"]["area_page"] = pages_done
                            job["progress"]["area_pages_total"] = grand_total
                            job["progress"]["current_pin"] = (
                                f"{name} — {_areas_done[0]}/{len(areas_in_radius)} areas done, "
                                f"page {pages_done}/{grand_total}"
                            )

                    result = _scrape_area_vendors(
                        _aid, _aslug,
                        page_delay=0.2,
                        session=None,
                        page_cb=_cb,
                    )
                    return result

                with _ANALYZE_JOBS_LOCK:
                    job["progress"]["current_pin"] = f"{name} — starting {len(areas_in_radius)} areas in parallel…"
                    job["progress"]["area_page"] = 0
                    job["progress"]["area_pages_total"] = 1
                    job["progress"]["vendors_collected"] = 0

                with ThreadPoolExecutor(max_workers=4) as _pool:
                    _futures = {_pool.submit(_scrape_one_area, (aid, aslug)): (aid, aslug)
                                for aid, aslug in areas_in_radius}
                    for _fut in _as_completed(_futures):
                        _aid, _aslug = _futures[_fut]
                        try:
                            area_vendors, last_meta = _fut.result()
                        except Exception as _area_exc:
                            logger.error("analyze_job pin=%r area=%r failed: %s", name, _aslug, _area_exc)
                            area_vendors = []
                        _areas_done[0] += 1
                        total_area_vendors += len(area_vendors)
                        for v in area_vendors:
                            bid = v.get("branchId") or v.get("branch_id")
                            if bid and bid in seen_branch_ids:
                                continue
                            if bid:
                                seen_branch_ids.add(bid)
                            try:
                                vlat = float(v.get("latitude") or 0)
                                vlng = float(v.get("longitude") or 0)
                            except (TypeError, ValueError):
                                vlat = vlng = 0.0
                            if vlat == 0.0 and vlng == 0.0:
                                vendors.append(v)
                            elif _hav(lat, lng, vlat, vlng) <= radius_km:
                                v["_distance_km"] = round(_hav(lat, lng, vlat, vlng), 3)
                                vendors.append(v)
                        with _ANALYZE_JOBS_LOCK:
                            job["progress"]["vendors_collected"] = len(vendors)

                # Apply Just Landed filter if requested
                if job.get("just_landed_only"):
                    vendors = [v for v in vendors if v.get("isNew")]

                meta = {**last_meta,
                        "areas_scraped": [s for _, s in areas_in_radius],
                        "vendors_in_radius": len(vendors),
                        "total_vendors_reported": total_area_vendors}
                facility_vendors[name] = vendors
                facility_meta[name] = meta
                logger.info("analyze_job pin=%r done areas=%d unique_vendors=%d",
                            name, len(areas_in_radius), len(vendors))
            except Exception as exc:
                logger.error("analyze_job pin=%r failed: %s", name, exc)
                facility_vendors[name] = []
                facility_meta[name] = {"error": str(exc)}

            if i < len(pins) - 1:
                _time.sleep(15)

        with _ANALYZE_JOBS_LOCK:
            job["progress"]["current"] = len(pins)
            job["progress"]["current_pin"] = "Building report..."

        facilities_meta_list = [
            {"name": p["name"], "emirate": "UAE", "go_live": "Live",
             "lat": p["lat"], "lng": p["lng"]}
            for p in pins
        ]

        matrix_df, raw_df = build_matrix(facility_vendors)

        if not matrix_df.empty:
            fac_cols = [c for c in matrix_df.columns if c not in ("restaurant_id", "brand_name", "cuisine", "avg_rating", "total_reviews")]
            matrix_df["_tb"] = matrix_df[fac_cols].sum(axis=1)
            matrix_df["_tf"] = (matrix_df[fac_cols] > 0).sum(axis=1)
            matrix_df = (
                matrix_df
                .sort_values(["_tf", "_tb", "brand_name"], ascending=[False, False, True])
                .drop(columns=["_tb", "_tf"])
                .reset_index(drop=True)
            )

        # ── KP facility proximity enrichment ─────────────────────────────────
        matrix_df, raw_df = _enrich_kp_proximity(matrix_df, raw_df, FACILITIES)

        # ── Lead scoring ──────────────────────────────────────────────────────
        matrix_df = _compute_lead_scores(matrix_df, raw_df)

        # ── Google Places enrichment (phone, address, legal name) ─────────────
        with _ANALYZE_JOBS_LOCK:
            job["progress"]["current_pin"] = "Enriching with Google Maps…"
        pin_lats = [float(p["lat"]) for p in pins]
        pin_lngs = [float(p["lng"]) for p in pins]
        centre_lat = sum(pin_lats) / len(pin_lats)
        centre_lng = sum(pin_lngs) / len(pin_lngs)
        # Sort by popularity before enrichment — the 300-brand cap goes to the most
        # reviewed/rated restaurants first (more likely to have a phone on Google Maps).
        import pandas as _pd_sort
        _sort_cols = [c for c in ("reviews", "rating", "estimated_orders") if c in raw_df.columns]
        if _sort_cols:
            _sort_df = raw_df.copy()
            for _sc in _sort_cols:
                _sort_df[_sc] = _pd_sort.to_numeric(_sort_df[_sc], errors="coerce")
            raw_df = raw_df.iloc[
                _sort_df[_sort_cols].apply(tuple, axis=1).argsort()[::-1].values
            ].reset_index(drop=True)
        raw_df = enrich_df_with_google_places(raw_df, centre_lat, centre_lng)

        # Geoapify enrichment — fills phones for brands Google Places missed (free, OSM-based)
        from geoapify_enrich import enrich_df_with_geoapify as _enrich_geoapify
        with _ANALYZE_JOBS_LOCK:
            job["progress"]["current_pin"] = "Enriching contacts via Geoapify…"
        _enrich_geoapify(raw_df, max_brands=3000)

        # Propagate enrichment to matrix (first non-empty per brand)
        if not matrix_df.empty and not raw_df.empty and "restaurant_id" in raw_df.columns:
            for col in ["contact_phone", "legal_name", "google_address", "google_maps_link", "geoapify_phone", "data_source"]:
                if col in raw_df.columns:
                    first_val = (
                        raw_df[raw_df[col].astype(str).str.strip() != ""]
                        .groupby("restaurant_id")[col]
                        .first()
                    )
                    matrix_df[col] = matrix_df["restaurant_id"].map(first_val).fillna(
                        "Talabat" if col == "data_source" else ""
                    )

        # ── Phone type: Mobile / Landline only — strip 600/800 service numbers ──
        import re as _re
        def _uae_phone_type(phone: str) -> str:
            """Classify UAE phone number. Returns empty string for service/600/800 numbers."""
            if not phone or not str(phone).strip():
                return ""
            p = _re.sub(r"[\s\-\(\)\.]+", "", str(phone))
            # UAE mobile: 05X / +9715X / 009715X
            if (p.startswith("05") or p.startswith("+9715") or
                    p.startswith("009715") or p.startswith("9715")):
                return "Mobile 📱"
            # UAE service/toll-free numbers (600 XXXXXX / 800 XXXXXX) — call center, not useful for outreach
            if (p.startswith("+971600") or p.startswith("971600") or p.startswith("600")
                    or p.startswith("+971800") or p.startswith("971800") or p.startswith("800")):
                return "Service 📞"
            # UAE landline (02/03/04/06/07/09)
            return "Landline ☎"

        for _df in (matrix_df, raw_df):
            if "contact_phone" in _df.columns:
                _df["phone_type"] = _df["contact_phone"].apply(_uae_phone_type)
                # Keep ONLY mobile numbers — clear landlines and service/600/800 numbers
                _non_mobile_mask = _df["phone_type"].isin(["Landline ☎", "Service 📞"])
                _df.loc[_non_mobile_mask, "contact_phone"] = ""
                _df.loc[_non_mobile_mask, "phone_type"] = ""

        # ── Google Gaps: find restaurants on Google Maps but not on Talabat ──────
        import pandas as _pd
        google_gaps_df = _pd.DataFrame()
        _gmap_key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
        if _gmap_key:
            from google_nearby import google_nearby_tiled, diff_vs_talabat, place_to_row
            with _ANALYZE_JOBS_LOCK:
                job["progress"]["current_pin"] = "Finding restaurants missing from Talabat…"
            _gap_rows: list = []
            for _pin in pins:
                _plat = float(_pin["lat"])
                _plng = float(_pin["lng"])
                _pradius = float(_pin.get("radius_km", 10.0))
                logger.info("google_nearby_tiled pin=%r radius=%.1fkm", _pin.get("name"), _pradius)
                _gplaces = google_nearby_tiled(_plat, _plng, _pradius, _gmap_key)
                _gaps = diff_vs_talabat(_gplaces, raw_df, _plat, _plng, radius_km=_pradius)
                for _g in _gaps:
                    _gap_rows.append(place_to_row(_g, _plat, _plng))
            if _gap_rows:
                google_gaps_df = _pd.DataFrame(_gap_rows).drop_duplicates(subset=["place_id"])
                google_gaps_df = google_gaps_df.sort_values("distance_km")
                logger.info("google_gaps total=%d", len(google_gaps_df))
        else:
            logger.info("google_gaps skipped — GOOGLE_MAPS_API_KEY not set")

        output_file = str(_ANALYZE_JOBS_DIR / f"analysis_{job_id}.xlsx")
        export_excel(matrix_df, raw_df, facilities_meta_list, facility_meta, output_file,
                     radius_km=10.0, google_gaps_df=google_gaps_df)

        # Persist output_file path immediately — if container restarts here the Excel is recoverable.
        with _ANALYZE_JOBS_LOCK:
            job["output_file"] = output_file
        _persist_job(job_id, job)

        # Build vendor coordinate list + rich points for frontend heatmap/dots
        vendor_coords: list = []
        vendor_points: list = []
        if not raw_df.empty and "latitude" in raw_df.columns and "longitude" in raw_df.columns:
            coords = raw_df[["latitude", "longitude"]].dropna()
            coords = coords[(coords["latitude"] != 0) & (coords["longitude"] != 0)]
            vendor_coords = coords.values.tolist()

            _vp_cols = [c for c in ["latitude", "longitude", "name", "cuisines", "branch_id",
                                     "branch_url", "restaurant_slug", "distance_km",
                                     "rating", "total_reviews"]
                        if c in raw_df.columns]
            for _, vrow in raw_df[_vp_cols].iterrows():
                try:
                    vlat = float(vrow.get("latitude") or 0)
                    vlng = float(vrow.get("longitude") or 0)
                except (TypeError, ValueError):
                    continue
                if vlat == 0 or vlng == 0:
                    continue
                bid = vrow.get("branch_id")
                burl = str(vrow.get("branch_url") or "")
                rslug = str(vrow.get("restaurant_slug") or "")
                if not burl and rslug and bid:
                    burl = f"https://www.talabat.com/uae/restaurant/{rslug}/{bid}"
                try:
                    rating = float(vrow.get("rating") or 0) or None
                    reviews = int(vrow.get("total_reviews") or 0) or None
                    dist = round(float(vrow.get("distance_km") or 0), 2)
                except (TypeError, ValueError):
                    rating = reviews = None
                    dist = 0.0
                vendor_points.append({
                    "lat": round(vlat, 6),
                    "lng": round(vlng, 6),
                    "n": str(vrow.get("name") or ""),
                    "c": str(vrow.get("cuisines") or ""),
                    "r": rating,
                    "rv": reviews,
                    "d": dist,
                    "u": burl,
                    "bid": int(bid) if bid else None,
                })

        just_landed_count = int((raw_df["is_new"] == True).sum()) if not raw_df.empty and "is_new" in raw_df.columns else 0  # noqa: E712

        # Collect per-pin errors so the frontend can warn the user
        pin_errors = {
            name: meta["error"]
            for name, meta in facility_meta.items()
            if "error" in meta
        }

        with _ANALYZE_JOBS_LOCK:
            job["status"] = "complete"
            job["output_file"] = output_file
            job["progress"]["current_pin"] = None
            job["result_summary"] = {
                "brands": len(matrix_df),
                "raw_rows": len(raw_df),
                "pins": len(pins),
                "just_landed_count": just_landed_count,
                "vendor_coords": vendor_coords,
                "vendor_points": vendor_points,
                "pin_errors": pin_errors,
            }
        _persist_job(job_id, job)
        logger.info("analyze_job complete job_id=%s brands=%d raw=%d", job_id, len(matrix_df), len(raw_df))

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("analyze_job failed job_id=%s: %s\n%s", job_id, exc, tb)
        with _ANALYZE_JOBS_LOCK:
            job["status"] = "failed"
            job["error"] = str(exc)
            job["traceback"] = tb
        _persist_job(job_id, job)
