from __future__ import annotations

import asyncio
import logging
import os
import traceback
import uuid

import requests
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from pin_validation import assert_client_pin_matches_body, validate_scrape_pin
from listing_harvest import country_path_slug, default_listing_url_for_slug, harvest_vendor_urls
from scrape_engine import run_area_scrape
from uae_cities import resolve_city

app = FastAPI(title="Talabat Area Scraper API", version="1.0.0")
logger = logging.getLogger("talabat_area_intel.api")


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
    radius_km: float = Field(default=12.0, ge=1.0, le=40.0)
    # Wider spacing + low concurrency defaults reduce Render 502/timeouts on long runs.
    spacing_km: float = Field(default=1.5, ge=0.35, le=3.0)
    concurrency: int = Field(default=1, ge=1, le=6)
    # "live" = drop rows classified as closed (keeps unknown + open); "all" = no status filter; "closed" = closed only.
    status_filter: str = Field(
        default="live",
        description="live | all | closed — see API docs; not the same as Talabat Pro 'live' badge.",
    )
    just_landed_only: bool = Field(
        default=False,
        description="Attempts Talabat 'Just Landed' listing filter; result rows also restricted to new signals when True.",
    )
    scroll_rounds: int = Field(default=18, ge=2, le=60)
    scroll_wait_ms: int = Field(default=900, ge=400, le=3000)
    # More points = longer runs; omit to use MAX_SCRAPE_SAMPLE_POINTS env (default 6).
    max_sample_points: int | None = Field(default=None, ge=1, le=400)
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
    seed_vendor_urls: list[str] | None = Field(
        default=None,
        description="If non-empty, skip geo-grid listing scrape; build rows from these Talabat vendor URLs and run "
        "vendor-page + optional Google Places enrichment, then radius/status filters (same record shape as /scrape).",
    )
    vendor_detail_enrich_max: int | None = Field(
        default=None,
        ge=1,
        le=120,
        description="Max vendor pages to open when seed_vendor_urls is set (capped by SCRAPER_SEED_ENRICH_MAX_CAP_API).",
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


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Nominatim requires a valid User-Agent (https://operations.osmfoundation.org/policies/nominatim/).
_DEFAULT_NOMINATIM_UA = "TalabatAreaIntel/1.0 (+https://github.com/maisam21-lab/maisam21-lab-talabat-area-intel)"


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


@app.get("/health/scrape-config")
def scrape_config(x_api_key: str | None = Header(default=None)) -> dict:
    """Non-secret guardrails and geocode toggles — use to verify deployed API settings (requires same API key as /scrape)."""
    verify_api_key(x_api_key)
    return {
        "ok": True,
        "google_maps_key_configured": bool(os.getenv("GOOGLE_MAPS_API_KEY", "").strip()),
        "geocode_use_google": _google_geocode_enabled(),
        "geocode_fallback_nominatim": _nominatim_enabled(),
        "scraper_wall_clock_sec_default": float(os.getenv("SCRAPER_WALL_CLOCK_SEC", "600")),
        "scraper_max_radius_km": float(os.getenv("SCRAPER_MAX_RADIUS_KM", "25")),
        "scraper_max_sample_points_cap_api": int(os.getenv("SCRAPER_MAX_SAMPLE_POINTS_CAP_API", "180")),
        "max_scrape_sample_points_default": int(os.getenv("MAX_SCRAPE_SAMPLE_POINTS", "6")),
        "restaurant_detail_enrich_max_default": int(os.getenv("RESTAURANT_DETAIL_ENRICH_MAX", "12")),
        "scraper_per_point_timeout_sec": float(os.getenv("SCRAPER_PER_POINT_TIMEOUT_SEC", "90")),
        "scraper_listing_fast_path": os.getenv("SCRAPER_LISTING_FAST_PATH", "0").strip(),
        "scraper_humanize": os.getenv("SCRAPER_HUMANIZE", "0").strip(),
        "google_places_enrich_env": os.getenv("GOOGLE_PLACES_ENRICH", "0").strip(),
        "scraper_listing_page_pagination": os.getenv("SCRAPER_LISTING_PAGE_PAGINATION", "0").strip(),
        "scraper_listing_max_pages": int(os.getenv("SCRAPER_LISTING_MAX_PAGES", "25")),
        "scraper_seed_url_list_max": int(os.getenv("SCRAPER_SEED_URL_LIST_MAX", "180")),
        "scraper_seed_enrich_max_cap_api": int(os.getenv("SCRAPER_SEED_ENRICH_MAX_CAP_API", "80")),
        "listing_harvest_response_max_urls": int(os.getenv("LISTING_HARVEST_RESPONSE_MAX_URLS", "2500")),
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


@app.post("/scrape")
async def scrape(payload: ScrapeRequest, request: Request, x_api_key: str | None = Header(default=None)) -> dict:
    request_id = getattr(request.state, "request_id", "")
    verify_api_key(x_api_key)
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
    max_r = float(os.getenv("SCRAPER_MAX_RADIUS_KM", "25"))
    if float(payload.radius_km) > max_r:
        raise HTTPException(status_code=400, detail=f"radius_km exceeds allowed max ({max_r:g})")
    max_sample_cap = int(os.getenv("SCRAPER_MAX_SAMPLE_POINTS_CAP_API", "180"))
    effective_max_samples = payload.max_sample_points
    if effective_max_samples is not None:
        effective_max_samples = min(int(effective_max_samples), max_sample_cap)

    seed_max = int(os.getenv("SCRAPER_SEED_URL_LIST_MAX", "180"))
    raw_seeds = [s for s in (payload.seed_vendor_urls or []) if isinstance(s, str) and s.strip()]
    if len(raw_seeds) > seed_max:
        raise HTTPException(
            status_code=400,
            detail=f"seed_vendor_urls has {len(raw_seeds)} entries; max is {seed_max} (SCRAPER_SEED_URL_LIST_MAX).",
        )

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
            "scrape_start request_id=%s pin=(%.5f,%.5f) radius=%.2f city=%r status=%s hv=%s sample_points=%s "
            "seeds=%s wall=%ss",
            request_id,
            pin_lat,
            pin_lng,
            float(payload.radius_km),
            (scrape_city_label or ""),
            payload.status_filter,
            bool(payload.high_volume),
            effective_max_samples,
            len(raw_seeds),
            int(wall_sec),
        )
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
                progress_cb=None,
                max_sample_points=effective_max_samples,
                dedupe_by_vendor_url=payload.dedupe_by_vendor_url,
                scrape_city=scrape_city_label,
                high_volume=payload.high_volume,
                scrape_target_label=(payload.scrape_target_label or "").strip(),
                meta_out=meta,
                google_places_enrich=payload.google_places_enrich,
                seed_vendor_urls=raw_seeds or None,
                vendor_detail_enrich_max=payload.vendor_detail_enrich_max,
            ),
            timeout=wall_sec,
        )
        step = "serialize_response"
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
            "scrape_target_label": (payload.scrape_target_label or "").strip(),
            "seed_vendor_url_count": len(raw_seeds),
            "scrape_wall_clock_sec_applied": int(wall_sec),
            "pin_lat": pin_lat,
            "pin_lng": pin_lng,
            "scrape_run_meta": meta,
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
