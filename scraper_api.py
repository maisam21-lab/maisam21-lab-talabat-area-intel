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
    spacing_km: float = Field(default=2.0, ge=0.5, le=3.0)
    concurrency: int = Field(default=1, ge=1, le=6)
    # "live" = exclude closed (includes unknown/open); "all" = no filter; "closed" = closed only.
    status_filter: str = Field(default="live")
    just_landed_only: bool = False
    scroll_rounds: int = Field(default=6, ge=2, le=60)
    scroll_wait_ms: int = Field(default=650, ge=400, le=3000)
    # More points = longer runs; omit to use MAX_SCRAPE_SAMPLE_POINTS env (default 1).
    max_sample_points: int | None = Field(default=None, ge=1, le=200)


class GeocodeRequest(BaseModel):
    query: str


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/geocode")
def geocode(payload: GeocodeRequest, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    if not payload.query.strip():
        raise HTTPException(status_code=400, detail="query is required")

    google_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not google_key:
        raise HTTPException(
            status_code=400,
            detail="GOOGLE_MAPS_API_KEY is not set. Add it to the API service environment on Render.",
        )

    query = payload.query.strip()
    attempts = [query, f"{query}, UAE"]

    try:
        google_last_error: str | None = None

        for q in attempts:
            zero_for_this_q = False
            for params in (
                {"address": q, "key": google_key, "region": "ae", "components": "country:AE"},
                {"address": q, "key": google_key, "region": "ae"},
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

        hint = None
        if google_last_error:
            hint = (
                f"Google Geocoding failed ({google_last_error}). "
                "Confirm GOOGLE_MAPS_API_KEY, Geocoding API enabled, and billing if required."
            )

        err_payload: dict = {
            "ok": False,
            "provider": "none",
            "result": None,
            "error": "No candidates from providers",
        }
        if google_last_error is not None:
            err_payload["google_error"] = google_last_error
        if hint:
            err_payload["hint"] = hint
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
