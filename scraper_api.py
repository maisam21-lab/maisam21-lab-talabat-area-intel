from __future__ import annotations

import os

import requests
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from scrape_engine import run_area_scrape

app = FastAPI(title="Talabat Area Scraper API", version="1.0.0")


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
    status_filter: str = Field(default="all")
    just_landed_only: bool = False
    scroll_rounds: int = Field(default=10, ge=4, le=60)
    scroll_wait_ms: int = Field(default=900, ge=600, le=3000)


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

    arcgis_key = os.getenv("ARCGIS_API_KEY", "").strip()
    google_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

    if not arcgis_key and not google_key:
        raise HTTPException(
            status_code=400,
            detail="No geocoding key configured. Set ARCGIS_API_KEY (preferred) or GOOGLE_MAPS_API_KEY.",
        )

    query = payload.query.strip()
    attempts = [query, f"{query}, UAE"]

    try:
        # Preferred geocoder: ArcGIS
        if arcgis_key:
            for q in attempts:
                arc_resp = requests.get(
                    "https://geocode-api.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates",
                    params={
                        "f": "json",
                        "singleLine": q,
                        "countryCode": "AE",
                        "maxLocations": 1,
                        "forStorage": "false",
                        "token": arcgis_key,
                    },
                    timeout=20,
                )
                arc_resp.raise_for_status()
                arc_data = arc_resp.json()

                if arc_data.get("error"):
                    return {
                        "ok": False,
                        "provider": "arcgis",
                        "result": None,
                        "error": arc_data.get("error"),
                    }

                candidates = arc_data.get("candidates") or []
                if candidates:
                    top = candidates[0]
                    location = top.get("location") or {}
                    if location.get("y") is not None and location.get("x") is not None:
                        return {
                            "ok": True,
                            "provider": "arcgis",
                            "result": {
                                "lat": float(location.get("y")),
                                "lng": float(location.get("x")),
                                "formatted_address": str(top.get("address") or q).strip(),
                            },
                        }

        # Fallback geocoder: Google (optional)
        if google_key:
            for q in attempts:
                g_resp = requests.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"address": q, "key": google_key, "region": "ae"},
                    timeout=20,
                )
                g_resp.raise_for_status()
                g_data = g_resp.json()
                results = g_data.get("results") or []
                if results:
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

        return {"ok": False, "provider": "none", "result": None, "error": "No candidates from providers"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Geocode failed: {exc}") from exc


@app.post("/scrape")
async def scrape(payload: ScrapeRequest, x_api_key: str | None = Header(default=None)) -> dict:
    verify_api_key(x_api_key)
    if payload.status_filter not in {"all", "live", "closed"}:
        raise HTTPException(status_code=400, detail="status_filter must be one of: all, live, closed")
    try:
        df = await run_area_scrape(
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
        )
        records = df.to_dict(orient="records")
        return {"count": len(records), "records": records}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Scrape failed: {exc}") from exc
