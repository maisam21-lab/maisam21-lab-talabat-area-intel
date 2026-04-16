from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from scrape_engine import run_area_scrape

app = FastAPI(title="Talabat Area Scraper API", version="1.0.0")


class ScrapeRequest(BaseModel):
    pin_lat: float
    pin_lng: float
    radius_km: float = Field(default=5.0, ge=1.0, le=30.0)
    spacing_km: float = Field(default=1.0, ge=0.5, le=3.0)
    concurrency: int = Field(default=3, ge=1, le=6)
    status_filter: str = Field(default="all")
    just_landed_only: bool = False
    scroll_rounds: int = Field(default=22, ge=6, le=60)
    scroll_wait_ms: int = Field(default=1300, ge=600, le=3000)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/scrape")
async def scrape(payload: ScrapeRequest) -> dict:
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
