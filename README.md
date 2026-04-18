# Talabat Area Intel

Area-first UAE restaurant intelligence with:

- visual map pin placement
- radius filtering (`5 km`, `10 km`, custom)
- status filter (`all`, `live`, `closed`)
- Just Landed toggle
- branch-level unique SKU (`branch_sku`)
- density heatmap

Scraping collects vendor links in the form `https://www.talabat.com/uae/{slug}` (Talabat’s main listing pattern), with `/restaurant/...` as a secondary pattern.

## Architecture

- `talabat_area_intel_app.py` -> Streamlit frontend (can run on Streamlit Cloud)
- `scraper_api.py` -> FastAPI scraping backend (run on Render with Docker)
- `scrape_engine.py` -> shared Playwright scraping engine
- `talabat_urls.py` -> UAE vendor path rules (`/uae/{slug}`)

## Local run (all-in-one)

```bash
py -3 -m pip install -r requirements.txt
py -3 -m playwright install chromium
py -3 -m streamlit run talabat_area_intel_app.py
```

For production, prefer split deployment (frontend + backend API).

## Split deployment (recommended)

### 1) Backend on Render

- Create a new Render Web Service from this repo/folder
- Docker file: `Dockerfile.api`
- Start command is already in Dockerfile:
  - `uvicorn scraper_api:app --host 0.0.0.0 --port 8000`

Health endpoint:

- `GET /health`

Scrape endpoint:

- `POST /scrape`

Geocode endpoint:

- `POST /geocode`
- `POST /geocode` and `POST /scrape` require header `X-API-Key` when `SCRAPER_API_KEY` is configured.

### 2) Frontend on Streamlit Cloud

- Deploy `talabat_area_intel_app.py`
- In sidebar set `API base URL` to your Render URL (example: `https://your-service.onrender.com`)
- Set `ARCGIS_API_KEY` on Render service env vars (preferred for backend `/geocode`).
- Optional fallback: `GOOGLE_MAPS_API_KEY`.
- Set `SCRAPER_API_KEY` on Render service env vars.
- Optional tuning (Render stability):
  - `MAX_SCRAPE_SAMPLE_POINTS` (default `8`) caps how many pin samples one `/scrape` run will visit (raise carefully; long runs can hit HTTP timeouts).
  - **Fuller rows:** high-volume runs enrich **one pass per unique vendor URL** (up to `RESTAURANT_DETAIL_ENRICH_MAX`, default **240** when `high_volume` is true unless you set the env). Set `SCRAPER_ENRICH_UNIQUE_VENDORS=1` for the same behavior on non-HV runs. Cap total URLs with `SCRAPER_VENDOR_ENRICH_HARD_CAP` (default 800). Tune `GOOGLE_PLACES_ENRICH_MAX` (default **48**) for more Places backfill.
- Set same `SCRAPER_API_KEY` in Streamlit secrets:

```toml
SCRAPER_API_KEY = "your_strong_shared_secret"
API_BASE_URL = "https://your-service.onrender.com"
```

The frontend sends scrape and geocode requests to Render and then displays/downloads results.

## API payload example

```json
{
  "pin_lat": 25.2048,
  "pin_lng": 55.2708,
  "radius_km": 10.0,
  "spacing_km": 1.5,
  "concurrency": 3,
  "status_filter": "all",
  "just_landed_only": false,
  "scroll_rounds": 22,
  "scroll_wait_ms": 1300
}
```

## Notes and limits

- This uses public website content and selectors that may change over time.
- Streamlit Cloud alone is not reliable for Playwright subprocess scraping.
- Keep scraping on backend infrastructure (Render Docker) for stability.
- Streamlit Cloud frontend should always point to Render backend for scraping/geocoding.
- Geocoding provider order: ArcGIS first (`ARCGIS_API_KEY`), then Google fallback.
- Enable `SCRAPER_API_KEY` in production to prevent public API abuse.
- Use responsibly and in compliance with platform terms and local regulations.
