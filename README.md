# Talabat Area Intel

Area-first UAE restaurant intelligence with:

- visual map pin placement
- radius filtering (`5 km`, `10 km`, custom)
- status filter (`all`, `live`, `closed`)
- Just Landed toggle
- branch-level unique SKU (`branch_sku`)
- new branch detection vs previous CSV
- density heatmap

## Architecture

- `talabat_area_intel_app.py` -> Streamlit frontend (can run on Streamlit Cloud)
- `scraper_api.py` -> FastAPI scraping backend (run on Render with Docker)
- `scrape_engine.py` -> shared Playwright scraping engine

## Local run (all-in-one)

```bash
py -3 -m pip install -r requirements.txt
py -3 -m playwright install chromium
py -3 -m streamlit run talabat_area_intel_app.py
```

Use frontend mode `Local Playwright`.

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

### 2) Frontend on Streamlit Cloud

- Deploy `talabat_area_intel_app.py`
- In sidebar, select:
  - `Run mode` = `Remote API (Render)`
  - `API base URL` = your Render URL (example: `https://your-service.onrender.com`)

The frontend sends scrape jobs to Render and then displays/downloads results.

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
- Use responsibly and in compliance with platform terms and local regulations.
