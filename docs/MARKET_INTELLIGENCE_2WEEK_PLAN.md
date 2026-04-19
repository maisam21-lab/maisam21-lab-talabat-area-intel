# Market intelligence — 14-day delivery plan

Context: whitespace analysis, licensee outreach (legal name + contact), pin-based coverage, KitchenPark supply overlay, and Excel-friendly exports. Built on Talabat Area Intel (Streamlit + FastAPI + Playwright).

## Week 1 — Data trust + coverage grid + supply overlay

| Day | Focus | Done when |
|-----|--------|-----------|
| **Mon** | **Legal / commercial name** — trace `legal_name` from vendor `__NEXT_DATA__` + HTML; document field mapping vs Talabat UI “info” | Short internal field map + 10-row spot-check checklist |
| **Tue** | **Contact completeness** — verify `contact_phone` / Places fallback; list known gaps (branch vs HQ) | Checklist + optional selector tweak if systematic miss |
| **Wed** | **Dubai ~5 km grid** — run `scripts/generate_dubai_km_grid.py`, import CSV, eyeball Esri/OSM | `data/dubai_grid_5km_template.csv` (or repo path) agreed with BCT |
| **Thu** | **Pin QA** — reproduce “bad pin” cases; confirm `scrape_run_meta` echo; radius slack documented | Issues logged or closed |
| **Fri** | **Supply overlay (Phase 1)** — CSV lat/lng on pin map (shipped in repo) | Ops can load KitchenPark CSV and see pins vs scrape radius |

**Exit criteria (end of week 1):** Manual sanity on 20–30 rows (legal + phone + brand); grid file for batch runs; supply visible on map.

## Week 2 — Whitespace workflow + radius decision + handoff

| Day | Focus | Done when |
|-----|--------|-----------|
| **Mon** | **Excel / whitespace export** — export columns needed for “brand in radius” pivot (brand_id, name, legal_name, phone, lat/lng, scrape_target_label, distance) | CSV template or export button spec implemented or stubbed |
| **Tue** | **Batch / grid runbook** — how to loop API over grid CSV (curl / script); wall-clock and `RESTAURANT_DETAIL_ENRICH_MAX` guidance | `docs/BATCH_SCRAPE_RUNBOOK.md` |
| **Wed** | **Driving radius / isochrone** — spike: Google Routes vs OSRM vs “polygon exclude sea”; cost + latency | 1-page decision: keep circle v1 vs phase 2 isochrone |
| **Thu** | **Heatmap + supply** — optional supply markers on density map; brand absent/present notes for Excel | Layer on heatmap or doc-only |
| **Fri** | **UAT with Dubai BCT** — joint sanity; freeze “v2” scope for stakeholders | Sign-off list + known limitations |

**Exit criteria (end of week 2):** Repeatable grid + scrape + export path; radius decision recorded; BCT-assisted QA complete.

## Already in product (do not re-scope as “new”)

- Pin + radius scrape, high-volume listing, vendor-page enrichment for unique URLs (HV), Google Places when key is set, outbound prioritization, CSV/JSON download.
- Listing harvest for URL discovery.

## Out of scope for 14 days (backlog)

- Full multi-tenant DB inside the app.
- Deliveroo / multi-aggregator merge.
- Automated legal-entity verification (lawyer-grade).

## Owners

- Engineering: Maysam — code, exports, grid script, API caps.
- QA: Dubai BCT + Maysam — manual sample checks.
