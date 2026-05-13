#!/usr/bin/env python3
"""
Run multiple /scrape jobs on a hex grid of pins inside a circle, then merge rows.

Talabat listing discovery is pin- and hub-sensitive; one center often plateaus at a few
dozen unique vendor URLs. Tiling pins (same as the app's batch client, but N>2) usually
raises merged coverage before dedupe.

Environment (same as Streamlit worker):
  API_BASE_URL     e.g. http://127.0.0.1:8000 (inside api container) or your HTTPS API
  SCRAPER_API_KEY  must match SCRAPER_API_KEY on the API

Example (from repo root on the host, API published on localhost:8000):
  python scripts/tile_radius_merged_scrape.py --center-lat 25.08 --center-lng 55.14 \\
    --radius-km 10 --tile-spacing-km 3.5 --max-pins 8 -o jbr_merged.csv

Example (exec into api container; set API_BASE_URL — the api service often has no default):
  docker compose exec -T -e API_BASE_URL=http://127.0.0.1:8000 api \\
    python /app/scripts/tile_radius_merged_scrape.py ...
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _profiles() -> dict[str, dict]:
    return {
        "Fast": {
            "high_volume": False,
            "max_sample_points": 20,
            "scroll_rounds": 6,
            "scroll_wait_ms": 500,
            "google_places_enrich": True,
            "enrich": False,
            "scrape_wall_clock_sec": 900,
        },
        "Balanced": {
            "high_volume": False,
            "max_sample_points": 20,
            "scroll_rounds": 6,
            "scroll_wait_ms": 500,
            "google_places_enrich": True,
            "enrich": False,
            "scrape_wall_clock_sec": 1800,
        },
        "Complete": {
            "high_volume": True,
            "max_sample_points": 20,
            "scroll_rounds": 6,
            "scroll_wait_ms": 500,
            "google_places_enrich": True,
            "enrich": False,
            "scrape_wall_clock_sec": 3300,
        },
        "Worker": {
            "high_volume": True,
            "max_sample_points": 90,
            "scroll_rounds": 8,
            "scroll_wait_ms": 800,
            "google_places_enrich": True,
            "enrich": True,
            "scrape_wall_clock_sec": 3600,
        },
    }


def main() -> int:
    repo = _repo_root()
    sys.path.insert(0, str(repo))

    import pandas as pd

    from batch_scrape_client import run_dual_area_scrape_via_api
    from geo_utils import generate_points_in_radius, haversine_series_km_from_pin

    ap = argparse.ArgumentParser(description="Tile pins inside a circle and merge scrape results.")
    ap.add_argument("--center-lat", type=float, required=True)
    ap.add_argument("--center-lng", type=float, required=True)
    ap.add_argument("--radius-km", type=float, required=True, help="Talabat search radius per pin.")
    ap.add_argument(
        "--tile-disk-km",
        type=float,
        default=None,
        help="Hex grid is drawn inside this radius from center (default: 0.88 * radius-km).",
    )
    ap.add_argument("--tile-spacing-km", type=float, default=3.5, help="Hex spacing for extra pins.")
    ap.add_argument("--max-pins", type=int, default=8, help="Cap grid pins (center counts as one).")
    ap.add_argument(
        "--profile",
        type=str,
        default="Complete",
        choices=list(_profiles().keys()),
        help="Scrape profile (wall clock and grid intensity).",
    )
    ap.add_argument("--dedupe-by-vendor-url", action="store_true", default=True)
    ap.add_argument("--no-dedupe-by-vendor-url", action="store_false", dest="dedupe_by_vendor_url")
    ap.add_argument(
        "--merge-dedupe",
        action="store_true",
        default=True,
        help="After concat, drop duplicate non-empty restaurant_url across tiles.",
    )
    ap.add_argument("--no-merge-dedupe", action="store_false", dest="merge_dedupe")
    ap.add_argument(
        "--clip-to-center-radius",
        type=float,
        default=None,
        help="Keep rows whose lat/lng are within this km of --center (default: same as --radius-km).",
    )
    ap.add_argument("--no-clip", action="store_true", help="Disable distance clip to center.")
    ap.add_argument("-o", "--output", type=str, required=True, help="Output CSV path.")
    args = ap.parse_args()

    api_base = os.getenv("API_BASE_URL", "").strip().rstrip("/")
    api_key = os.getenv("SCRAPER_API_KEY", "").strip()
    if not api_base:
        print("ERROR: set API_BASE_URL (e.g. http://127.0.0.1:8000)", file=sys.stderr)
        return 2
    if not api_key:
        print("ERROR: set SCRAPER_API_KEY", file=sys.stderr)
        return 2

    disk_km = float(args.tile_disk_km) if args.tile_disk_km is not None else float(args.radius_km) * 0.88
    pins = generate_points_in_radius(args.center_lat, args.center_lng, disk_km, float(args.tile_spacing_km))
    pins = pins[: max(1, int(args.max_pins))]

    prof = _profiles()[args.profile]
    wall = int(prof.get("scrape_wall_clock_sec") or 1800)
    poll_budget = float(wall) + 600.0

    spacing_km = 1.8
    concurrency = 3

    base_payload: dict = {
        "radius_km": float(args.radius_km),
        "spacing_km": spacing_km,
        "concurrency": concurrency,
        "scroll_rounds": int(prof["scroll_rounds"]),
        "scroll_wait_ms": int(prof["scroll_wait_ms"]),
        "status_filter": "all",
        "just_landed_only": False,
        "max_sample_points": int(prof["max_sample_points"]),
        "dedupe_by_vendor_url": bool(args.dedupe_by_vendor_url),
        "high_volume": bool(prof["high_volume"]),
        "google_places_enrich": bool(prof["google_places_enrich"]),
        "enrich": bool(prof.get("enrich", False)),
        "scrape_target_label": None,
        "city": None,
        "scrape_wall_clock_sec": wall,
    }

    rows = []
    for i, (lat, lng) in enumerate(pins):
        rows.append(
            {
                "lat": float(lat),
                "lng": float(lng),
                "label": f"tile_{i+1}",
                "area_slot": f"T{i+1}",
            }
        )
    locations_df = pd.DataFrame(rows)

    print(
        f"API={api_base} profile={args.profile} pins={len(locations_df)} "
        f"radius_km={args.radius_km} tile_disk_km={disk_km} poll_budget_sec_per_pin≈{poll_budget:.0f}",
        flush=True,
    )

    merged, errors = run_dual_area_scrape_via_api(
        api_base_url=api_base,
        headers={"X-API-Key": api_key},
        locations_df=locations_df,
        base_payload=base_payload,
        timeout_sec=poll_budget,
    )

    raw_n = len(merged)
    if args.merge_dedupe and not merged.empty and "restaurant_url" in merged.columns:
        m = merged.copy()
        m["_u"] = m["restaurant_url"].fillna("").astype(str).str.strip()
        part_a = m.loc[m["_u"] == ""].drop(columns=["_u"], errors="ignore")
        part_b = m.loc[m["_u"] != ""].drop_duplicates(subset=["_u"], keep="first").drop(columns=["_u"], errors="ignore")
        merged = pd.concat([part_a, part_b], ignore_index=True)

    clip_r = None if args.no_clip else (float(args.clip_to_center_radius) if args.clip_to_center_radius else float(args.radius_km))
    if clip_r is not None and not merged.empty and "lat" in merged.columns and "lng" in merged.columns:
        d = haversine_series_km_from_pin(args.center_lat, args.center_lng, merged["lat"], merged["lng"])
        merged = merged.loc[d <= clip_r + 0.05].copy()

    out_path = Path(args.output).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)

    print(f"Wrote {out_path} rows={len(merged)} (raw_concat≈{raw_n})", flush=True)
    if errors:
        print("Warnings/errors:", file=sys.stderr)
        for e in errors[:20]:
            print(f"  {e}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more", file=sys.stderr)
    if merged.empty:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
