#!/usr/bin/env python3
"""
Generate a latitude/longitude grid (~spacing_km) over a Dubai-focused bounding box
for batch scrapes / whitespace coverage (meeting: pins every ~5 km — validate on a map).

Default bbox is a broad Dubai emirate–oriented rectangle; edit DUBAI_BBOX in this file
or pass --lat-min/--lat-max/--lng-min/--lng-max for your own AOI.

Usage:
  py -3 scripts/generate_dubai_km_grid.py --out dubai_grid_5km.csv
  py -3 scripts/generate_dubai_km_grid.py --spacing-km 5 --out grid.csv
"""

from __future__ import annotations

import argparse
import csv
import math


# Approximate SW / NE corners (decimal degrees). Widen or narrow after map QA.
DUBAI_BBOX = (24.88, 55.00, 25.48, 55.75)  # lat_min, lng_min, lat_max, lng_max


def km_to_dlat(km: float) -> float:
    return km / 110.574


def km_to_dlng(km: float, at_lat_deg: float) -> float:
    cos_lat = max(0.2, math.cos(math.radians(at_lat_deg)))
    return km / (111.320 * cos_lat)


def generate_grid(
    lat_min: float,
    lng_min: float,
    lat_max: float,
    lng_max: float,
    spacing_km: float,
) -> list[tuple[str, float, float]]:
    """Return list of (cell_id, lat, lng) covering bbox in a simple raster."""
    if lat_min >= lat_max or lng_min >= lng_max or spacing_km <= 0:
        return []
    mid_lat = (lat_min + lat_max) / 2.0
    dlat = km_to_dlat(spacing_km)
    dlng = km_to_dlng(spacing_km, mid_lat)
    rows: list[tuple[str, float, float]] = []
    ri = 0
    lat = lat_min
    while lat <= lat_max + 1e-9:
        ci = 0
        lng = lng_min
        while lng <= lng_max + 1e-9:
            cell_id = f"D{ri:03d}_C{ci:03d}"
            rows.append((cell_id, round(lat, 6), round(lng, 6)))
            ci += 1
            lng += dlng
        ri += 1
        lat += dlat
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate lat/lng grid CSV for Dubai-area batch scrapes.")
    ap.add_argument("--spacing-km", type=float, default=5.0, help="Approximate spacing between grid points (km).")
    ap.add_argument("--out", default="dubai_km_grid.csv", help="Output CSV path.")
    ap.add_argument("--lat-min", type=float, default=None)
    ap.add_argument("--lat-max", type=float, default=None)
    ap.add_argument("--lng-min", type=float, default=None)
    ap.add_argument("--lng-max", type=float, default=None)
    args = ap.parse_args()

    lat_min = float(args.lat_min) if args.lat_min is not None else DUBAI_BBOX[0]
    lng_min = float(args.lng_min) if args.lng_min is not None else DUBAI_BBOX[1]
    lat_max = float(args.lat_max) if args.lat_max is not None else DUBAI_BBOX[2]
    lng_max = float(args.lng_max) if args.lng_max is not None else DUBAI_BBOX[3]

    pts = generate_grid(lat_min, lng_min, lat_max, lng_max, float(args.spacing_km))
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cell_id", "lat", "lng", "suggested_radius_km", "notes"])
        for cell_id, la, ln in pts:
            w.writerow([cell_id, la, ln, 10.0, "Validate on map; tune radius per micro-market"])
    print(f"Wrote {len(pts)} points to {args.out}")


if __name__ == "__main__":
    main()
