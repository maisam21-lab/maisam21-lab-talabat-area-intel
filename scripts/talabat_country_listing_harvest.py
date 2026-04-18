#!/usr/bin/env python3
"""
Harvest Talabat vendor URLs from a country-wide "restaurants" listing by walking
"next" pagination — same idea as the Egypt Kaggle notebook, with country configurable:

  https://github.com/Marwan-xDiab/Talabat-Egypt-Restaurant-Web-Scraping-

Default is UAE (English listing). Requires: playwright install chromium

Usage:
  py -3 scripts/talabat_country_listing_harvest.py --country uae --out uae_vendor_links.csv
  py -3 scripts/talabat_country_listing_harvest.py --country egypt --listing-url https://www.talabat.com/egypt/restaurants
"""

from __future__ import annotations

import argparse
import asyncio
import csv

from listing_harvest import (
    country_path_slug,
    default_listing_url_for_slug,
    harvest_vendor_urls,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Harvest Talabat listing vendor URLs (country-style notebook).")
    ap.add_argument("--country", default="uae", help="Country key: uae, egypt, or Talabat path slug (e.g. kuwait).")
    ap.add_argument(
        "--listing-url",
        default="",
        help="Override full listing URL (default: built-in per --country).",
    )
    ap.add_argument("--out", default="talabat_vendor_links.csv", help="Output CSV (one column: url).")
    ap.add_argument("--max-next", type=int, default=80, help="Max 'next' pagination clicks (safety cap).")
    ap.add_argument("--headed", action="store_true", help="Show browser window.")
    args = ap.parse_args()

    slug = country_path_slug(args.country)
    listing = (args.listing_url or "").strip() or default_listing_url_for_slug(slug)

    links = asyncio.run(
        harvest_vendor_urls(
            listing,
            country_slug=slug,
            max_next=max(0, args.max_next),
            headless=not args.headed,
        )
    )
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url", "country_slug"])
        for u in links:
            w.writerow([u, slug])
    print(f"Wrote {len(links)} rows to {args.out}")


if __name__ == "__main__":
    main()
