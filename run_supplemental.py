"""
Supplemental run for Sharjah and Abu Dhabi facilities.
Merges results into an existing whitespace Excel or produces a standalone file.

Usage:
  python run_supplemental.py                              # standalone output
  python run_supplemental.py --merge kp_whitespace_v2.xlsx  # merge into existing
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from whitespace_analysis import (
    FACILITIES,
    scrape_facility,
    build_matrix,
    export_excel,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("supplemental")

# Facilities not covered well in the main Dubai run
SUPPLEMENTAL_FACILITIES = [
    f for f in FACILITIES
    if f["go_live"] == "Live" and f["emirate"] in ("Sharjah", "Abu Dhabi")
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--radius", type=float, default=10.0)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--output", type=str, default="kp_supplemental.xlsx")
    args = parser.parse_args()

    logger.info("Supplemental facilities: %s", [f["name"] for f in SUPPLEMENTAL_FACILITIES])

    facility_vendors: dict = {}
    facility_meta: dict = {}

    import time
    for i, facility in enumerate(SUPPLEMENTAL_FACILITIES, 1):
        logger.info("[%d/%d] %s", i, len(SUPPLEMENTAL_FACILITIES), facility["name"])
        try:
            vendors, meta = scrape_facility(
                facility,
                radius_km=args.radius,
                max_pages=args.max_pages,
                page_delay=0.5,
            )
            facility_vendors[facility["name"]] = vendors
            facility_meta[facility["name"]] = meta
        except Exception as exc:
            logger.error("Failed: %s — %s", facility["name"], exc)
            facility_vendors[facility["name"]] = []
            facility_meta[facility["name"]] = {"error": str(exc)}
        if i < len(SUPPLEMENTAL_FACILITIES):
            time.sleep(15)

    matrix_df, raw_df = build_matrix(facility_vendors)
    export_excel(matrix_df, raw_df, SUPPLEMENTAL_FACILITIES, facility_meta, args.output, args.radius)

    print(f"\nDone — {len(matrix_df)} brands across {len(SUPPLEMENTAL_FACILITIES)} facilities → {args.output}")


if __name__ == "__main__":
    main()
