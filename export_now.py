"""
Quick export from checkpoint — sorted by highest total branch count across all scraped facilities.
Usage: python export_now.py [checkpoint.json] [output.xlsx]
"""
from __future__ import annotations
import json, sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from whitespace_analysis import build_matrix, export_excel, FACILITIES

DEFAULT_CKPT  = "kp_whitespace_v2_checkpoint.json"
DEFAULT_OUT   = "kp_whitespace_v2_PARTIAL.xlsx"

ckpt_path  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CKPT
out_path   = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT

with open(ckpt_path, encoding="utf-8") as f:
    data = json.load(f)

facility_vendors = {name: entry["vendors"] for name, entry in data["facilities"].items()}
facility_meta    = {name: entry["meta"]    for name, entry in data["facilities"].items()}
radius_km        = data.get("radius_km", 10.0)

done_names = list(facility_vendors.keys())
done_facilities = [f for f in FACILITIES if f["name"] in done_names]

print(f"Facilities in checkpoint: {done_names}")

matrix_df, raw_df = build_matrix(facility_vendors)

# Sort matrix by total branch count descending (not just presence count)
if not matrix_df.empty:
    fac_cols = [c for c in matrix_df.columns if c not in ("restaurant_id", "brand_name", "cuisine")]
    matrix_df["_total_branches"] = matrix_df[fac_cols].sum(axis=1)
    matrix_df["_total_facilities"] = (matrix_df[fac_cols] > 0).sum(axis=1)
    matrix_df = matrix_df.sort_values(
        ["_total_facilities", "_total_branches", "brand_name"],
        ascending=[False, False, True]
    ).drop(columns=["_total_branches", "_total_facilities"]).reset_index(drop=True)

# Sort raw records by facility then descending vendor count
if not raw_df.empty and "kp_facility" in raw_df.columns:
    raw_df = raw_df.sort_values(["kp_facility", "name"], ascending=[True, True]).reset_index(drop=True)

export_excel(matrix_df, raw_df, done_facilities, facility_meta, out_path, radius_km=radius_km)

print(f"\nExported {len(done_facilities)} facilities | {len(matrix_df)} brands | {len(raw_df)} raw rows")
print(f"Output -> {out_path}")
