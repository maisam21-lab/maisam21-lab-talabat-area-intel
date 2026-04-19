"""Normalize Kitchen Park / supply CSV rows for map overlays (lat, lng, label)."""

from __future__ import annotations

import pandas as pd


def normalize_supply_overlay_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Return a copy with columns ``lat``, ``lng``, ``label`` or None if no usable coordinates."""
    if df is None or df.empty:
        return None
    work = df.copy()
    colmap = {str(c).strip().lower(): c for c in work.columns}

    def pick(*candidates: str) -> str | None:
        for name in candidates:
            if name in colmap:
                return colmap[name]
            for k, orig in colmap.items():
                if k.replace(" ", "").replace("_", "") == name.replace(" ", "").replace("_", ""):
                    return orig
        return None

    lat_c = pick("lat", "latitude", "y")
    lng_c = pick("lng", "lon", "long", "longitude", "x")
    if not lat_c or not lng_c:
        return None
    name_c = pick("name", "label", "site", "kitchen", "location", "id")

    out = pd.DataFrame(
        {
            "lat": pd.to_numeric(work[lat_c], errors="coerce"),
            "lng": pd.to_numeric(work[lng_c], errors="coerce"),
        }
    )
    if name_c and name_c in work.columns:
        out["label"] = work[name_c].astype(str).str.strip()
    else:
        out["label"] = ""

    out = out.dropna(subset=["lat", "lng"])
    out = out[(out["lat"].between(-90, 90)) & (out["lng"].between(-180, 180))]
    if out.empty:
        return None
    return out.reset_index(drop=True)
