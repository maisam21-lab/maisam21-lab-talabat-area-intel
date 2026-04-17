"""Brand-level outbound acquisition view: cuisine, order proxies, store counts, and a composite priority score.

Talabat scrape does not expose true last-7-days orders; we use `estimated_orders` when present as a demand proxy.
"""

from __future__ import annotations

import math
import re
from typing import Any

import pandas as pd


def _safe_float(x: Any) -> float | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip().replace(",", "")
    if not s:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _safe_int(x: Any) -> int | None:
    f = _safe_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except (ValueError, OverflowError):
        return None


def _primary_cuisine(cuisines: Any) -> str:
    raw = str(cuisines or "").strip()
    if not raw:
        return "Unknown"
    for sep in (",", "|", ";", "/"):
        if sep in raw:
            raw = raw.split(sep)[0].strip()
            break
    return raw[:120] if raw else "Unknown"


def _brand_key(row: pd.Series) -> str:
    bid = str(row.get("brand_id") or "").strip()
    if bid:
        return f"brand_id:{bid}"
    rid = str(row.get("talabat_restaurant_id") or "").strip()
    if rid:
        return f"id:{rid}"
    name = str(row.get("restaurant_name") or "").strip()
    base = name.split(" - ", 1)[0].strip().lower()
    return f"name:{base}" if base else f"sku:{row.get('branch_sku', '')}"


def _median_delivery_fee_aed(series: pd.Series) -> float | None:
    vals: list[float] = []
    for s in series.dropna().astype(str):
        m = re.search(r"(\d+(?:\.\d+)?)\s*(?:aed|د\.إ)?", s.lower().replace(" ", ""))
        if m:
            try:
                vals.append(float(m.group(1)))
            except ValueError:
                pass
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    m = n // 2
    if n % 2 == 1:
        return float(vals[m])
    return (vals[m - 1] + vals[m]) / 2.0


def _sum_reviews(g: pd.DataFrame) -> int:
    total = 0
    for col in ("reviews_count", "google_reviews_count"):
        if col not in g.columns:
            continue
        for x in g[col].tolist():
            n = _safe_int(x)
            if n is not None and n > 0:
                total += n
    return total


def build_brand_prioritization_table(df: pd.DataFrame) -> pd.DataFrame:
    """One row per brand cluster with metrics for outbound targeting."""
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    for c in (
        "talabat_restaurant_id",
        "brand_id",
        "brand_display_name",
        "restaurant_name",
        "cuisines",
        "estimated_orders",
        "rating",
        "google_rating",
        "reviews_count",
        "google_reviews_count",
        "delivery_fee",
        "scrape_city",
        "scrape_target_label",
    ):
        if c not in work.columns:
            work[c] = ""

    work["_brand_key"] = work.apply(_brand_key, axis=1)
    work["_primary_cuisine"] = work["cuisines"].map(_primary_cuisine)
    work["_est_int"] = work["estimated_orders"].map(_safe_int)

    rows: list[dict[str, Any]] = []
    for key, g in work.groupby("_brand_key", dropna=False):
        name_series = g["restaurant_name"].astype(str).str.strip()
        stems = name_series.str.split(" - ", n=1, expand=False).str[0]
        mode_stem = stems.mode()
        if len(mode_stem) > 0:
            display_name = str(mode_stem.iloc[0])
        elif len(stems) > 0:
            display_name = str(stems.iloc[0])
        else:
            display_name = str(key)
        cuisine_mode = g["_primary_cuisine"].mode()
        primary = str(cuisine_mode.iloc[0]) if len(cuisine_mode) else "Unknown"
        n_stores = int(len(g))
        est_vals = [x for x in g["_est_int"].tolist() if x is not None and x > 0]
        # If Talabat repeats chain-level total on each branch, max ≈ total; if per-branch, sum is safer.
        est_proxy = int(max(est_vals)) if est_vals else None
        if est_vals and len(est_vals) > 1:
            if max(est_vals) == min(est_vals):
                est_proxy = max(est_vals)
            else:
                est_proxy = int(sum(est_vals))

        rb = None
        for _, row in g.iterrows():
            for col in ("rating", "google_rating"):
                v = _safe_float(row.get(col))
                if v is not None and 0 <= v <= 5:
                    rb = v if rb is None else max(rb, v)

        rev_sum = _sum_reviews(g)
        med_fee = _median_delivery_fee_aed(g["delivery_fee"]) if "delivery_fee" in g else None
        cities = sorted({str(x).strip() for x in g["scrape_city"].dropna().astype(str) if str(x).strip()})
        targets = sorted({str(x).strip() for x in g["scrape_target_label"].dropna().astype(str) if str(x).strip()})
        bid_series = g["brand_id"].astype(str).str.strip()
        bid_series = bid_series[bid_series != ""]
        brand_id_cell = str(bid_series.iloc[0]) if len(bid_series) else ""

        opr = None
        if est_proxy is not None and n_stores > 0:
            opr = round(float(est_proxy) / float(n_stores), 2)

        rows.append(
            {
                "_brand_key": key,
                "brand_id": brand_id_cell,
                "Brand": display_name[:200],
                "Primary Cuisine": primary,
                "Est_orders_proxy": est_proxy,
                "Number of Stores": n_stores,
                "Orders_per_restaurant": opr,
                "_rating_best": rb,
                "_reviews_sum": rev_sum,
                "_median_delivery_aed": med_fee,
                "Cities_present": ", ".join(cities) if cities else "",
                "Targets_present": ", ".join(targets) if targets else "",
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # Fill NaN for scoring
    out["_rating_best"] = out["_rating_best"].fillna(0.0)
    out["_reviews_sum"] = out["_reviews_sum"].fillna(0).astype(int)
    med_col = out["_median_delivery_aed"]
    if med_col.notna().any():
        out["_median_delivery_aed"] = med_col.fillna(med_col.median())
    out["_median_delivery_aed"] = out["_median_delivery_aed"].fillna(15.0)

    return out


def add_priority_scores(
    brand_df: pd.DataFrame,
    *,
    w_rating: float,
    w_reviews: float,
    w_orders: float,
    w_delivery: float,
    w_scale: float,
) -> pd.DataFrame:
    """Add 0–100 `Outbound_priority` and `Target_tier` (S/A/B/C) using min-max normalization within this scrape."""
    if brand_df is None or brand_df.empty:
        return brand_df

    out = brand_df.copy()
    total_w = w_rating + w_reviews + w_orders + w_delivery + w_scale
    if total_w <= 0:
        total_w = 1.0
    wr, wrev, wo, wd, ws = w_rating / total_w, w_reviews / total_w, w_orders / total_w, w_delivery / total_w, w_scale / total_w

    def _norm(series: pd.Series, invert: bool = False) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce").astype(float)
        lo, hi = float(s.min()), float(s.max())
        if hi <= lo or math.isnan(lo):
            return pd.Series(0.5, index=s.index)
        x = (s - lo) / (hi - lo)
        if invert:
            x = 1.0 - x
        return x.clip(0, 1)

    r_score = _norm(out["_rating_best"])
    rev_score = _norm(np_log1p_safe(out["_reviews_sum"]))
    ord_series = pd.to_numeric(out["Est_orders_proxy"], errors="coerce").fillna(0)
    ord_score = _norm(np_log1p_safe(ord_series))
    del_score = _norm(out["_median_delivery_aed"], invert=True)
    scale_score = _norm(np_log1p_safe(out["Number of Stores"]))

    out["Outbound_priority"] = (
        wr * r_score + wrev * rev_score + wo * ord_score + wd * del_score + ws * scale_score
    ) * 100.0
    out["Outbound_priority"] = out["Outbound_priority"].round(1)

    q = out["Outbound_priority"].quantile([0.85, 0.65, 0.35])
    s_hi = float(q.loc[0.85]) if pd.notna(q.loc[0.85]) else 90.0
    a_hi = float(q.loc[0.65]) if pd.notna(q.loc[0.65]) else 70.0
    b_hi = float(q.loc[0.35]) if pd.notna(q.loc[0.35]) else 50.0

    def tier(x: float) -> str:
        if x >= s_hi:
            return "S"
        if x >= a_hi:
            return "A"
        if x >= b_hi:
            return "B"
        return "C"

    out["Target_tier"] = out["Outbound_priority"].map(tier)
    return out


def np_log1p_safe(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce").fillna(0).astype(float)
    return pd.Series([math.log1p(max(0.0, v)) for v in x], index=x.index)


def format_for_dashboard(brand_df: pd.DataFrame) -> pd.DataFrame:
    """Human column names for the UI (matches KitchenPark-style outbound sheet)."""
    if brand_df is None or brand_df.empty:
        return brand_df
    d = brand_df.copy()
    d = d.rename(
        columns={
            "Est_orders_proxy": "Est. orders (Talabat proxy)",
            "Number of Stores": "Number of Stores",
            "Orders_per_restaurant": "Orders per restaurant",
            "brand_id": "Brand ID",
            "Targets_present": "Target labels",
        }
    )
    cols = [
        "Target_tier",
        "Outbound_priority",
        "Brand ID",
        "Brand",
        "Primary Cuisine",
        "Est. orders (Talabat proxy)",
        "Number of Stores",
        "Orders per restaurant",
        "Cities_present",
        "Target labels",
    ]
    cols = [c for c in cols if c in d.columns]
    return d[cols].sort_values("Outbound_priority", ascending=False)


MODEL_HELP = (
    "**How this model works (v1)**\n"
    "- **Primary Cuisine**: first cuisine tag from Talabat listing text.\n"
    "- **Est. orders (Talabat proxy)**: best aggregate of `estimated_orders` from the scrape when Talabat exposes it "
    "(not guaranteed to be last 7 days).\n"
    "- **Number of Stores**: rows grouped by **brand_id** when present, else Talabat restaurant id, else brand name stem.\n"
    "- **Orders per restaurant**: proxy ÷ store count when a proxy exists.\n"
    "- **Outbound_priority**: weighted blend of rating quality, review volume, order proxy, cheaper delivery fee, "
    "and footprint (more stores → higher reach). Tiers **S/A/B/C** are quantiles within this run.\n"
    "Tune weights below; enrich vendor pages for fuller Talabat fields."
)
