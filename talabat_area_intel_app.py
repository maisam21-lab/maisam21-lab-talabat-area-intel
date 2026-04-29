from __future__ import annotations

import html
import io
import math
import os
import threading
import time
import uuid
from datetime import datetime

import folium
import pandas as pd
import requests
import streamlit as st
from folium.plugins import Fullscreen, HeatMap, MousePosition
from streamlit_folium import st_folium

from outbound_prioritization import (
    MODEL_HELP,
    add_priority_scores,
    build_brand_prioritization_table,
    format_for_dashboard,
)
from pin_validation import parse_scrape_pin_or_raise_value_error
from streamlit_location import (
    ensure_scrape_location,
    folium_center_vs_location_mismatch,
    get_scrape_location,
    seed_city_preset_if_changed,
    set_scrape_location,
    store_folium_payload,
    sync_legacy_pin_mirror,
)
try:
    from batch_scrape_client import run_dual_area_scrape_via_api
except ImportError:
    # Deployment-safe fallback when an older module version is present.
    from batch_scrape_client import run_batch_scrape_via_api as run_dual_area_scrape_via_api
from google_map_tiles import (
    ensure_google_map_tile_sessions,
    google_2d_tile_url_template,
    google_maps_tile_attribution,
)
from supply_overlay import normalize_supply_overlay_df
from uae_cities import UAE_CITY_DISPLAY, UAE_CITY_PRESETS

DEFAULT_PIN = (25.2048, 55.2708)

# More grid points + deeper scroll = more listing URLs merged (slower; watch SCRAPER_WALL_CLOCK_SEC on Render).
_DEFAULT_MAX_SAMPLE_POINTS = 6
_DEFAULT_SPACING_KM = 1.8
_DEFAULT_SCROLL_ROUNDS = 6
_DEFAULT_SCROLL_WAIT_MS = 500
_DEFAULT_CONCURRENCY = 3

_CITY_SLUGS = ["dubai", "sharjah", "abudhabi", "alain", "ajman"]

# Product defaults (no client toggles): full grid + cuisine sweep, keep all listing rows, request Places enrichment.
_SCRAPE_DEDUPE_BY_VENDOR_URL = False
_SCRAPE_HIGH_VOLUME = True
_SCRAPE_MAX_SAMPLE_POINTS = 6
_SCRAPE_CLIENT_TIMEOUT_SEC = 1300

_SCRAPE_PROFILES: dict[str, dict] = {
    # Quick baseline in constrained hosting.
    "Fast": {
        "high_volume": False,
        "max_sample_points": 20,
        "scroll_rounds": 6,
        "scroll_wait_ms": 500,
        "google_places_enrich": True,
    },
    # Better coverage with moderate runtime.
    "Balanced": {
        "high_volume": False,
        "max_sample_points": 20,
        "scroll_rounds": 6,
        "scroll_wait_ms": 500,
        "google_places_enrich": True,
    },
    # Highest completeness; slower and more timeout-prone.
    "Complete": {
        "high_volume": True,
        "max_sample_points": 20,
        "scroll_rounds": 6,
        "scroll_wait_ms": 500,
        "google_places_enrich": True,
    },
}
_DEFAULT_SCRAPE_PROFILE = "Complete"
_BUILD_STAMP = os.getenv("APP_BUILD_STAMP", "2026-04-23-executive-mode-80dbf21")


def inject_ui_theme() -> None:
    """Apply a cleaner, executive-style UI theme for Streamlit controls and sections."""
    st.markdown(
        """
<style>
:root {
  --bg: #f6f8fc;
  --card: #ffffff;
  --text: #0f172a;
  --muted: #475569;
  --line: #e2e8f0;
  --brand: #2563eb;
}
.stApp {
  background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 280px, var(--bg) 100%);
}
.block-container {
  max-width: 1380px;
  padding-top: 1.2rem;
  padding-bottom: 2rem;
}
h1, h2, h3 {
  letter-spacing: -0.02em;
}
[data-testid="stSidebar"] {
  border-right: 1px solid var(--line);
}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
  padding-top: 0.4rem;
}
div[data-testid="stMetric"] {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 0.65rem 0.85rem 0.55rem 0.85rem;
  box-shadow: 0 1px 2px rgba(2, 6, 23, 0.04);
}
div[data-testid="stAlert"] {
  border-radius: 12px;
}
div[data-testid="stDataFrame"] {
  border: 1px solid var(--line);
  border-radius: 12px;
  overflow: hidden;
  background: #fff;
}
button[kind="primary"] {
  border-radius: 999px !important;
  font-weight: 600 !important;
}
button[kind="secondary"] {
  border-radius: 999px !important;
}
.exec-pill {
  display: inline-block;
  padding: 0.22rem 0.65rem;
  border-radius: 999px;
  background: #dbeafe;
  color: #1d4ed8;
  font-size: 0.78rem;
  font-weight: 600;
  margin-left: 0.4rem;
}
.platform-pill {
  display: inline-block;
  padding: 0.2rem 0.58rem;
  border-radius: 999px;
  font-size: 0.74rem;
  font-weight: 700;
  margin-left: 0.38rem;
  border: 1px solid transparent;
}
.platform-pill.active {
  background: #fee2e2;
  color: #991b1b;
  border-color: #fecaca;
}
.platform-pill.soon {
  background: #f1f5f9;
  color: #64748b;
  border-color: #e2e8f0;
}
.step-card {
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  background: #ffffff;
  padding: 0.7rem 0.85rem;
}
.step-card.done {
  border-color: #86efac;
  background: #f0fdf4;
}
.step-card.active {
  border-color: #93c5fd;
  background: #eff6ff;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def init_state() -> None:
    # Hard-reset deprecated widget-bound keys that caused StreamlitAPIException in older builds.
    st.session_state.pop("run_pin_lat__custom_pin_mode", None)
    st.session_state.pop("run_pin_lng__custom_pin_mode", None)
    ensure_scrape_location(
        default_lat=float(DEFAULT_PIN[0]),
        default_lng=float(DEFAULT_PIN[1]),
        default_label="Dubai (default)",
        migrate_from_legacy_keys=True,
    )
    sync_legacy_pin_mirror()
    st.session_state.setdefault("results_df", pd.DataFrame())
    st.session_state.setdefault("last_successful_results_df", pd.DataFrame())
    st.session_state.setdefault("last_run_done", False)
    st.session_state.setdefault("last_run_returned_zero", False)
    st.session_state.setdefault("results_fingerprint", None)
    st.session_state.setdefault("last_scrape_run_meta", {})
    st.session_state.setdefault("_last_successful_run_effective_pin", None)
    st.session_state.setdefault("last_geocode_provider", None)
    st.session_state.setdefault("last_geocode_label", None)
    st.session_state.setdefault("supply_overlay_df", None)
    st.session_state.setdefault("google_coverage_df", pd.DataFrame())
    loc0 = get_scrape_location()
    lat0 = float(loc0["lat"])
    lng0 = float(loc0["lng"])
    # Second pin defaults a few km away so A/B are not identical on first open.
    st.session_state.setdefault("dual_area_a_lat", lat0)
    st.session_state.setdefault("dual_area_a_lng", lng0)
    st.session_state.setdefault("dual_area_b_lat", lat0 + 0.018)
    st.session_state.setdefault("dual_area_b_lng", lng0)
    st.session_state.setdefault("dual_area_a_label", "")
    st.session_state.setdefault("dual_area_b_label", "")
    st.session_state.setdefault("dual_map_next_slot", "A")
    st.session_state.setdefault("dual_last_click_sig", "")
    st.session_state.setdefault("runpin_last_click_sig", "")


def _bounds_for_radius(lat: float, lng: float, radius_km: float, pad: float = 1.15) -> tuple[list[float], list[float]]:
    """South-west and north-east corners so the map frames pin + search radius."""
    r = max(radius_km, 0.5) * pad
    d_lat = r / 110.574
    cos_lat = max(0.25, math.cos(math.radians(lat)))
    d_lng = r / (111.32 * cos_lat)
    return [lat - d_lat, lng - d_lng], [lat + d_lat, lng + d_lng]


def _add_supply_overlay_feature_group(fmap: folium.Map, supply_df: pd.DataFrame | None) -> None:
    supply = normalize_supply_overlay_df(supply_df)
    if supply is None or supply.empty:
        return
    supply_fg = folium.FeatureGroup(name="Supply / Kitchen Park")
    for _, s in supply.iterrows():
        la, ln = float(s["lat"]), float(s["lng"])
        lab = str(s.get("label") or "")[:120]
        folium.CircleMarker(
            location=[la, ln],
            radius=7,
            color="#EA580C",
            weight=2,
            fill=True,
            fill_color="#F97316",
            fill_opacity=0.85,
            tooltip=lab or "Supply",
            popup=folium.Popup(html.escape(lab or f"{la:.5f},{ln:.5f}"), max_width=220),
        ).add_to(supply_fg)
    supply_fg.add_to(fmap)


def _add_google_coverage_feature_group(fmap: folium.Map, coverage_df: pd.DataFrame | None) -> None:
    if coverage_df is None or not isinstance(coverage_df, pd.DataFrame) or coverage_df.empty:
        return
    if "lat" not in coverage_df.columns or "lng" not in coverage_df.columns:
        return
    fg = folium.FeatureGroup(name="Google-only coverage")
    for _, row in coverage_df.iterrows():
        try:
            la = float(row["lat"])
            ln = float(row["lng"])
        except (TypeError, ValueError):
            continue
        nm = html.escape(str(row.get("name") or "Google place")[:120])
        rating = row.get("rating")
        rt = f" · ⭐ {rating}" if rating not in (None, "") else ""
        popup = folium.Popup(f"<b>{nm}</b>{rt}", max_width=240)
        folium.CircleMarker(
            location=[la, ln],
            radius=6,
            color="#15803D",
            weight=2,
            fill=True,
            fill_color="#22C55E",
            fill_opacity=0.85,
            tooltip=f"{str(row.get('name') or 'Google place')[:80]}{rt}",
            popup=popup,
        ).add_to(fg)
    fg.add_to(fmap)


def _get_google_maps_api_key_for_basemap() -> str:
    try:
        secret = str(st.secrets.get("GOOGLE_MAPS_API_KEY", "")).strip()
        if secret:
            return secret
    except Exception:
        pass
    return (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()


def _configure_map_basemaps(fmap: folium.Map) -> str:
    """
    Add basemap tile layers: **Google** (Map Tiles API) when ``GOOGLE_MAPS_API_KEY`` is set
    and ``STREAMLIT_MAP_BASEMAP`` is not ``esri``; otherwise **Esri** (no Google key required).

    Returns ``\"google\"`` or ``\"esri\"`` for UI captions.
    """
    prefer = os.getenv("STREAMLIT_MAP_BASEMAP", "").strip().lower()
    key = _get_google_maps_api_key_for_basemap()
    if prefer != "esri" and key:
        cache = st.session_state.setdefault("_google_map_tile_session_cache", {})
        lang = os.getenv("STREAMLIT_GOOGLE_MAP_TILE_LANGUAGE", "en-US").strip() or "en-US"
        reg = os.getenv("STREAMLIT_GOOGLE_MAP_TILE_REGION", "AE").strip() or "AE"
        rm_sess, sh_sess = ensure_google_map_tile_sessions(key, cache, language=lang, region=reg)
        if rm_sess and sh_sess:
            g_attr = google_maps_tile_attribution()
            folium.TileLayer(
                tiles=google_2d_tile_url_template(key, rm_sess),
                attr=g_attr,
                name="Google roadmap",
                max_zoom=22,
            ).add_to(fmap)
            folium.TileLayer(
                tiles=google_2d_tile_url_template(key, sh_sess),
                attr=g_attr,
                name="Google satellite (labels)",
                max_zoom=22,
            ).add_to(fmap)
            return "google"
    _add_esri_basemaps(fmap)
    return "esri"


def _add_esri_basemaps(fmap: folium.Map) -> None:
    """Esri street + satellite; satellite is imagery-only until the reference overlay is on."""
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        attr=(
            'Tiles © <a href="https://www.esri.com/">Esri</a> '
            "(HERE, Garmin, OpenStreetMap contributors, GIS user community)"
        ),
        name="Street map (English labels)",
        max_zoom=19,
    ).add_to(fmap)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr='Tiles © <a href="https://www.esri.com/">Esri</a> (Earthstar Geographics, USDA, USGS, AeroGRID, IGN)',
        name="Satellite (photos only — no text on this layer)",
        max_zoom=19,
    ).add_to(fmap)
    # Raster imagery has no labels; this Esri reference layer adds place / boundary names (English-biased).
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
        attr='Labels © <a href="https://www.esri.com/">Esri</a>',
        name="Place names overlay (English · use with satellite)",
        overlay=True,
        control=True,
        opacity=0.92,
        show=True,
        max_zoom=19,
    ).add_to(fmap)


def render_pin_map(
    radius_km: float,
    *,
    lock_pin: bool = False,
    supply_df: pd.DataFrame | None = None,
    google_coverage_df: pd.DataFrame | None = None,
    dual_points: list[dict[str, float | str]] | None = None,
) -> dict:
    """Render Folium pin + radius; map clicks update ``scrape_location`` only (single source of truth)."""
    loc = get_scrape_location()
    lat = float(loc["lat"])
    lng = float(loc["lng"])
    label = str(loc.get("label") or "Search pin")

    fmap = folium.Map(
        location=[lat, lng],
        tiles=None,
        zoom_start=12,
        zoom_control=True,
        control_scale=True,
    )

    basemap = _configure_map_basemaps(fmap)

    area = folium.FeatureGroup(name="Search area").add_to(fmap)

    folium.Circle(
        location=[lat, lng],
        radius=radius_km * 1000.0,
        color="#1D4ED8",
        weight=3,
        fill=True,
        fill_color="#2563EB",
        fill_opacity=0.14,
        tooltip=f"Scrape radius: {radius_km:g} km",
    ).add_to(area)

    folium.Circle(
        location=[lat, lng],
        radius=min(180.0, max(40.0, radius_km * 35.0)),
        color="#1E40AF",
        weight=2,
        fill=True,
        fill_color="#1D4ED8",
        fill_opacity=0.35,
        tooltip="Pin precision",
    ).add_to(area)

    safe_label = html.escape(label)
    popup_html = (
        f"<div style='min-width:200px;font-size:13px'>"
        f"<b style='color:#1e3a8a'>{safe_label}</b><br>"
        f"<span style='color:#444'>{lat:.6f}, {lng:.6f}</span><br>"
        f"<span style='color:#64748b'>Radius: <b>{radius_km:g} km</b></span>"
        f"</div>"
    )
    folium.Marker(
        location=[lat, lng],
        tooltip=f"Pin · {radius_km:g} km search",
        popup=folium.Popup(popup_html, max_width=280),
        icon=folium.Icon(color="blue"),
    ).add_to(area)

    if dual_points:
        dual_fg = folium.FeatureGroup(name="Dual-area pins")
        color_map = {"A": "#DC2626", "B": "#7C3AED"}
        for p in dual_points:
            try:
                dla = float(p["lat"])
                dln = float(p["lng"])
            except (TypeError, ValueError, KeyError):
                continue
            slot = str(p.get("slot") or "").upper().strip()[:1] or "?"
            fill = color_map.get(slot, "#334155")
            label_txt = html.escape(str(p.get("label") or f"Area {slot}")[:120])
            folium.CircleMarker(
                location=[dla, dln],
                radius=7,
                color=fill,
                weight=2,
                fill=True,
                fill_color=fill,
                fill_opacity=0.9,
                tooltip=f"Area {slot}: {dla:.5f}, {dln:.5f}",
                popup=folium.Popup(f"<b>Area {slot}</b><br>{label_txt}<br>{dla:.6f}, {dln:.6f}", max_width=260),
            ).add_to(dual_fg)
        dual_fg.add_to(fmap)

    _add_supply_overlay_feature_group(fmap, supply_df)
    _add_google_coverage_feature_group(fmap, google_coverage_df)

    Fullscreen(position="topright", title="Fullscreen", title_cancel="Exit Full Screen").add_to(fmap)
    # num_digits only — lat_formatter/lng_formatter expect JS functions and can blank the map if misused.
    MousePosition(
        position="bottomleft",
        separator=" · ",
        prefix="Cursor: ",
        num_digits=5,
    ).add_to(fmap)
    folium.LayerControl(position="topright", collapsed=False).add_to(fmap)

    sw, ne = _bounds_for_radius(lat, lng, radius_km)
    fmap.fit_bounds([sw, ne], padding=(24, 24), max_zoom=16)

    if basemap == "google":
        st.caption(
            "**Google** basemaps (Map Tiles API). Use layer control (top-right) to switch roadmap / satellite. "
            "Click map to move pin."
        )
    else:
        st.caption(
            "**Satellite** is photos only (no street text). For English road names use **Street map**, "
            "or keep **Place names overlay** on. Layer control: top-right. Click map to move pin."
        )
    out = st_folium(
        fmap,
        width=1400,
        height=520,
        width="stretch",
        returned_objects=["last_clicked", "center"],
        key="talabat_pin_map",
    )
    out = dict(out or {})
    return out


def render_heatmap(
    df: pd.DataFrame,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    *,
    supply_df: pd.DataFrame | None = None,
    google_coverage_df: pd.DataFrame | None = None,
) -> None:
    st.subheader("Restaurant Density Heatmap")
    view_df = df.dropna(subset=["lat", "lng"]).copy()
    if view_df.empty:
        st.info("No coordinates available for heatmap.")
        return

    fmap = folium.Map(
        location=[float(pin_lat), float(pin_lng)],
        tiles=None,
        zoom_start=12,
        zoom_control=True,
        control_scale=True,
    )
    basemap = _configure_map_basemaps(fmap)

    if "distance_km_from_pin" in view_df.columns:
        dist = pd.to_numeric(view_df["distance_km_from_pin"], errors="coerce")
        view_df = view_df.loc[dist.notna() & (dist <= float(radius_km) + 0.4)].copy()

    heat_rows: list[list[float]] = []
    for _, row in view_df.iterrows():
        try:
            la = float(row["lat"])
            ln = float(row["lng"])
        except (TypeError, ValueError):
            continue
        heat_rows.append([la, ln])
    if heat_rows:
        heat_fg = folium.FeatureGroup(name="Platform listing density", overlay=True, control=True)
        HeatMap(
            heat_rows,
            min_opacity=0.33,
            max_zoom=17,
            radius=18,
            blur=12,
            gradient={0.4: "#2563EB", 0.6: "#7C3AED", 0.8: "#F59E0B", 0.96: "#EF4444"},
        ).add_to(heat_fg)
        heat_fg.add_to(fmap)

    folium.Circle(
        location=[float(pin_lat), float(pin_lng)],
        radius=float(radius_km) * 1000.0,
        color="#1D4ED8",
        weight=2,
        fill=True,
        fill_color="#2563EB",
        fill_opacity=0.08,
        tooltip=f"Scrape radius: {radius_km:g} km",
    ).add_to(fmap)
    folium.Marker(
        location=[float(pin_lat), float(pin_lng)],
        tooltip="Search pin",
        icon=folium.Icon(color="blue"),
    ).add_to(fmap)

    _add_supply_overlay_feature_group(fmap, supply_df)
    _add_google_coverage_feature_group(fmap, google_coverage_df)

    Fullscreen(position="topright", title="Fullscreen", title_cancel="Exit Full Screen").add_to(fmap)
    folium.LayerControl(position="topright", collapsed=False).add_to(fmap)

    lats = [float(pin_lat)] + [r[0] for r in heat_rows]
    lngs = [float(pin_lng)] + [r[1] for r in heat_rows]
    supply_bounds = normalize_supply_overlay_df(supply_df)
    if supply_bounds is not None and not supply_bounds.empty:
        lats.extend(supply_bounds["lat"].astype(float).tolist())
        lngs.extend(supply_bounds["lng"].astype(float).tolist())
    if google_coverage_df is not None and isinstance(google_coverage_df, pd.DataFrame) and not google_coverage_df.empty:
        if "lat" in google_coverage_df.columns and "lng" in google_coverage_df.columns:
            lats.extend(pd.to_numeric(google_coverage_df["lat"], errors="coerce").dropna().astype(float).tolist())
            lngs.extend(pd.to_numeric(google_coverage_df["lng"], errors="coerce").dropna().astype(float).tolist())
    pad_lat = max(0.002, (max(lats) - min(lats)) * 0.08 + 0.001)
    pad_lng = max(0.002, (max(lngs) - min(lngs)) * 0.08 + 0.001)
    sw = [min(lats) - pad_lat, min(lngs) - pad_lng]
    ne = [max(lats) + pad_lat, max(lngs) + pad_lng]
    fmap.fit_bounds([sw, ne], padding=(28, 28), max_zoom=16)

    if basemap == "google":
        st.caption(
            "Same **Google** basemaps as the pin map. Heat = **Platform listing density** from this scrape "
            "(single aggregator; multi-aggregator overlay is not wired yet)."
        )
    else:
        st.caption(
            "Same English-first basemaps as the pin map. **Street** = full English-style road labels; "
            "**Satellite** + **Place names overlay** for labels. Heat = **Platform listing density** from this scrape "
            "(single aggregator; multi-aggregator overlay is not wired yet)."
        )
    st_folium(
        fmap,
        width="stretch",
        height=480,
        key="talabat_heatmap_map",
    )


def get_frontend_api_key() -> str:
    try:
        secret = str(st.secrets.get("SCRAPER_API_KEY", "")).strip()
        if secret:
            return secret
    except Exception:
        pass
    return os.getenv("SCRAPER_API_KEY", "").strip()


def render_outbound_prioritization_dashboard(df: pd.DataFrame) -> None:
    """Brand-level outbound view: cuisine, order/store proxies, weighted priority score."""
    st.subheader("Outbound prioritization (brand view)")
    st.caption(
        "KitchenPark-style lens: rank brands for acquisition outreach using cuisine fit, ratings, reviews, "
        "delivery fee, and footprint. Talabat does not expose true **last-7-days** orders in this scrape — "
        "the order column is a **platform proxy** when available."
    )
    brand_raw = build_brand_prioritization_table(df)
    if brand_raw is None or brand_raw.empty:
        st.info("Not enough data to build a brand-level view.")
        return

    with st.expander("Prioritization model (weights & definitions)", expanded=False):
        st.markdown(MODEL_HELP)
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            w_r = st.slider("Rating", 0.0, 1.0, 0.28, 0.01, help="Talabat / Google rating blend per brand")
        with c2:
            w_rev = st.slider("Reviews", 0.0, 1.0, 0.22, 0.01, help="Sum of review counts across sampled branches")
        with c3:
            w_ord = st.slider("Order proxy", 0.0, 1.0, 0.22, 0.01, help="Talabat estimated_orders when present")
        with c4:
            w_del = st.slider("Delivery fee", 0.0, 1.0, 0.14, 0.01, help="Lower median fee → higher score")
        with c5:
            w_scale = st.slider("Store footprint", 0.0, 1.0, 0.14, 0.01, help="More stores in sample → higher reach")

    scored = add_priority_scores(
        brand_raw,
        w_rating=w_r,
        w_reviews=w_rev,
        w_orders=w_ord,
        w_delivery=w_del,
        w_scale=w_scale,
    )
    view = format_for_dashboard(scored)
    st.dataframe(view, width="stretch", height=380)

    top_n = view.head(18).copy()
    if not top_n.empty and "Outbound_priority" in top_n.columns:
        chart_df = top_n.set_index("Brand")["Outbound_priority"].sort_values(ascending=True)
        st.caption("Top brands by composite **Outbound_priority** (this scrape only).")
        st.bar_chart(chart_df)

    st.download_button(
        "Download brand prioritization CSV",
        data=view.to_csv(index=False).encode("utf-8"),
        file_name="talabat_outbound_brand_priorities.csv",
        mime="text/csv",
        key="dl_brand_priority",
    )


def _pick_numeric_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for col in candidates:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(dtype=float)


def _pick_rating_series(df: pd.DataFrame) -> pd.Series:
    return _pick_numeric_series(df, ["rating_effective", "rating", "google_rating"])


def _pick_area_group_series(df: pd.DataFrame) -> pd.Series:
    for col in ("batch_location_label", "scrape_target_label", "dual_area"):
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            s = s.mask(s == "", "Unlabeled area")
            return s
    return pd.Series(["Current run"] * len(df), index=df.index)


def render_executive_mode(df: pd.DataFrame, meta: dict) -> None:
    """Leadership-ready summary: KPI strip, run confidence, A/B delta, and area opportunity ranking."""
    st.subheader("Executive Mode")
    st.caption("GM view: headline KPIs, run confidence, dual-area delta, and top opportunity areas.")

    rating_s = _pick_rating_series(df)
    orders_day_s = _pick_numeric_series(df, ["estimated_orders_per_day", "estimated_orders"])
    orders_week_s = _pick_numeric_series(df, ["estimated_orders_per_week"])
    brand_key_s = (
        df["brand_id"].astype(str).str.strip()
        if "brand_id" in df.columns
        else df.get("restaurant_name", pd.Series([""] * len(df))).astype(str).str.strip().str.lower()
    )
    unique_brands = int((brand_key_s.replace("", pd.NA).dropna()).nunique()) if len(brand_key_s) else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Rows", f"{len(df):,}")
    k2.metric("Unique brands", f"{unique_brands:,}")
    avg_rating = float(rating_s.dropna().mean()) if not rating_s.dropna().empty else 0.0
    k3.metric("Avg rating", f"{avg_rating:.2f}" if avg_rating > 0 else "—")
    weekly_proxy = float(orders_week_s.dropna().sum()) if not orders_week_s.dropna().empty else 0.0
    if weekly_proxy <= 0:
        weekly_proxy = float(orders_day_s.dropna().sum() * 7.0) if not orders_day_s.dropna().empty else 0.0
    k4.metric("Weekly orders proxy", f"{int(round(weekly_proxy)):,}" if weekly_proxy > 0 else "—")

    legal_cov = 0.0
    if "legal_name" in df.columns and len(df):
        legal_cov = float((df["legal_name"].astype(str).str.strip() != "").mean())
    phone_cov = 0.0
    if "contact_phone" in df.columns and len(df):
        phone_cov = float((df["contact_phone"].astype(str).str.strip() != "").mean())
    elif "phone" in df.columns and len(df):
        phone_cov = float((df["phone"].astype(str).str.strip() != "").mean())
    rating_cov = float(rating_s.notna().mean()) if len(df) else 0.0
    geo_cov = 0.0
    if {"lat", "lng"}.issubset(df.columns) and len(df):
        lat_ok = pd.to_numeric(df["lat"], errors="coerce").notna()
        lng_ok = pd.to_numeric(df["lng"], errors="coerce").notna()
        geo_cov = float((lat_ok & lng_ok).mean())
    scale_score = min(float(len(df)) / 250.0, 1.0)
    meta_score = 1.0 if meta.get("request_id") else 0.0
    confidence = int(round(100.0 * (0.20 * legal_cov + 0.15 * phone_cov + 0.15 * rating_cov + 0.15 * geo_cov + 0.25 * scale_score + 0.10 * meta_score)))
    conf_txt = "High" if confidence >= 75 else ("Medium" if confidence >= 50 else "Low")
    st.info(
        f"Run confidence: **{confidence}/100 ({conf_txt})** · "
        f"legal `{legal_cov*100:.0f}%` · phone `{phone_cov*100:.0f}%` · rating `{rating_cov*100:.0f}%` · geo `{geo_cov*100:.0f}%`."
    )

    if "dual_area" in df.columns and {"A", "B"}.issubset(set(df["dual_area"].astype(str).str.upper().str.strip())):
        comp = df.copy()
        comp["dual_area"] = comp["dual_area"].astype(str).str.upper().str.strip()
        ab = comp[comp["dual_area"].isin(["A", "B"])].copy()
        ab["__rating"] = _pick_rating_series(ab)
        ab["__orders_day"] = _pick_numeric_series(ab, ["estimated_orders_per_day", "estimated_orders"])
        agg_dict = {
            "rows": ("dual_area", "size"),
            "avg_rating": ("__rating", "mean"),
            "orders_day": ("__orders_day", "sum"),
        }
        if "brand_id" in ab.columns:
            agg_dict["brands"] = ("brand_id", lambda s: s.astype(str).str.strip().replace("", pd.NA).dropna().nunique())
        else:
            agg_dict["brands"] = ("restaurant_name", "nunique")
        agg = ab.groupby("dual_area", dropna=False).agg(**agg_dict)
        if {"A", "B"}.issubset(set(agg.index)):
            a = agg.loc["A"]
            b = agg.loc["B"]
            st.markdown("**Area A vs B (executive delta)**")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Rows (A)", f"{int(a['rows']):,}", delta=f"{int(a['rows'] - b['rows']):+d} vs B")
            d2.metric("Brands (A)", f"{int(a['brands']):,}", delta=f"{int(a['brands'] - b['brands']):+d} vs B")
            if pd.notna(a["avg_rating"]) and pd.notna(b["avg_rating"]):
                d3.metric("Avg rating (A)", f"{float(a['avg_rating']):.2f}", delta=f"{float(a['avg_rating'] - b['avg_rating']):+.2f} vs B")
            else:
                d3.metric("Avg rating (A)", "—")
            if pd.notna(a["orders_day"]) and pd.notna(b["orders_day"]):
                d4.metric("Orders/day proxy (A)", f"{int(round(float(a['orders_day']))):,}", delta=f"{int(round(float(a['orders_day'] - b['orders_day']))):+d} vs B")
            else:
                d4.metric("Orders/day proxy (A)", "—")

    area_s = _pick_area_group_series(df)
    work = df.copy()
    work["__area"] = area_s
    work["__rating"] = _pick_rating_series(work).fillna(0.0)
    work["__orders"] = _pick_numeric_series(work, ["estimated_orders_per_day", "estimated_orders"]).fillna(0.0)
    if "restaurant_name" not in work.columns:
        work["restaurant_name"] = ""
    grp = work.groupby("__area", dropna=False).agg(
        rows=("__area", "size"),
        avg_rating=("__rating", "mean"),
        orders_proxy=("__orders", "sum"),
        restaurants=("restaurant_name", "nunique"),
    ).reset_index()
    if len(grp) > 0:
        def _norm(s: pd.Series) -> pd.Series:
            s = pd.to_numeric(s, errors="coerce").fillna(0.0)
            lo, hi = float(s.min()), float(s.max())
            if hi <= lo:
                return pd.Series([0.5] * len(s), index=s.index)
            return (s - lo) / (hi - lo)

        grp["score_orders"] = _norm(grp["orders_proxy"])
        grp["score_scale"] = _norm(grp["restaurants"])
        grp["score_rating_gap"] = _norm(4.3 - grp["avg_rating"])
        grp["opportunity_score"] = (0.45 * grp["score_orders"] + 0.30 * grp["score_scale"] + 0.25 * grp["score_rating_gap"]) * 100.0
        grp = grp.sort_values(["opportunity_score", "orders_proxy"], ascending=[False, False]).reset_index(drop=True)
        top = grp.head(10).copy()
        top["opportunity_score"] = top["opportunity_score"].round(1)
        top["avg_rating"] = top["avg_rating"].round(2)
        st.markdown("**Top opportunity areas**")
        st.dataframe(
            top.rename(columns={"__area": "Area"})[["Area", "opportunity_score", "orders_proxy", "restaurants", "avg_rating", "rows"]],
            width="stretch",
            height=260,
        )
        if not top.empty:
            lead = top.iloc[0]
            st.success(
                f"Primary recommendation: prioritize **{lead['__area']}** first "
                f"(score {float(lead['opportunity_score']):.1f}, orders proxy {int(round(float(lead['orders_proxy']))):,})."
            )


def get_api_base_url() -> str:
    try:
        secret_url = str(st.secrets.get("API_BASE_URL", "")).strip()
        if secret_url:
            return secret_url.rstrip("/")
    except Exception:
        pass
    return os.getenv("API_BASE_URL", "https://maisam21-lab-talabat-area-intel.onrender.com").strip().rstrip("/")


def _friendly_api_error(response: requests.Response) -> str:
    rid = (response.headers.get("X-Request-ID") or "").strip()
    code = int(response.status_code)
    ctype = (response.headers.get("Content-Type") or "").lower()
    if "application/json" in ctype:
        try:
            data = response.json()
        except ValueError:
            data = {}
        detail = str(data.get("error") or data.get("detail") or response.reason or "Request failed")
        rid = str(data.get("request_id") or rid).strip()
    else:
        detail = "Upstream gateway returned a non-JSON error page before the API could send structured JSON."
    hint = {
        502: "Try a smaller radius or check API logs for this request id.",
        504: "Scrape timed out. Raise server timeout limits or check API logs.",
        500: "Internal scrape failure. Check backend logs with request id.",
    }.get(code, "Check API logs with request id.")
    rid_txt = f" Request ID: {rid}." if rid else ""
    return f"{code} {response.reason}: {detail}. {hint}{rid_txt}"


def compact_output_df(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Remove all-empty columns from user-facing outputs.

    Keeps only core geometry/identity columns by default, then appends any
    additional columns that have at least one non-empty value.
    """
    if df is None or df.empty:
        return pd.DataFrame(), []
    core = [
        "restaurant_name",
        "brand_display_name",
        "restaurant_url",
        "legal_name",
        "contact_phone",
        "just_landed",
        "just_landed_date",
        "rating",
        "google_rating",
        "rating_effective",
        "estimated_orders",
        "estimated_orders_per_day",
        "estimated_orders_per_week",
        "lat",
        "lng",
        "distance_km_from_pin",
    ]
    exclude_always = {"status", "area_label"}
    keep: list[str] = [c for c in core if c in df.columns]
    removed: list[str] = []

    for c in df.columns:
        if c in keep:
            continue
        if c in exclude_always:
            removed.append(c)
            continue
        s = df[c]
        non_empty = (s.notna()) & (s.astype(str).str.strip() != "")
        if bool(non_empty.any()):
            keep.append(c)
        else:
            removed.append(c)
    return df.loc[:, keep].copy(), removed


def ensure_just_landed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep Just Landed signal visible in UI/exports with stable defaults."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if "just_landed" not in out.columns:
        out["just_landed"] = "false"
    jl = out["just_landed"].astype(str).str.strip().str.lower()
    out["just_landed"] = jl.map({"yes": "true", "true": "true"}).fillna("false")
    if "just_landed_date" not in out.columns:
        out["just_landed_date"] = ""
    out["just_landed_date"] = out["just_landed_date"].fillna("").astype(str).str.strip()
    return out


def build_excel_export_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "platform" not in out.columns:
        out["platform"] = "Talabat"
    if "orders_week_estimate" not in out.columns:
        if "estimated_orders_per_week" in out.columns:
            out["orders_week_estimate"] = out["estimated_orders_per_week"]
        elif "estimated_orders_per_day" in out.columns:
            out["orders_week_estimate"] = pd.to_numeric(out["estimated_orders_per_day"], errors="coerce") * 7.0
        else:
            out["orders_week_estimate"] = ""
    if "area_label" not in out.columns:
        if "scrape_target_label" in out.columns:
            out["area_label"] = out["scrape_target_label"]
        elif "batch_location_label" in out.columns:
            out["area_label"] = out["batch_location_label"]
        else:
            out["area_label"] = ""
    out["scrape_date"] = datetime.utcnow().strftime("%Y-%m-%d")
    export_cols = [
        "restaurant_name",
        "brand_display_name",
        "legal_name",
        "contact_phone",
        "cuisines",
        "orders_week_estimate",
        "platform",
        "status",
        "just_landed",
        "just_landed_date",
        "lat",
        "lng",
        "distance_km_from_pin",
        "area_label",
        "scrape_date",
    ]
    for c in export_cols:
        if c not in out.columns:
            out[c] = ""
    return out.loc[:, export_cols].rename(
        columns={
            "restaurant_name": "restaurant",
            "brand_display_name": "brand",
            "contact_phone": "phone",
            "cuisines": "cuisine",
            "distance_km_from_pin": "distance_from_pin_km",
        }
    )


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio) as writer:
        df.to_excel(writer, index=False, sheet_name="Area Intel")
    return bio.getvalue()


def main() -> None:
    st.set_page_config(page_title="Area Intel | Kitchen Park", layout="wide")
    inject_ui_theme()
    init_state()

    st.markdown(
        "## Area Intel "
        "<span class='platform-pill active'>Talabat</span>"
        "<span class='platform-pill soon'>Deliveroo · Coming soon</span>"
        "<span class='platform-pill soon'>Careem · Coming soon</span>",
        unsafe_allow_html=True,
    )
    st.caption("Kitchen Park · Market Intelligence")
    st.warning(f"Build: `{_BUILD_STAMP}`")

    with st.sidebar:
        st.header("Scrape Controls")
        api_base_url = get_api_base_url()
        api_key = get_frontend_api_key()
        headers = {"X-API-Key": api_key} if api_key else {}
        show_advanced = False
        area_mode = "custom"
        is_city_mode = False
        city_key = "dubai"

        st.markdown("**1) Location**")
        city_key = st.selectbox(
            "City preset",
            _CITY_SLUGS,
            format_func=lambda k: UAE_CITY_DISPLAY[k],
            index=0,
        )
        city_lat, city_lng, _ = UAE_CITY_PRESETS[city_key]
        if st.button("Use city preset pin", width="stretch"):
            seed_city_preset_if_changed(
                city_key,
                float(city_lat),
                float(city_lng),
                UAE_CITY_DISPLAY[city_key],
            )
            sync_legacy_pin_mirror()
            st.rerun()

        target_area_label = ""
        listing_status_mode = "live"
        new_on_platform_only = st.checkbox(
            "Just Landed only",
            value=False,
            help="When enabled, only vendors marked as newly launched are included.",
        )
        include_google_coverage = True
        selected_profile_name = "Complete"
        radius_pick = st.radio("2) Radius", options=[5, 10], horizontal=True, index=1)
        radius_km = float(radius_pick)
        target_area_label = st.text_input(
            "3) Area label (for exports)",
            value="",
            help="Stored as area_label in exports.",
        )

        geocode_query = st.text_input("Address search (UAE)", value="")
        geocode_btn = st.button("Set pin from search", width="stretch")

        if geocode_btn:
            try:
                g_response = requests.post(
                    f"{api_base_url.rstrip('/')}/geocode",
                    json={"query": geocode_query},
                    headers=headers,
                    timeout=30,
                )
                g_response.raise_for_status()
                payload = g_response.json()
                result = payload.get("result")
                if payload.get("ok") and result:
                    set_scrape_location(
                        float(result["lat"]),
                        float(result["lng"]),
                        str(result.get("formatted_address") or geocode_query).strip(),
                        "geocode",
                    )
                    sync_legacy_pin_mirror()
                    provider = payload.get("provider", "unknown")
                    st.session_state["last_geocode_provider"] = str(provider)
                    st.session_state["last_geocode_label"] = str(get_scrape_location().get("label") or "")
                    st.success(f"Pin set from search ({provider}): {get_scrape_location()['label']}")
                    if payload.get("note"):
                        st.info(str(payload["note"]))
                else:
                    hint = payload.get("hint")
                    if hint:
                        st.warning(hint)
                    else:
                        st.warning(f"No geocoding result. {payload.get('error', 'no details')}")
            except Exception as exc:
                st.error(f"Geocode failed via backend: {exc}")
        st.warning("Coastline tip: keep the pin slightly inland to avoid sparse ocean-side sampling.")
        run_single = st.button("Start Scraping", type="primary", width="stretch")

    _pin_widget_scope = str(city_key) if is_city_mode else "custom_pin_mode"
    st.subheader("Run pin (single source for scraping)")
    st.caption(
        "Lat/lng here, map clicks, and **Start Scraping** all use the same `scrape_location` session object. "
        "The API echoes pins and radius counts in **Resolved scrape parameters** after each run."
    )
    loc_ui = get_scrape_location()
    lat_internal_key = f"_run_pin_lat__{_pin_widget_scope}"
    lng_internal_key = f"_run_pin_lng__{_pin_widget_scope}"
    st.session_state.setdefault(lat_internal_key, float(loc_ui["lat"]))
    st.session_state.setdefault(lng_internal_key, float(loc_ui["lng"]))
    source_now = str(loc_ui.get("source") or "")
    if source_now in {"folium_click", "geocode", "city_preset", "init"}:
        st.session_state[lat_internal_key] = float(loc_ui["lat"])
        st.session_state[lng_internal_key] = float(loc_ui["lng"])
    rp1, rp2 = st.columns(2)
    with rp1:
        run_lat = st.number_input(
            "Run pin latitude",
            value=float(st.session_state[lat_internal_key]),
            format="%.6f",
            step=0.0001,
            key=f"run_pin_lat_input__{_pin_widget_scope}",
        )
    with rp2:
        run_lng = st.number_input(
            "Run pin longitude",
            value=float(st.session_state[lng_internal_key]),
            format="%.6f",
            step=0.0001,
            key=f"run_pin_lng_input__{_pin_widget_scope}",
        )
    st.session_state[lat_internal_key] = float(run_lat)
    st.session_state[lng_internal_key] = float(run_lng)
    set_scrape_location(
        float(run_lat),
        float(run_lng),
        str(loc_ui.get("label") or "Run pin"),
        "manual_form",
    )
    sync_legacy_pin_mirror()

    preview_two_pinned = False
    dual_points_for_map: list[dict[str, float | str]] | None = None
    if preview_two_pinned:
        dual_points_for_map = [
            {
                "slot": "A",
                "lat": float(st.session_state["dual_area_a_lat"]),
                "lng": float(st.session_state["dual_area_a_lng"]),
                "label": str(st.session_state.get("dual_area_a_label") or "Area A"),
            },
            {
                "slot": "B",
                "lat": float(st.session_state["dual_area_b_lat"]),
                "lng": float(st.session_state["dual_area_b_lng"]),
                "label": str(st.session_state.get("dual_area_b_label") or "Area B"),
            },
        ]

    pin_done = str(get_scrape_location().get("source") or "") not in ("init", "")
    step1_cls = "step-card done" if pin_done else "step-card active"
    step2_cls = "step-card active" if pin_done else "step-card"
    step3_cls = "step-card"
    cstep1, cstep2, cstep3 = st.columns(3)
    with cstep1:
        st.markdown(f"<div class='{step1_cls}'><b>Step 1</b><br>Drop a pin (search or click map)</div>", unsafe_allow_html=True)
    with cstep2:
        st.markdown(f"<div class='{step2_cls}'><b>Step 2</b><br>Set radius &amp; label</div>", unsafe_allow_html=True)
    with cstep3:
        st.markdown(f"<div class='{step3_cls}'><b>Step 3</b><br>Download Excel</div>", unsafe_allow_html=True)

    st.subheader("Interactive search map")
    folium_out = render_pin_map(
        radius_km,
        lock_pin=False,
        supply_df=st.session_state.get("supply_overlay_df"),
        google_coverage_df=st.session_state.get("google_coverage_df"),
        dual_points=dual_points_for_map,
    )
    store_folium_payload(folium_out)
    if folium_out.get("last_clicked"):
        lc = folium_out["last_clicked"]
        click_lat = float(lc["lat"])
        click_lng = float(lc["lng"])
        click_sig = f"{click_lat:.6f},{click_lng:.6f}"
        if preview_two_pinned:
            if click_sig != str(st.session_state.get("dual_last_click_sig") or ""):
                slot = str(st.session_state.get("dual_map_next_slot") or "A").upper()
                if slot not in ("A", "B"):
                    slot = "A"
                if slot == "A":
                    st.session_state["dual_area_a_lat"] = click_lat
                    st.session_state["dual_area_a_lng"] = click_lng
                    st.session_state["dual_map_next_slot"] = "B"
                else:
                    st.session_state["dual_area_b_lat"] = click_lat
                    st.session_state["dual_area_b_lng"] = click_lng
                    st.session_state["dual_map_next_slot"] = "A"
                st.session_state["dual_last_click_sig"] = click_sig
                # Keep the main run pin synced with the latest map click.
                set_scrape_location(click_lat, click_lng, "Custom pin (map)", "folium_click")
                sync_legacy_pin_mirror()
                st.toast(f"Area {slot} pin → {click_lat:.5f}, {click_lng:.5f}", icon="📌")
                st.rerun()
        else:
            if click_sig != str(st.session_state.get("runpin_last_click_sig") or ""):
                st.session_state["runpin_last_click_sig"] = click_sig
                set_scrape_location(click_lat, click_lng, "Custom pin (map)", "folium_click")
                sync_legacy_pin_mirror()
                st.toast(f"Pin → {click_lat:.5f}, {click_lng:.5f}", icon="📍")
                st.rerun()
    loc_after_map = get_scrape_location()
    mismatch, mismatch_msg = folium_center_vs_location_mismatch(loc_after_map)
    if mismatch and mismatch_msg:
        st.warning(mismatch_msg)

    st.subheader("Run")
    loc_run = get_scrape_location()
    if is_city_mode:
        st.write(
            f"**City (label):** `{UAE_CITY_DISPLAY[city_key]}` · **Run pin sent to API:** "
            f"`{float(loc_run['lat']):.6f}, {float(loc_run['lng']):.6f}` "
            "(adjust numbers above or click the map)"
        )
    else:
        st.write(f"**Run pin:** `{float(loc_run['lat']):.6f}, {float(loc_run['lng']):.6f}`")
    st.write(
        f"Radius: `{radius_km} km` · Profile: `{selected_profile_name}` · Google Places when API key is set · "
        f"Status: `{listing_status_mode}` · New-only: `{new_on_platform_only}` · "
        f"Target label: `{target_area_label.strip() or '—'}`"
    )
    st.caption(
        "Expected runtime is usually a few minutes, but can be longer on heavy areas. "
        "Scrape wall-clock is controlled by the API service environment."
    )
    pinned_count = "one"
    use_two_pinned = False
    run_two_pins = False

    loc_fp = get_scrape_location()
    dual_sig = "none"
    dual_sig = "one_pin"

    current_fingerprint = "|".join(
        [
            area_mode,
            city_key if is_city_mode else "custom",
            pinned_count,
            listing_status_mode,
            str(new_on_platform_only),
            selected_profile_name,
            str(include_google_coverage),
            dual_sig,
            target_area_label.strip(),
            f"{float(loc_fp['lat']):.6f}",
            f"{float(loc_fp['lng']):.6f}",
            str(radius_km),
        ]
    )

    def _start_scrape_timer(timer_box):
        stop_event = threading.Event()
        start_ts = time.time()

        def _tick():
            while not stop_event.is_set():
                elapsed = int(time.time() - start_ts)
                mins = elapsed // 60
                secs = elapsed % 60
                try:
                    timer_box.markdown(f"⏱ Scraping · {mins:02d}:{secs:02d} elapsed")
                except Exception:
                    return
                time.sleep(1)

        th = threading.Thread(target=_tick, daemon=True)
        th.start()
        return stop_event, start_ts

    if run_two_pins:
        progress = st.progress(0.0)
        status_box = st.empty()
        timer_box = st.empty()
        stop_event, start_ts = _start_scrape_timer(timer_box)
        a_lab = str(st.session_state.get("dual_area_a_label") or "").strip() or "Area A"
        b_lab = str(st.session_state.get("dual_area_b_label") or "").strip() or "Area B"
        dual_area_df = pd.DataFrame(
            [
                {
                    "lat": float(st.session_state["dual_area_a_lat"]),
                    "lng": float(st.session_state["dual_area_a_lng"]),
                    "label": a_lab,
                    "area_slot": "A",
                },
                {
                    "lat": float(st.session_state["dual_area_b_lat"]),
                    "lng": float(st.session_state["dual_area_b_lng"]),
                    "label": b_lab,
                    "area_slot": "B",
                },
            ]
        )
        profile_cfg = _SCRAPE_PROFILES.get(selected_profile_name, _SCRAPE_PROFILES["Complete"])
        base_payload = {
            "radius_km": float(radius_km),
            "spacing_km": _DEFAULT_SPACING_KM,
            "concurrency": _DEFAULT_CONCURRENCY,
            "scroll_rounds": int(profile_cfg["scroll_rounds"]),
            "scroll_wait_ms": int(profile_cfg["scroll_wait_ms"]),
            "status_filter": "all",
            "just_landed_only": bool(new_on_platform_only),
            "max_sample_points": int(profile_cfg["max_sample_points"]),
            "dedupe_by_vendor_url": _SCRAPE_DEDUPE_BY_VENDOR_URL,
            "high_volume": bool(profile_cfg["high_volume"]),
            "google_places_enrich": bool(profile_cfg["google_places_enrich"]),
            "enrich": False,
            "scrape_target_label": None,
            "city": None,
        }
        with st.spinner("Dual-area scrape: running pin A, then pin B..."):
            dual_df, dual_errors = run_dual_area_scrape_via_api(
                api_base_url=api_base_url,
                headers=headers,
                locations_df=dual_area_df,
                base_payload=base_payload,
                timeout_sec=_SCRAPE_CLIENT_TIMEOUT_SEC,
            )
        dual_df = ensure_just_landed_columns(dual_df)
        stop_event.set()
        elapsed = int(time.time() - start_ts)
        timer_box.success(f"Done in {elapsed // 60:02d}:{elapsed % 60:02d}")
        progress.progress(1.0)
        previous_success_df = st.session_state.get("last_successful_results_df", pd.DataFrame())
        if dual_df is not None and not dual_df.empty:
            st.session_state["results_df"] = dual_df
            st.session_state["last_successful_results_df"] = dual_df
            st.session_state["last_run_returned_zero"] = False
        elif previous_success_df is not None and not previous_success_df.empty:
            st.session_state["results_df"] = previous_success_df
            st.session_state["last_run_returned_zero"] = True
        else:
            st.session_state["results_df"] = dual_df
            st.session_state["last_run_returned_zero"] = True
        st.session_state["google_coverage_df"] = pd.DataFrame()
        st.session_state["last_run_done"] = True
        st.session_state["results_fingerprint"] = current_fingerprint
        st.session_state["last_scrape_run_meta"] = {
            "dual_area_mode": True,
            "dual_area_locations": int(len(dual_area_df)),
            "dual_area_errors": int(len(dual_errors)),
        }
        if dual_errors:
            st.warning("Dual-area run finished with some errors: " + "; ".join(dual_errors[:5]))
        if dual_df.empty:
            st.warning("Dual-area run returned no rows.")
        else:
            status_box.info(f"Dual-area scrape completed · rows={len(dual_df):,} · pins={len(dual_area_df)}")

    if run_single:
        progress = st.progress(0.0)
        status_box = st.empty()
        timer_box = st.empty()
        stop_event, start_ts = _start_scrape_timer(timer_box)
        if not include_google_coverage:
            st.session_state["google_coverage_df"] = pd.DataFrame()

        with st.spinner("Cooking up some data magic... 🪄✨"):
            loc_req = get_scrape_location()
            try:
                parse_scrape_pin_or_raise_value_error(loc_req["lat"], loc_req["lng"])
            except ValueError as exc:
                st.error(f"Run pin is invalid — fix lat/lng before scraping. ({exc})")
                st.session_state["results_df"] = pd.DataFrame()
                st.session_state["last_run_done"] = False
            else:
                profile_cfg = _SCRAPE_PROFILES.get(selected_profile_name, _SCRAPE_PROFILES["Complete"])
                payload = {
                    "radius_km": float(radius_km),
                    "spacing_km": _DEFAULT_SPACING_KM,
                    "concurrency": _DEFAULT_CONCURRENCY,
                    "scroll_rounds": int(profile_cfg["scroll_rounds"]),
                    "scroll_wait_ms": int(profile_cfg["scroll_wait_ms"]),
                    "status_filter": listing_status_mode,
                    "just_landed_only": bool(new_on_platform_only),
                    "max_sample_points": int(profile_cfg["max_sample_points"]),
                    "dedupe_by_vendor_url": _SCRAPE_DEDUPE_BY_VENDOR_URL,
                    "high_volume": bool(profile_cfg["high_volume"]),
                    "google_places_enrich": bool(profile_cfg["google_places_enrich"]),
                    "enrich": False,
                    "scrape_target_label": target_area_label.strip() or None,
                    "pin_lat": float(loc_req["lat"]),
                    "pin_lng": float(loc_req["lng"]),
                    "client_asserted_pin_lat": float(loc_req["lat"]),
                    "client_asserted_pin_lng": float(loc_req["lng"]),
                }
                if is_city_mode:
                    payload["city"] = city_key
                else:
                    payload["city"] = None
                if not target_area_label.strip() and is_city_mode:
                    payload["scrape_target_label"] = UAE_CITY_DISPLAY.get(city_key, city_key)
                try:
                    fallback_notes: list[str] = []
                    request_id = uuid.uuid4().hex
                    req_headers = dict(headers)
                    req_headers["X-Request-ID"] = request_id
                    def _poll_result(request_id_to_poll: str, total_timeout_sec: float = _SCRAPE_CLIENT_TIMEOUT_SEC) -> dict:
                        deadline = time.time() + float(total_timeout_sec)
                        while time.time() < deadline:
                            poll_resp = requests.get(
                                f"{api_base_url.rstrip('/')}/result/{request_id_to_poll}",
                                headers=req_headers,
                                timeout=30,
                            )
                            if poll_resp.status_code >= 400:
                                raise RuntimeError(_friendly_api_error(poll_resp))
                            poll_data = poll_resp.json() if poll_resp.content else {}
                            poll_status = str((poll_data or {}).get("status") or "").lower()
                            if poll_status == "complete":
                                return poll_data
                            if poll_status == "failed":
                                raise RuntimeError(str((poll_data or {}).get("error") or "Remote scrape job failed"))
                            time.sleep(10)
                        raise RuntimeError("Remote scrape job timed out while waiting for completion.")

                    def _post_scrape(req_payload: dict, timeout_msg: str) -> tuple[int, dict] | None:
                        try:
                            enqueue_resp = requests.post(
                                f"{api_base_url.rstrip('/')}/scrape",
                                json=req_payload,
                                headers=req_headers,
                                timeout=min(float(_SCRAPE_CLIENT_TIMEOUT_SEC), 60.0),
                            )
                            if enqueue_resp.status_code >= 400:
                                return int(enqueue_resp.status_code), {}
                            enqueue_data = enqueue_resp.json() if enqueue_resp.content else {}
                            rid = str(
                                enqueue_data.get("request_id")
                                or enqueue_resp.headers.get("X-Request-ID")
                                or request_id
                            ).strip()
                            if not rid:
                                raise RuntimeError("Missing request_id from /scrape enqueue response")
                            result_data = _poll_result(rid)
                            return 200, result_data
                        except requests.exceptions.ReadTimeout:
                            status_box.warning(timeout_msg)
                            return None

                    scrape_result = _post_scrape(payload, "Primary scrape hit client read-timeout. Retrying with lighter settings...")
                    if scrape_result is None or int(scrape_result[0]) >= 400:
                        code = int(scrape_result[0]) if scrape_result is not None else 504
                        # Hosted gateway timeouts are common; retry with progressively lighter payloads.
                        if code in (502, 504):
                            fallback_payload = dict(payload)
                            fallback_payload["high_volume"] = False
                            fallback_payload["just_landed_only"] = False
                            fallback_payload["status_filter"] = "all"
                            fallback_payload["max_sample_points"] = min(int(payload["max_sample_points"]), 12)
                            fallback_payload["scroll_rounds"] = 10
                            fallback_notes.append("Fallback 1 used: disabled high-volume, reduced grid sample cap and scroll rounds.")
                            scrape_result = _post_scrape(
                                fallback_payload,
                                "Lighter retry also hit read-timeout. Retrying once with ultra-light settings...",
                            )
                            code = int(scrape_result[0]) if scrape_result is not None else 504
                            if code in (502, 504):
                                ultra_payload = dict(fallback_payload)
                                ultra_payload["just_landed_only"] = False
                                ultra_payload["status_filter"] = "all"
                                ultra_payload["max_sample_points"] = min(int(fallback_payload["max_sample_points"]), 4)
                                ultra_payload["scroll_rounds"] = 8
                                ultra_payload["spacing_km"] = 2.5
                                fallback_notes.append("Fallback 2 used: ultra-light profile with very low sample cap and wider spacing.")
                                scrape_result = _post_scrape(
                                    ultra_payload,
                                    "Ultra-light retry also hit read-timeout.",
                                )
                        if scrape_result is None:
                            raise RuntimeError(
                                "Client read-timeout after all retries. Backend is overloaded or blocked by host limits."
                            )
                        if int(scrape_result[0]) >= 400:
                            raise RuntimeError(f"HTTP {int(scrape_result[0])} from /scrape enqueue")
                    api_data = scrape_result[1] if scrape_result is not None else {}
                    api_request_id = str(api_data.get("request_id") or request_id)
                    df = pd.DataFrame(api_data.get("records", []))
                    df = ensure_just_landed_columns(df)
                    if df.empty:
                        status_box.warning("Scrape returned 0 rows. Trying emergency single-point fallback...")
                        emergency_payload = dict(payload)
                        emergency_payload["high_volume"] = False
                        emergency_payload["dedupe_by_vendor_url"] = True
                        emergency_payload["just_landed_only"] = False
                        emergency_payload["status_filter"] = "all"
                        emergency_payload["max_sample_points"] = 1
                        emergency_payload["scroll_rounds"] = 4
                        emergency_payload["scroll_wait_ms"] = min(int(payload["scroll_wait_ms"]), 700)
                        em_enqueue = requests.post(
                            f"{api_base_url.rstrip('/')}/scrape",
                            json=emergency_payload,
                            headers=req_headers,
                            timeout=min(float(_SCRAPE_CLIENT_TIMEOUT_SEC), 60.0),
                        )
                        if em_enqueue.status_code < 400:
                            em_enqueue_data = em_enqueue.json() if em_enqueue.content else {}
                            em_rid = str(
                                em_enqueue_data.get("request_id")
                                or em_enqueue.headers.get("X-Request-ID")
                                or api_request_id
                            ).strip()
                            em_data = _poll_result(em_rid)
                            em_df = pd.DataFrame(em_data.get("records", []))
                            em_df = ensure_just_landed_columns(em_df)
                            if not em_df.empty:
                                df = em_df
                                api_request_id = str(
                                    em_data.get("request_id") or api_request_id
                                )
                                api_data = em_data
                    gdf = pd.DataFrame()
                    st.session_state["last_scrape_city"] = api_data.get("city")
                    meta_run = api_data.get("scrape_run_meta") or {}
                    meta_run.setdefault("request_id", api_request_id)
                    st.session_state["last_scrape_run_meta"] = meta_run
                    elat = meta_run.get("effective_scrape_pin_lat")
                    elng = meta_run.get("effective_scrape_pin_lng")
                    if elat is not None and elng is not None:
                        st.session_state["_last_successful_run_effective_pin"] = (float(elat), float(elng))
                    if include_google_coverage:
                        try:
                            gc_resp = requests.post(
                                f"{api_base_url.rstrip('/')}/google-coverage",
                                json={
                                    "pin_lat": float(loc_req["lat"]),
                                    "pin_lng": float(loc_req["lng"]),
                                    "radius_km": float(radius_km),
                                },
                                headers=req_headers,
                                timeout=80,
                            )
                            if gc_resp.status_code < 400:
                                gc_data = gc_resp.json()
                                gdf = pd.DataFrame(gc_data.get("records", []))
                            else:
                                st.warning(f"Google coverage fetch skipped: {_friendly_api_error(gc_resp)}")
                        except Exception as exc:
                            st.warning(f"Google coverage fetch failed: {exc}")
                    st.session_state["google_coverage_df"] = gdf
                    progress.progress(1.0)
                    status_box.info(f"Remote scrape completed · request_id={api_request_id}")
                    if fallback_notes:
                        st.warning("Primary scrape degraded due to timeouts: " + " ".join(fallback_notes))
                except Exception as exc:
                    st.error(f"Remote API scrape failed: {exc}")
                    df = pd.DataFrame()
                    st.session_state["google_coverage_df"] = pd.DataFrame()

                previous_success_df = st.session_state.get("last_successful_results_df", pd.DataFrame())
                if df is not None and not df.empty:
                    st.session_state["results_df"] = df
                    st.session_state["last_successful_results_df"] = df
                    st.session_state["last_run_returned_zero"] = False
                elif previous_success_df is not None and not previous_success_df.empty:
                    st.session_state["results_df"] = previous_success_df
                    st.session_state["last_run_returned_zero"] = True
                else:
                    st.session_state["results_df"] = df
                    st.session_state["last_run_returned_zero"] = True
                st.session_state["last_run_done"] = True
                st.session_state["results_fingerprint"] = current_fingerprint
        stop_event.set()
        elapsed = int(time.time() - start_ts)
        timer_box.success(f"Done in {elapsed // 60:02d}:{elapsed % 60:02d}")

    df = st.session_state.get("results_df", pd.DataFrame())
    if st.session_state.get("last_run_returned_zero", False):
        st.warning(
            "Latest scrape returned 0 rows. Showing your last successful results so the table stays available."
        )
    if df is None or df.empty:
        gdf_fallback = st.session_state.get("google_coverage_df", pd.DataFrame())
        if isinstance(gdf_fallback, pd.DataFrame) and not gdf_fallback.empty:
            g = gdf_fallback.copy()
            df = pd.DataFrame(
                {
                    "restaurant_name": g.get("name", pd.Series([""] * len(g))),
                    "google_rating": g.get("rating", pd.Series([""] * len(g))),
                    "google_reviews_count": g.get("user_ratings_total", pd.Series([""] * len(g))),
                    "google_place_id": g.get("google_place_id", pd.Series([""] * len(g))),
                    "lat": g.get("lat", pd.Series([None] * len(g))),
                    "lng": g.get("lng", pd.Series([None] * len(g))),
                    "platform": pd.Series(["Google coverage"] * len(g)),
                    "status": pd.Series(["coverage_only"] * len(g)),
                }
            )
            st.info("Showing Google coverage rows while platform scrape returns zero restaurants.")
    if df is None or df.empty:
        if st.session_state.get("last_run_done"):
            st.warning(
                "**No restaurants extracted.** Try radius **10 km**, status **all** (temporarily), Just Landed **off**, then run again. "
                "If it still returns zero rows, open Render → API service → **Logs** for the failing `/scrape` call."
            )
        else:
            st.info("No results yet. Set pin and click Start Scraping.")
        return

    view_df, dropped_cols = compact_output_df(df)
    if "platform" not in view_df.columns:
        view_df["platform"] = "Talabat"
    if dropped_cols:
        st.caption(f"Hiding **{len(dropped_cols)}** all-empty columns from table/export (noise reduction).")

    if (
        st.session_state.get("results_fingerprint")
        and st.session_state.get("results_fingerprint") != current_fingerprint
    ):
        st.warning(
            "**Area settings changed** since the table below was built. "
            "Click **Start Scraping** again to refresh."
        )

    meta = st.session_state.get("last_scrape_run_meta") or {}
    if meta and show_advanced:
        with st.expander("Resolved scrape parameters (debug)", expanded=False):
            st.json(meta)

    last_eff = st.session_state.get("_last_successful_run_effective_pin")
    if last_eff and last_eff[0] is not None:
        cur = get_scrape_location()
        if abs(float(cur["lat"]) - float(last_eff[0])) > 1e-5 or abs(float(cur["lng"]) - float(last_eff[1])) > 1e-5:
            st.warning(
                "**Run pin changed** since the scrape that produced the table below. "
                "The API `scrape_run_meta.effective_scrape_pin_*` is for the displayed rows; re-run to align."
            )

    st.success(
        f"Collected **{len(df):,}** rows. Same brand may appear for different branches or grid samples; "
        "use **brand_id** for rollups and **branch_sku** for unique rows."
    )
    st.caption(
        "Runs use high-volume listing coverage, **vendor pages for many unique restaurants** (API caps), "
        "and Google Places when the API has a Maps key. Tune `RESTAURANT_DETAIL_ENRICH_MAX` / wall clock on the host if runs time out."
    )
    brand_series = df.get("brand_id", df.get("brand_display_name", pd.Series(dtype=str)))
    brand_count = int(pd.Series(brand_series).astype(str).str.strip().replace("", pd.NA).dropna().nunique())
    rating_series = _pick_rating_series(df)
    avg_rating = float(rating_series.dropna().mean()) if not rating_series.dropna().empty else 0.0
    m1, m2, m3 = st.columns(3)
    m1.metric("Total rows", int(len(df)))
    m2.metric("Unique brands", brand_count)
    m3.metric("Avg rating", f"{avg_rating:.2f}" if avg_rating > 0 else "—")

    tab_results, tab_heatmap, tab_outbound = st.tabs(
        ["Results", "Heatmap", "Whitespace"]
    )

    gdf = st.session_state.get("google_coverage_df", pd.DataFrame())

    with tab_results:
        st.dataframe(view_df, width="stretch", height=460)
        excel_df = build_excel_export_df(view_df)
        excel_bytes = dataframe_to_excel_bytes(excel_df)
        c1, c2, c3 = st.columns([2, 1, 1])
        c1.download_button(
            "Download Excel",
            data=excel_bytes,
            file_name="area_intel_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
        )
        c2.download_button(
            "CSV",
            data=view_df.to_csv(index=False).encode("utf-8"),
            file_name="area_intel_results.csv",
            mime="text/csv",
        )
        c3.download_button(
            "JSON",
            data=view_df.to_json(orient="records", force_ascii=False).encode("utf-8"),
            file_name="area_intel_results.json",
            mime="application/json",
        )
        if isinstance(gdf, pd.DataFrame) and not gdf.empty:
            talabat_place_ids = set(df.get("google_place_id", pd.Series(dtype=str)).astype(str).str.strip().str.lower().tolist())
            talabat_place_ids.discard("")
            if "google_place_id" in gdf.columns:
                google_place_ids = gdf["google_place_id"].astype(str).str.strip().str.lower()
                google_only = gdf.loc[~google_place_ids.isin(talabat_place_ids)].copy()
            else:
                google_only = gdf.copy()
            with st.expander("Google-only coverage candidates", expanded=False):
                st.dataframe(google_only, width="stretch", height=240)

    meta_pin_lat = meta.get("effective_scrape_pin_lat")
    meta_pin_lng = meta.get("effective_scrape_pin_lng")
    if meta_pin_lat is not None and meta_pin_lng is not None:
        hm_lat, hm_lng = float(meta_pin_lat), float(meta_pin_lng)
    else:
        # Fallback for older API responses that may not include effective pin fields yet.
        last_eff = st.session_state.get("_last_successful_run_effective_pin")
        if last_eff and len(last_eff) == 2:
            hm_lat, hm_lng = float(last_eff[0]), float(last_eff[1])
        else:
            loc_hm = get_scrape_location()
            hm_lat, hm_lng = float(loc_hm["lat"]), float(loc_hm["lng"])
    with tab_heatmap:
        st.caption(f"Heatmap center uses the effective scrape pin: `{hm_lat:.6f}, {hm_lng:.6f}`")
        render_heatmap(
            df,
            pin_lat=hm_lat,
            pin_lng=hm_lng,
            radius_km=float(radius_km),
            supply_df=st.session_state.get("supply_overlay_df"),
            google_coverage_df=st.session_state.get("google_coverage_df"),
        )

    with tab_outbound:
        render_outbound_prioritization_dashboard(df)


if __name__ == "__main__":
    main()
