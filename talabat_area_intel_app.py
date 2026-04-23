from __future__ import annotations

import html
import math
import os
import uuid

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
_DEFAULT_SPACING_KM = 1.5
_DEFAULT_SCROLL_ROUNDS = 18
_DEFAULT_SCROLL_WAIT_MS = 900
_DEFAULT_CONCURRENCY = 1

_CITY_SLUGS = ["dubai", "sharjah", "abudhabi", "alain", "ajman"]

# Product defaults (no client toggles): full grid + cuisine sweep, keep all listing rows, request Places enrichment.
_SCRAPE_DEDUPE_BY_VENDOR_URL = False
_SCRAPE_HIGH_VOLUME = False
_SCRAPE_MAX_SAMPLE_POINTS = 6
_SCRAPE_CLIENT_TIMEOUT_SEC = 1300

_SCRAPE_PROFILES: dict[str, dict] = {
    # Quick baseline in constrained hosting.
    "Fast": {
        "high_volume": False,
        "max_sample_points": 6,
        "scroll_rounds": 10,
        "scroll_wait_ms": _DEFAULT_SCROLL_WAIT_MS,
        "google_places_enrich": True,
    },
    # Better coverage with moderate runtime.
    "Balanced": {
        "high_volume": False,
        "max_sample_points": 14,
        "scroll_rounds": 14,
        "scroll_wait_ms": _DEFAULT_SCROLL_WAIT_MS,
        "google_places_enrich": True,
    },
    # Highest completeness; slower and more timeout-prone.
    "Complete": {
        "high_volume": True,
        "max_sample_points": 60,
        "scroll_rounds": 18,
        "scroll_wait_ms": _DEFAULT_SCROLL_WAIT_MS,
        "google_places_enrich": True,
    },
}
_DEFAULT_SCRAPE_PROFILE = "Complete"
_BUILD_STAMP = os.getenv("APP_BUILD_STAMP", "2026-04-23-executive-mode-80dbf21")


def init_state() -> None:
    ensure_scrape_location(
        default_lat=float(DEFAULT_PIN[0]),
        default_lng=float(DEFAULT_PIN[1]),
        default_label="Dubai (default)",
        migrate_from_legacy_keys=True,
    )
    sync_legacy_pin_mirror()
    st.session_state.setdefault("results_df", pd.DataFrame())
    st.session_state.setdefault("last_run_done", False)
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
        use_container_width=True,
        returned_objects=["last_clicked", "center"],
        key="talabat_pin_map",
    )
    out = dict(out or {})
    if out.get("last_clicked") and not lock_pin:
        lc = out["last_clicked"]
        set_scrape_location(float(lc["lat"]), float(lc["lng"]), "Custom pin (map)", "folium_click")
        sync_legacy_pin_mirror()
        st.toast(f"Pin → {float(lc['lat']):.5f}, {float(lc['lng']):.5f}", icon="📍")
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
        heat_fg = folium.FeatureGroup(name="Talabat density heat", overlay=True, control=True)
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
            "Same **Google** basemaps as the pin map. Heat = **Talabat listing density** from this scrape "
            "(single aggregator; multi-aggregator overlay is not wired yet)."
        )
    else:
        st.caption(
            "Same English-first basemaps as the pin map. **Street** = full English-style road labels; "
            "**Satellite** + **Place names overlay** for labels. Heat = **Talabat listing density** from this scrape "
            "(single aggregator; multi-aggregator overlay is not wired yet)."
        )
    st_folium(
        fmap,
        width=1400,
        height=480,
        use_container_width=True,
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
    st.dataframe(view, use_container_width=True, height=380)

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
            use_container_width=True,
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


def main() -> None:
    st.set_page_config(page_title="Talabat Area Intel (English)", layout="wide")
    init_state()

    st.title("Talabat UAE Area Intel")
    st.warning(f"Build: `{_BUILD_STAMP}`")
    st.caption(
        "**KitchenPark / expansion analytics:** compare cities for outbound acquisition using cuisine, ratings, "
        "delivery signals, and coverage. Each scrape requests **Google Places** enrichment when the API has a Maps key; "
        "geocode can still use OpenStreetMap. **Maps:** Google roadmap/satellite when ``GOOGLE_MAPS_API_KEY`` is set "
        "and **Map Tiles API** is enabled (otherwise Esri). Set ``STREAMLIT_MAP_BASEMAP=esri`` to force Esri."
    )

    with st.sidebar:
        st.header("Scrape Controls")
        st.caption(
            "**Geocode:** if `GOOGLE_MAPS_API_KEY` is set and `GEOCODE_USE_GOOGLE=1`, Google is tried first; "
            "otherwise OpenStreetMap Nominatim. **Maps:** same key is reused for **Map Tiles** basemaps unless "
            "`STREAMLIT_MAP_BASEMAP=esri`. **Scrape:** high-volume listing + Google Places enrich is always requested "
            "(Places runs only when the key and billing allow it)."
        )
        api_base_url = get_api_base_url()
        api_key = get_frontend_api_key()
        headers = {"X-API-Key": api_key} if api_key else {}

        area_mode = st.radio(
            "Area mode",
            ["UAE city (KitchenPark)", "Custom pin"],
            index=1,
            help=            "City mode sets a default centre per emirate; **click the map** to move the search pin — "
            "the API uses that pin, not a hidden fixed city centre. Custom pin uses lat/lng fields only.",
        )
        is_city_mode = area_mode.startswith("UAE")
        city_key = "dubai"
        city_lat, city_lng = DEFAULT_PIN[0], DEFAULT_PIN[1]
        city_suggested_r = 12.0
        if is_city_mode:
            city_key = st.selectbox(
                "City",
                _CITY_SLUGS,
                format_func=lambda k: UAE_CITY_DISPLAY[k],
                index=0,
            )
            city_lat, city_lng, city_suggested_r = UAE_CITY_PRESETS[city_key]
            st.caption(
                f"Suggested radius for this emirate: **{city_suggested_r:g} km** "
                "(input is constrained to **5–10 km**)."
            )
            seed_city_preset_if_changed(
                city_key,
                float(city_lat),
                float(city_lng),
                UAE_CITY_DISPLAY[city_key],
            )
            sync_legacy_pin_mirror()

        target_area_label = st.text_input(
            "Target area label (optional)",
            value="",
            help="Stored on every row as scrape_target_label for reporting (e.g. Dubai Marina, DIP). "
            "Use with Custom pin + radius for micro-market scrapes.",
        )
        listing_status_mode = st.selectbox(
            "Listing status filter",
            options=["live", "all", "closed"],
            index=0,
            format_func=lambda x: {
                "live": "Live + unknown (drop closed-looking rows)",
                "all": "All rows (no status filter)",
                "closed": "Closed-looking rows only",
            }[x],
            help="This is our classifier on listing text, not Talabat's full operational status.",
        )
        new_on_platform_only = st.checkbox(
            "New on platform only",
            value=False,
            help="Turns on Talabat 'Just Landed' listing mode when the UI exposes it, then keeps rows with "
            "just_landed=yes or recently_added_90d=yes.",
        )
        include_google_coverage = st.checkbox(
            "Include Google-only coverage layer",
            value=True,
            help="Fetches nearby Google restaurants around the same pin/radius and overlays them on maps.",
        )
        radius_km = st.number_input(
            "Radius (km)",
            min_value=5.0,
            max_value=10.0,
            value=float(min(10.0, max(5.0, city_suggested_r if is_city_mode else 10.0))),
            step=0.5,
            key="scrape_radius_km_widget",
        )

        if not is_city_mode:
            geocode_query = st.text_input("Search place/address (UAE)", value="")
            geocode_btn = st.button("Set Pin from Search", use_container_width=True)
            lp = st.session_state.get("last_geocode_provider")
            ll = st.session_state.get("last_geocode_label")
            if lp:
                lab = str(ll or "").strip()
                suffix = f" — {lab[:80]}…" if len(lab) > 80 else (f" — {lab}" if lab else "")
                st.caption(f"**Last geocode provider:** `{lp}`{suffix}")
            else:
                st.caption("**Last geocode provider:** — (run *Set Pin from Search* once to record)")
        else:
            geocode_query = ""
            geocode_btn = False

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

    _pin_widget_scope = str(city_key) if is_city_mode else "custom_pin_mode"
    st.subheader("Run pin (single source for scraping)")
    st.caption(
        "Lat/lng here, map clicks, and **Start Scraping** all use the same `scrape_location` session object. "
        "The API echoes pins and radius counts in **Resolved scrape parameters** after each run."
    )
    loc_ui = get_scrape_location()
    rp1, rp2 = st.columns(2)
    with rp1:
        run_lat = st.number_input(
            "Run pin latitude",
            value=float(loc_ui["lat"]),
            format="%.6f",
            step=0.0001,
            key=f"run_pin_lat__{_pin_widget_scope}",
        )
    with rp2:
        run_lng = st.number_input(
            "Run pin longitude",
            value=float(loc_ui["lng"]),
            format="%.6f",
            step=0.0001,
            key=f"run_pin_lng__{_pin_widget_scope}",
        )
    set_scrape_location(
        float(run_lat),
        float(run_lng),
        str(loc_ui.get("label") or "Run pin"),
        "manual_form",
    )
    sync_legacy_pin_mirror()

    preview_two_pinned = (not is_city_mode) and str(st.session_state.get("pinned_scrape_count", "one")) == "two"
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

    st.subheader("Interactive search map")
    folium_out = render_pin_map(
        radius_km,
        lock_pin=False,
        supply_df=st.session_state.get("supply_overlay_df"),
        google_coverage_df=st.session_state.get("google_coverage_df"),
        dual_points=dual_points_for_map,
    )
    store_folium_payload(folium_out)
    if preview_two_pinned and folium_out.get("last_clicked"):
        lc = folium_out["last_clicked"]
        click_lat = float(lc["lat"])
        click_lng = float(lc["lng"])
        click_sig = f"{click_lat:.6f},{click_lng:.6f}"
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
            st.toast(f"Area {slot} pin → {click_lat:.5f}, {click_lng:.5f}", icon="📌")
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
        f"Radius: `{radius_km} km` · Profile: `{_DEFAULT_SCRAPE_PROFILE}` (default) · Google Places when API key is set · "
        f"Status: `{listing_status_mode}` · New-only: `{new_on_platform_only}` · "
        f"Target label: `{target_area_label.strip() or '—'}`"
    )
    st.caption(
        "Expected runtime is usually a few minutes, but can be longer on heavy areas. "
        "Scrape wall-clock is controlled by the API service environment."
    )
    if is_city_mode:
        pinned_count = "one"
        st.caption("**Two pinned areas** is only available in **Custom pin** mode (city mode always uses one run pin).")
    else:
        pinned_count = st.radio(
            "Pinned areas to scrape",
            options=["one", "two"],
            format_func=lambda v: "One (run pin above + map)" if v == "one" else "Two (pins A + B below)",
            horizontal=True,
            key="pinned_scrape_count",
        )
    use_two_pinned = (not is_city_mode) and pinned_count == "two"

    if use_two_pinned:
        st.caption(
            "Runs two `/scrape` calls (A then B) with the same radius and profile. "
            "Rows include **`dual_area`** (`A` / `B`) and **`batch_location_label`** (your optional labels). "
            "Sidebar *Target area label* is not applied — use A/B labels below. "
            "Map clicks assign pins in sequence: **A**, then **B**, then **A**..."
        )
        ca, cb = st.columns(2)
        with ca:
            st.markdown("**Area A**")
            st.number_input("Latitude", key="dual_area_a_lat", format="%.6f", step=0.0001)
            st.number_input("Longitude", key="dual_area_a_lng", format="%.6f", step=0.0001)
            st.text_input("Label (optional)", key="dual_area_a_label")
            if st.button("Copy current run pin → A", key="dual_copy_run_to_a"):
                st.session_state["dual_area_a_lat"] = float(loc_run["lat"])
                st.session_state["dual_area_a_lng"] = float(loc_run["lng"])
                st.rerun()
        with cb:
            st.markdown("**Area B**")
            st.number_input("Latitude", key="dual_area_b_lat", format="%.6f", step=0.0001)
            st.number_input("Longitude", key="dual_area_b_lng", format="%.6f", step=0.0001)
            st.text_input("Label (optional)", key="dual_area_b_label")
            if st.button("Copy current run pin → B", key="dual_copy_run_to_b"):
                st.session_state["dual_area_b_lat"] = float(loc_run["lat"])
                st.session_state["dual_area_b_lng"] = float(loc_run["lng"])
                st.rerun()

    dual_a_ok, dual_b_ok = False, False
    dual_pin_err = ""
    if not is_city_mode:
        try:
            parse_scrape_pin_or_raise_value_error(
                st.session_state["dual_area_a_lat"], st.session_state["dual_area_a_lng"]
            )
            dual_a_ok = True
        except ValueError as exc:
            dual_pin_err = f"Area A pin invalid: {exc}"
        try:
            parse_scrape_pin_or_raise_value_error(
                st.session_state["dual_area_b_lat"], st.session_state["dual_area_b_lng"]
            )
            dual_b_ok = True
        except ValueError as exc:
            dual_pin_err = dual_pin_err or f"Area B pin invalid: {exc}"
    pins_identical = False
    if not is_city_mode and dual_a_ok and dual_b_ok:
        pins_identical = abs(float(st.session_state["dual_area_a_lat"]) - float(st.session_state["dual_area_b_lat"])) < 1e-6 and abs(
            float(st.session_state["dual_area_a_lng"]) - float(st.session_state["dual_area_b_lng"])
        ) < 1e-6
    dual_ready = (not is_city_mode) and dual_a_ok and dual_b_ok and not pins_identical
    if use_two_pinned and dual_pin_err:
        st.warning(dual_pin_err)
    if use_two_pinned and pins_identical:
        st.warning("Area A and Area B pins are identical — move B or use *Copy current run pin* after moving the map.")

    run_single = st.button(
        "Start scraping",
        type="primary",
        use_container_width=True,
        disabled=use_two_pinned,
    )
    run_two_pins = False
    if use_two_pinned:
        run_two_pins = st.button(
            "Scrape two areas (A + B)",
            type="primary",
            use_container_width=True,
            disabled=not dual_ready,
        )

    loc_fp = get_scrape_location()
    dual_sig = "none"
    if use_two_pinned:
        dual_sig = (
            f"A:{float(st.session_state['dual_area_a_lat']):.5f},{float(st.session_state['dual_area_a_lng']):.5f}|"
            f"B:{float(st.session_state['dual_area_b_lat']):.5f},{float(st.session_state['dual_area_b_lng']):.5f}"
        )
    elif not is_city_mode:
        dual_sig = "one_pin"

    current_fingerprint = "|".join(
        [
            area_mode,
            city_key if is_city_mode else "custom",
            pinned_count,
            listing_status_mode,
            str(new_on_platform_only),
            _DEFAULT_SCRAPE_PROFILE,
            str(include_google_coverage),
            dual_sig,
            target_area_label.strip(),
            f"{float(loc_fp['lat']):.6f}",
            f"{float(loc_fp['lng']):.6f}",
            str(radius_km),
        ]
    )

    if run_two_pins:
        progress = st.progress(0.0)
        status_box = st.empty()
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
        profile_cfg = _SCRAPE_PROFILES.get(_DEFAULT_SCRAPE_PROFILE, _SCRAPE_PROFILES["Complete"])
        base_payload = {
            "radius_km": float(radius_km),
            "spacing_km": _DEFAULT_SPACING_KM,
            "concurrency": _DEFAULT_CONCURRENCY,
            "scroll_rounds": int(profile_cfg["scroll_rounds"]),
            "scroll_wait_ms": int(profile_cfg["scroll_wait_ms"]),
            "status_filter": "all",
            "just_landed_only": False,
            "max_sample_points": int(profile_cfg["max_sample_points"]),
            "dedupe_by_vendor_url": _SCRAPE_DEDUPE_BY_VENDOR_URL,
            "high_volume": bool(profile_cfg["high_volume"]),
            "google_places_enrich": bool(profile_cfg["google_places_enrich"]),
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
        progress.progress(1.0)
        st.session_state["results_df"] = dual_df
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
                profile_cfg = _SCRAPE_PROFILES.get(_DEFAULT_SCRAPE_PROFILE, _SCRAPE_PROFILES["Complete"])
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
                    request_id = uuid.uuid4().hex
                    req_headers = dict(headers)
                    req_headers["X-Request-ID"] = request_id
                    def _post_scrape(req_payload: dict, timeout_msg: str) -> requests.Response | None:
                        try:
                            return requests.post(
                                f"{api_base_url.rstrip('/')}/scrape",
                                json=req_payload,
                                headers=req_headers,
                                timeout=_SCRAPE_CLIENT_TIMEOUT_SEC,
                            )
                        except requests.exceptions.ReadTimeout:
                            status_box.warning(timeout_msg)
                            return None

                    response = _post_scrape(payload, "Primary scrape hit client read-timeout. Retrying with lighter settings...")
                    if response is None or response.status_code >= 400:
                        code = int(response.status_code) if response is not None else 504
                        # Hosted gateway timeouts are common; retry with progressively lighter payloads.
                        if code in (502, 504):
                            fallback_payload = dict(payload)
                            fallback_payload["high_volume"] = False
                            fallback_payload["max_sample_points"] = min(int(payload["max_sample_points"]), 12)
                            fallback_payload["scroll_rounds"] = 10
                            response = _post_scrape(
                                fallback_payload,
                                "Lighter retry also hit read-timeout. Retrying once with ultra-light settings...",
                            )
                            code = int(response.status_code) if response is not None else 504
                            if code in (502, 504):
                                ultra_payload = dict(fallback_payload)
                                ultra_payload["max_sample_points"] = min(int(fallback_payload["max_sample_points"]), 4)
                                ultra_payload["scroll_rounds"] = 8
                                ultra_payload["spacing_km"] = 2.5
                                response = _post_scrape(
                                    ultra_payload,
                                    "Ultra-light retry also hit read-timeout.",
                                )
                        if response is None:
                            raise RuntimeError(
                                "Client read-timeout after all retries. Backend is overloaded or blocked by host limits."
                            )
                        if response.status_code >= 400:
                            raise RuntimeError(_friendly_api_error(response))
                    api_data = response.json()
                    api_request_id = str(api_data.get("request_id") or response.headers.get("X-Request-ID") or request_id)
                    df = pd.DataFrame(api_data.get("records", []))
                    if df.empty:
                        status_box.warning("Scrape returned 0 rows. Trying emergency single-point fallback...")
                        emergency_payload = dict(payload)
                        emergency_payload["high_volume"] = False
                        emergency_payload["dedupe_by_vendor_url"] = True
                        emergency_payload["status_filter"] = "all"
                        emergency_payload["max_sample_points"] = 1
                        emergency_payload["scroll_rounds"] = 4
                        emergency_payload["scroll_wait_ms"] = min(int(payload["scroll_wait_ms"]), 700)
                        em_resp = requests.post(
                            f"{api_base_url.rstrip('/')}/scrape",
                            json=emergency_payload,
                            headers=req_headers,
                            timeout=_SCRAPE_CLIENT_TIMEOUT_SEC,
                        )
                        if em_resp.status_code < 400:
                            em_data = em_resp.json()
                            em_df = pd.DataFrame(em_data.get("records", []))
                            if not em_df.empty:
                                df = em_df
                                api_request_id = str(
                                    em_data.get("request_id") or em_resp.headers.get("X-Request-ID") or api_request_id
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
                except Exception as exc:
                    st.error(f"Remote API scrape failed: {exc}")
                    df = pd.DataFrame()
                    st.session_state["google_coverage_df"] = pd.DataFrame()

                st.session_state["results_df"] = df
                st.session_state["last_run_done"] = True
                st.session_state["results_fingerprint"] = current_fingerprint

    df = st.session_state.get("results_df", pd.DataFrame())
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
    if meta:
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
    render_executive_mode(df, meta)
    m1, m2 = st.columns(2)
    m1.metric("Rows in export", int(len(df)))
    m2.metric("Not closed", int((df["status"] != "closed").sum()))

    gdf = st.session_state.get("google_coverage_df", pd.DataFrame())
    if isinstance(gdf, pd.DataFrame) and not gdf.empty:
        talabat_place_ids = set(df.get("google_place_id", pd.Series(dtype=str)).astype(str).str.strip().str.lower().tolist())
        talabat_place_ids.discard("")
        if "google_place_id" in gdf.columns:
            google_place_ids = gdf["google_place_id"].astype(str).str.strip().str.lower()
            google_only = gdf.loc[~google_place_ids.isin(talabat_place_ids)].copy()
        else:
            google_only = gdf.copy()
        cgo1, cgo2 = st.columns(2)
        cgo1.metric("Google nearby candidates", int(len(gdf)))
        cgo2.metric("Google-only (not in Talabat rows)", int(len(google_only)))
        with st.expander("Google-only coverage candidates", expanded=False):
            st.caption(
                "Nearby Google restaurants in the same pin/radius that are not matched to current Talabat rows by place_id."
            )
            st.dataframe(google_only, use_container_width=True, height=280)
            st.download_button(
                "Download Google-only CSV",
                data=google_only.to_csv(index=False).encode("utf-8"),
                file_name="google_only_coverage_candidates.csv",
                mime="text/csv",
            )

    st.dataframe(view_df, use_container_width=True, height=420)

    render_outbound_prioritization_dashboard(df)

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
    st.caption(f"Heatmap center uses the effective scrape pin: `{hm_lat:.6f}, {hm_lng:.6f}`")
    render_heatmap(
        df,
        pin_lat=hm_lat,
        pin_lng=hm_lng,
        radius_km=float(radius_km),
        supply_df=st.session_state.get("supply_overlay_df"),
        google_coverage_df=st.session_state.get("google_coverage_df"),
    )

    c1, c2 = st.columns(2)
    c1.download_button(
        "Download CSV",
        data=view_df.to_csv(index=False).encode("utf-8"),
        file_name="talabat_area_intel_results.csv",
        mime="text/csv",
    )
    c2.download_button(
        "Download JSON",
        data=view_df.to_json(orient="records", force_ascii=False).encode("utf-8"),
        file_name="talabat_area_intel_results.json",
        mime="application/json",
    )


if __name__ == "__main__":
    main()
