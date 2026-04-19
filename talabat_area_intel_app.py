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
_SCRAPE_HIGH_VOLUME = True
_SCRAPE_MAX_SAMPLE_POINTS = 140
_SCRAPE_WALL_CLOCK_SEC = 1800
_SCRAPE_CLIENT_TIMEOUT_SEC = 2100


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

    _add_supply_overlay_feature_group(fmap, supply_df)

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

    heat_rows: list[list[float]] = []
    for _, row in view_df.iterrows():
        try:
            la = float(row["lat"])
            ln = float(row["lng"])
        except (TypeError, ValueError):
            continue
        heat_rows.append([la, ln])
    if heat_rows:
        HeatMap(
            heat_rows,
            min_opacity=0.28,
            max_zoom=17,
            radius=28,
            blur=19,
            gradient={0.35: "#2563EB", 0.55: "#7C3AED", 0.75: "#F59E0B", 0.95: "#EF4444"},
        ).add_to(fmap)

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

    Fullscreen(position="topright", title="Fullscreen", title_cancel="Exit Full Screen").add_to(fmap)
    folium.LayerControl(position="topright", collapsed=False).add_to(fmap)

    lats = [float(pin_lat)] + [r[0] for r in heat_rows]
    lngs = [float(pin_lng)] + [r[1] for r in heat_rows]
    supply_bounds = normalize_supply_overlay_df(supply_df)
    if supply_bounds is not None and not supply_bounds.empty:
        lats.extend(supply_bounds["lat"].astype(float).tolist())
        lngs.extend(supply_bounds["lng"].astype(float).tolist())
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


def main() -> None:
    st.set_page_config(page_title="Talabat Area Intel (English)", layout="wide")
    init_state()

    st.title("Talabat UAE Area Intel")
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
            index=0,
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
            st.caption(f"Suggested radius for this emirate: **{city_suggested_r:g} km** (adjust below).")
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

        radius_km = st.number_input(
            "Radius (km)",
            min_value=1.0,
            max_value=40.0,
            value=float(city_suggested_r if is_city_mode else 10.0),
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

        with st.expander("Supply overlay (CSV)", expanded=False):
            st.caption(
                "Optional markers on the **pin map** and **heatmap** (e.g. Kitchen Park sites). "
                "Columns: **lat** and **lng** (or latitude / longitude). Optional: **name**, label, site, id."
            )
            up = st.file_uploader("Upload supply CSV", type=["csv"], key="supply_overlay_upload")
            if up is not None:
                try:
                    sdf = pd.read_csv(up)
                    norm = normalize_supply_overlay_df(sdf)
                    if norm is None:
                        st.warning("Could not find lat/lng columns or no valid coordinate rows.")
                    else:
                        st.session_state["supply_overlay_df"] = norm
                        st.success(f"Loaded **{len(norm)}** supply point(s).")
                except Exception as exc:
                    st.error(f"Failed to read CSV: {exc}")
            overlay = st.session_state.get("supply_overlay_df")
            if overlay is not None and isinstance(overlay, pd.DataFrame) and not overlay.empty:
                if st.button("Clear supply overlay", key="clear_supply_overlay"):
                    st.session_state["supply_overlay_df"] = None
                    st.rerun()

        with st.expander("Listing harvest (API — vendor URL discovery)", expanded=False):
            st.caption(
                "Country-wide restaurants listing with **Next** pagination. "
                "Returns vendor URLs for your own tools or CSV export."
            )
            harvest_country = st.text_input("Country key", value="uae", key="listing_harvest_country")
            harvest_max_next = st.number_input("Max Next clicks", min_value=0, max_value=120, value=25, key="listing_harvest_max_next")
            if st.button("Run listing harvest", use_container_width=True, key="listing_harvest_btn"):
                try:
                    hr = requests.post(
                        f"{api_base_url.rstrip('/')}/listing-harvest",
                        json={
                            "country": harvest_country.strip() or "uae",
                            "max_next": int(harvest_max_next),
                            "harvest_wall_clock_sec": 480,
                        },
                        headers=headers,
                        timeout=600,
                    )
                    if hr.status_code >= 400:
                        st.session_state["_listing_harvest_err"] = hr.text[:800]
                        st.session_state["_listing_harvest_data"] = None
                    else:
                        st.session_state["_listing_harvest_data"] = hr.json()
                        st.session_state["_listing_harvest_err"] = None
                except Exception as exc:
                    st.session_state["_listing_harvest_err"] = str(exc)
                    st.session_state["_listing_harvest_data"] = None
            lh_err = st.session_state.get("_listing_harvest_err")
            lh_data = st.session_state.get("_listing_harvest_data")
            if lh_err:
                st.warning(str(lh_err))
            if lh_data and lh_data.get("ok"):
                ntot = int(lh_data.get("count_total") or 0)
                nret = int(lh_data.get("urls_returned") or 0)
                st.success(f"Harvested **{ntot:,}** vendor URLs (returned **{nret:,}** in JSON).")
                if lh_data.get("truncated"):
                    st.info("Response truncated — raise ``LISTING_HARVEST_RESPONSE_MAX_URLS`` on the API or page locally.")
                urls = lh_data.get("urls") or []
                if urls:
                    st.download_button(
                        "Download harvest CSV",
                        data=("url\n" + "\n".join(urls)).encode("utf-8"),
                        file_name="talabat_listing_harvest_urls.csv",
                        mime="text/csv",
                        key="dl_listing_harvest",
                    )
                    preview = "\n".join(urls[:40])
                    st.text_area("Copy / preview (first 40)", value=preview, height=200, key="listing_harvest_preview")

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

    st.subheader("Interactive search map")
    folium_out = render_pin_map(
        radius_km,
        lock_pin=False,
        supply_df=st.session_state.get("supply_overlay_df"),
    )
    store_folium_payload(folium_out)
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
        f"Radius: `{radius_km} km` · High-volume listing + all rows · Google Places when API key is set · "
        f"Status: `{listing_status_mode}` · New-only: `{new_on_platform_only}` · "
        f"Target label: `{target_area_label.strip() or '—'}`"
    )
    run = st.button("Start Scraping", type="primary", use_container_width=True)

    loc_fp = get_scrape_location()

    current_fingerprint = "|".join(
        [
            area_mode,
            city_key if is_city_mode else "custom",
            listing_status_mode,
            str(new_on_platform_only),
            target_area_label.strip(),
            f"{float(loc_fp['lat']):.6f}",
            f"{float(loc_fp['lng']):.6f}",
            str(radius_km),
        ]
    )

    if run:
        progress = st.progress(0.0)
        status_box = st.empty()

        with st.spinner("Cooking up some data magic... 🪄✨"):
            loc_req = get_scrape_location()
            try:
                parse_scrape_pin_or_raise_value_error(loc_req["lat"], loc_req["lng"])
            except ValueError as exc:
                st.error(f"Run pin is invalid — fix lat/lng before scraping. ({exc})")
                st.session_state["results_df"] = pd.DataFrame()
                st.session_state["last_run_done"] = False
            else:
                payload = {
                    "radius_km": float(radius_km),
                    "spacing_km": _DEFAULT_SPACING_KM,
                    "concurrency": _DEFAULT_CONCURRENCY,
                    "scroll_rounds": _DEFAULT_SCROLL_ROUNDS,
                    "scroll_wait_ms": _DEFAULT_SCROLL_WAIT_MS,
                    "status_filter": listing_status_mode,
                    "just_landed_only": bool(new_on_platform_only),
                    "max_sample_points": _SCRAPE_MAX_SAMPLE_POINTS,
                    "dedupe_by_vendor_url": _SCRAPE_DEDUPE_BY_VENDOR_URL,
                    "high_volume": _SCRAPE_HIGH_VOLUME,
                    "google_places_enrich": True,
                    "scrape_target_label": target_area_label.strip() or None,
                    "scrape_wall_clock_sec": _SCRAPE_WALL_CLOCK_SEC,
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
                    response = requests.post(
                        f"{api_base_url.rstrip('/')}/scrape",
                        json=payload,
                        headers=req_headers,
                        timeout=_SCRAPE_CLIENT_TIMEOUT_SEC,
                    )
                    if response.status_code >= 400:
                        raise RuntimeError(_friendly_api_error(response))
                    api_data = response.json()
                    api_request_id = str(api_data.get("request_id") or response.headers.get("X-Request-ID") or request_id)
                    df = pd.DataFrame(api_data.get("records", []))
                    st.session_state["last_scrape_city"] = api_data.get("city")
                    meta_run = api_data.get("scrape_run_meta") or {}
                    meta_run.setdefault("request_id", api_request_id)
                    st.session_state["last_scrape_run_meta"] = meta_run
                    elat = meta_run.get("effective_scrape_pin_lat")
                    elng = meta_run.get("effective_scrape_pin_lng")
                    if elat is not None and elng is not None:
                        st.session_state["_last_successful_run_effective_pin"] = (float(elat), float(elng))
                    progress.progress(1.0)
                    status_box.info(f"Remote scrape completed · request_id={api_request_id}")
                except Exception as exc:
                    st.error(f"Remote API scrape failed: {exc}")
                    df = pd.DataFrame()

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
    m1, m2 = st.columns(2)
    m1.metric("Rows in export", int(len(df)))
    m2.metric("Not closed", int((df["status"] != "closed").sum()))

    st.dataframe(df, use_container_width=True, height=420)

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
    )

    c1, c2 = st.columns(2)
    c1.download_button(
        "Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="talabat_area_intel_results.csv",
        mime="text/csv",
    )
    c2.download_button(
        "Download JSON",
        data=df.to_json(orient="records", force_ascii=False).encode("utf-8"),
        file_name="talabat_area_intel_results.json",
        mime="application/json",
    )


if __name__ == "__main__":
    main()
