from __future__ import annotations

import html
import math
import os

import folium
import pandas as pd
import requests
import streamlit as st
from folium.plugins import Fullscreen, HeatMap, MousePosition
from streamlit_folium import st_folium

from uae_cities import UAE_CITY_DISPLAY, UAE_CITY_PRESETS

DEFAULT_PIN = (25.2048, 55.2708)

# Match backend defaults (sidebar controls removed; tune via API env on Render if needed).
_DEFAULT_STATUS_FILTER = "live"
_DEFAULT_JUST_LANDED_ONLY = False
# More grid points + deeper scroll = more listing URLs merged (slower; watch SCRAPER_WALL_CLOCK_SEC on Render).
_DEFAULT_MAX_SAMPLE_POINTS = 3
_DEFAULT_SPACING_KM = 1.5
_DEFAULT_SCROLL_ROUNDS = 18
_DEFAULT_SCROLL_WAIT_MS = 900
_DEFAULT_CONCURRENCY = 1

_CITY_SLUGS = ["dubai", "sharjah", "abudhabi", "alain", "ajman"]


def init_state() -> None:
    st.session_state.setdefault("pin_lat", DEFAULT_PIN[0])
    st.session_state.setdefault("pin_lng", DEFAULT_PIN[1])
    st.session_state.setdefault("pin_label", "Dubai (default)")
    st.session_state.setdefault("results_df", pd.DataFrame())
    st.session_state.setdefault("last_run_done", False)
    st.session_state.setdefault("results_fingerprint", None)


def _bounds_for_radius(lat: float, lng: float, radius_km: float, pad: float = 1.15) -> tuple[list[float], list[float]]:
    """South-west and north-east corners so the map frames pin + search radius."""
    r = max(radius_km, 0.5) * pad
    d_lat = r / 110.574
    cos_lat = max(0.25, math.cos(math.radians(lat)))
    d_lng = r / (111.32 * cos_lat)
    return [lat - d_lat, lng - d_lng], [lat + d_lat, lng + d_lng]


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
    pin_lat: float | None = None,
    pin_lng: float | None = None,
    pin_label: str | None = None,
    lock_pin: bool = False,
) -> None:
    lat = float(pin_lat if pin_lat is not None else st.session_state["pin_lat"])
    lng = float(pin_lng if pin_lng is not None else st.session_state["pin_lng"])
    label = str(pin_label if pin_label is not None else st.session_state.get("pin_label") or "Search pin")

    fmap = folium.Map(
        location=[lat, lng],
        tiles=None,
        zoom_start=12,
        zoom_control=True,
        control_scale=True,
    )

    _add_esri_basemaps(fmap)

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

    st.caption(
        "**Satellite** is photos only (no street text). For English road names use **Street map**, "
        "or keep **Place names overlay** on. Layer control: top-right. Click map to move pin."
    )
    out = st_folium(
        fmap,
        width=1400,
        height=520,
        use_container_width=True,
        returned_objects=["last_clicked"],
        key="talabat_pin_map",
    )
    if out and out.get("last_clicked") and not lock_pin:
        st.session_state["pin_lat"] = float(out["last_clicked"]["lat"])
        st.session_state["pin_lng"] = float(out["last_clicked"]["lng"])
        st.session_state["pin_label"] = "Custom pin (map)"
        st.toast(f"Pin → {st.session_state['pin_lat']:.5f}, {st.session_state['pin_lng']:.5f}", icon="📍")


def render_heatmap(df: pd.DataFrame, pin_lat: float, pin_lng: float, radius_km: float) -> None:
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
    _add_esri_basemaps(fmap)

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

    Fullscreen(position="topright", title="Fullscreen", title_cancel="Exit Full Screen").add_to(fmap)
    folium.LayerControl(position="topright", collapsed=False).add_to(fmap)

    lats = [float(pin_lat)] + [r[0] for r in heat_rows]
    lngs = [float(pin_lng)] + [r[1] for r in heat_rows]
    pad_lat = max(0.002, (max(lats) - min(lats)) * 0.08 + 0.001)
    pad_lng = max(0.002, (max(lngs) - min(lngs)) * 0.08 + 0.001)
    sw = [min(lats) - pad_lat, min(lngs) - pad_lng]
    ne = [max(lats) + pad_lat, max(lngs) + pad_lng]
    fmap.fit_bounds([sw, ne], padding=(28, 28), max_zoom=16)

    st.caption(
        "Same English-first basemaps as the pin map. **Street** = full English-style road labels; "
        "**Satellite** + **Place names overlay** for labels. Heat = vendor density."
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


def get_api_base_url() -> str:
    try:
        secret_url = str(st.secrets.get("API_BASE_URL", "")).strip()
        if secret_url:
            return secret_url.rstrip("/")
    except Exception:
        pass
    return os.getenv("API_BASE_URL", "https://maisam21-lab-talabat-area-intel.onrender.com").strip().rstrip("/")


def main() -> None:
    st.set_page_config(page_title="Talabat Area Intel (English)", layout="wide")
    init_state()

    st.title("Talabat UAE Area Intel")
    st.caption(
        "**KitchenPark / expansion analytics:** compare cities for outbound acquisition using cuisine, ratings, "
        "delivery signals, and coverage. **No Google Cloud required** for search or scraping. "
        "Maps use Esri (English-friendly labels)."
    )

    with st.sidebar:
        st.header("Scrape Controls")
        st.caption(
            "Address search works **without** Google (OpenStreetMap). "
            "Remove `GOOGLE_MAPS_API_KEY` or set `GEOCODE_USE_GOOGLE=0` if you do not use GCP."
        )
        api_base_url = get_api_base_url()
        api_key = get_frontend_api_key()
        headers = {"X-API-Key": api_key} if api_key else {}

        area_mode = st.radio(
            "Area mode",
            ["UAE city (KitchenPark)", "Custom pin"],
            index=0,
            help="City mode uses fixed UAE centers (Dubai, Sharjah, Abu Dhabi, Al Ain, Ajman). "
            "Custom pin uses the map and manual coordinates.",
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
            st.session_state["pin_lat"] = float(city_lat)
            st.session_state["pin_lng"] = float(city_lng)
            st.session_state["pin_label"] = f"{UAE_CITY_DISPLAY[city_key]} (city preset)"

        dedupe_by_url = st.checkbox(
            "Dedupe: one row per vendor URL",
            value=False,
            help="Off (default): keep every listing row — same brand can appear for different branches / grid samples. "
            "On: collapse to one row per restaurant link.",
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
                    st.session_state["pin_lat"] = float(result["lat"])
                    st.session_state["pin_lng"] = float(result["lng"])
                    st.session_state["pin_label"] = str(result.get("formatted_address") or geocode_query).strip()
                    provider = payload.get("provider", "unknown")
                    st.success(f"Pin set from search ({provider}): {st.session_state['pin_label']}")
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

    st.subheader("Interactive search map")
    if is_city_mode:
        render_pin_map(
            radius_km,
            pin_lat=float(city_lat),
            pin_lng=float(city_lng),
            pin_label=UAE_CITY_DISPLAY[city_key],
            lock_pin=True,
        )
    else:
        render_pin_map(radius_km)

    if not is_city_mode:
        st.subheader("Manual Pin Override")
        mp1, mp2 = st.columns(2)
        with mp1:
            st.session_state["pin_lat"] = st.number_input(
                "Pin lat", value=float(st.session_state["pin_lat"]), format="%.6f"
            )
        with mp2:
            st.session_state["pin_lng"] = st.number_input(
                "Pin lng", value=float(st.session_state["pin_lng"]), format="%.6f"
            )
    else:
        st.caption("Switch to **Custom pin** to move the pin by hand or use geocode search.")

    st.subheader("Run")
    if is_city_mode:
        st.write(f"**City:** `{UAE_CITY_DISPLAY[city_key]}` · center `{city_lat:.6f}, {city_lng:.6f}`")
    else:
        st.write(f"Current pin: `{st.session_state['pin_lat']:.6f}, {st.session_state['pin_lng']:.6f}`")
    st.write(f"Radius: `{radius_km} km` · Dedupe by URL: `{dedupe_by_url}`")
    run = st.button("Start Scraping", type="primary", use_container_width=True)

    current_fingerprint = "|".join(
        [
            area_mode,
            city_key if is_city_mode else "custom",
            str(dedupe_by_url),
            f"{float(st.session_state['pin_lat']):.6f}",
            f"{float(st.session_state['pin_lng']):.6f}",
            str(radius_km),
        ]
    )

    if run:
        progress = st.progress(0.0)
        status_box = st.empty()

        with st.spinner("Scraping..."):
            payload = {
                "radius_km": float(radius_km),
                "spacing_km": _DEFAULT_SPACING_KM,
                "concurrency": _DEFAULT_CONCURRENCY,
                "scroll_rounds": _DEFAULT_SCROLL_ROUNDS,
                "scroll_wait_ms": _DEFAULT_SCROLL_WAIT_MS,
                "status_filter": _DEFAULT_STATUS_FILTER,
                "just_landed_only": _DEFAULT_JUST_LANDED_ONLY,
                "max_sample_points": _DEFAULT_MAX_SAMPLE_POINTS,
                "dedupe_by_vendor_url": dedupe_by_url,
            }
            if is_city_mode:
                payload["city"] = city_key
                payload["pin_lat"] = float(city_lat)
                payload["pin_lng"] = float(city_lng)
            else:
                payload["city"] = None
                payload["pin_lat"] = float(st.session_state["pin_lat"])
                payload["pin_lng"] = float(st.session_state["pin_lng"])
            try:
                response = requests.post(
                    f"{api_base_url.rstrip('/')}/scrape",
                    json=payload,
                    headers=headers,
                    timeout=600,
                )
                if response.status_code >= 400:
                    detail = response.text[:500]
                    raise RuntimeError(f"{response.status_code} {response.reason}: {detail}")
                api_data = response.json()
                df = pd.DataFrame(api_data.get("records", []))
                st.session_state["last_dedupe_by_url"] = bool(api_data.get("dedupe_by_vendor_url", False))
                st.session_state["last_scrape_city"] = api_data.get("city")
                progress.progress(1.0)
                status_box.info("Remote scrape completed")
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

    dedupe_done = bool(st.session_state.get("last_dedupe_by_url", False))
    if dedupe_done:
        st.success(f"Collected **{len(df):,}** rows (one row per vendor URL).")
    else:
        st.success(
            f"Collected **{len(df):,}** listing rows (dedupe **off** — same brand may appear for branches / samples). "
            "Use `scrape_city` and `branch_sku` for analysis."
        )
    st.caption(
        "More filled columns: API env `RESTAURANT_DETAIL_ENRICH_MAX` (Talabat vendor pages). "
        "Optional Google-only enrichment: `GOOGLE_PLACES_ENRICH=1` + a Maps key with Places API (skip if you have no GCP). "
        "`SCRAPER_AGGRESSIVE_LISTING=1` or `SCRAPER_LISTING_SCROLL_ROUNDS` for deeper listing scroll. "
        "If runs time out, lower `max_sample_points` or raise `SCRAPER_WALL_CLOCK_SEC` on Render."
    )
    m1, m2 = st.columns(2)
    m1.metric("Rows in export", int(len(df)))
    m2.metric("Not closed", int((df["status"] != "closed").sum()))

    st.dataframe(df, use_container_width=True, height=420)
    _hlat = float(city_lat) if is_city_mode else float(st.session_state["pin_lat"])
    _hlng = float(city_lng) if is_city_mode else float(st.session_state["pin_lng"])
    render_heatmap(df, pin_lat=_hlat, pin_lng=_hlng, radius_km=float(radius_km))

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
