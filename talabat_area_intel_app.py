from __future__ import annotations

import html
import math
import os

import folium
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from folium.plugins import Fullscreen, MousePosition
from streamlit_folium import st_folium

DEFAULT_PIN = (25.2048, 55.2708)

# Match backend defaults (sidebar controls removed; tune via API env on Render if needed).
_DEFAULT_STATUS_FILTER = "live"
_DEFAULT_JUST_LANDED_ONLY = False
_DEFAULT_MAX_SAMPLE_POINTS = 2


def init_state() -> None:
    st.session_state.setdefault("pin_lat", DEFAULT_PIN[0])
    st.session_state.setdefault("pin_lng", DEFAULT_PIN[1])
    st.session_state.setdefault("pin_label", "Dubai (default)")
    st.session_state.setdefault("results_df", pd.DataFrame())
    st.session_state.setdefault("last_run_done", False)
    st.session_state.setdefault("results_fingerprint", None)


def _google_maps_frontend_key() -> str:
    """Optional: same key as backend geocode; used for pydeck Google basemap (English-friendly labels)."""
    try:
        s = str(st.secrets.get("GOOGLE_MAPS_API_KEY", "")).strip()
        if s:
            return s
    except Exception:
        pass
    return (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()


def _bounds_for_radius(lat: float, lng: float, radius_km: float, pad: float = 1.15) -> tuple[list[float], list[float]]:
    """South-west and north-east corners so the map frames pin + search radius."""
    r = max(radius_km, 0.5) * pad
    d_lat = r / 110.574
    cos_lat = max(0.25, math.cos(math.radians(lat)))
    d_lng = r / (111.32 * cos_lat)
    return [lat - d_lat, lng - d_lng], [lat + d_lat, lng + d_lng]


def render_pin_map(radius_km: float) -> None:
    lat = float(st.session_state["pin_lat"])
    lng = float(st.session_state["pin_lng"])
    label = str(st.session_state.get("pin_label") or "Search pin")

    fmap = folium.Map(
        location=[lat, lng],
        tiles=None,
        zoom_start=12,
        zoom_control=True,
        control_scale=True,
    )

    # Esri World Street: English-centric labels in UAE (Carto/OSM tiles often switch to Arabic when zoomed in).
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        attr=(
            'Tiles © <a href="https://www.esri.com/">Esri</a> '
            "(HERE, Garmin, OpenStreetMap contributors, GIS user community)"
        ),
        name="Street map (English)",
        max_zoom=19,
    ).add_to(fmap)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr='Tiles © <a href="https://www.esri.com/">Esri</a> (Earthstar Geographics, USDA, USGS, AeroGRID, IGN)',
        name="Satellite (Esri)",
        max_zoom=19,
    ).add_to(fmap)

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
        icon=folium.Icon(color="blue", icon="info-sign"),
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
        "The view frames your pin and scrape radius when they change. "
        "Basemaps use Esri (English labels at all zoom levels). "
        "Click to move the pin. Use the layer control (street / satellite), fullscreen, and cursor coordinates (bottom-left)."
    )
    out = st_folium(
        fmap,
        width=1400,
        height=520,
        use_container_width=True,
        returned_objects=["last_clicked"],
        key="talabat_pin_map",
    )
    if out and out.get("last_clicked"):
        st.session_state["pin_lat"] = float(out["last_clicked"]["lat"])
        st.session_state["pin_lng"] = float(out["last_clicked"]["lng"])
        st.session_state["pin_label"] = "Custom pin (map)"
        st.toast(f"Pin → {st.session_state['pin_lat']:.5f}, {st.session_state['pin_lng']:.5f}", icon="📍")


def _heatmap_zoom_for_radius(radius_km: float) -> float:
    if radius_km <= 3.0:
        return 13.0
    if radius_km <= 8.0:
        return 12.0
    if radius_km <= 15.0:
        return 11.0
    return 10.0


def render_heatmap(df: pd.DataFrame, pin_lat: float, pin_lng: float, radius_km: float) -> None:
    st.subheader("Restaurant Density Heatmap")
    view_df = df.dropna(subset=["lat", "lng"]).copy()
    if view_df.empty:
        st.info("No coordinates available for heatmap.")
        return

    layer = pdk.Layer(
        "HeatmapLayer",
        data=view_df,
        get_position="[lng, lat]",
        get_weight=1,
        radiusPixels=45,
        intensity=1.2,
        threshold=0.05,
        opacity=0.8,
    )
    view_state = pdk.ViewState(
        latitude=float(pin_lat),
        longitude=float(pin_lng),
        zoom=_heatmap_zoom_for_radius(radius_km),
        pitch=22,
    )
    gkey = _google_maps_frontend_key()
    if gkey:
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_provider="google_maps",
            map_style=pdk.map_styles.GOOGLE_ROAD,
            api_keys={"google_maps": gkey},
        )
        st.caption("Heatmap basemap: Google Maps roadmap (English labels).")
    else:
        # Carto vector styles often show OSM `name:ar` when zoomed in; no-label avoids mixed scripts without a Maps key.
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_provider="carto",
            map_style=pdk.map_styles.LIGHT_NO_LABELS,
        )
        st.caption(
            "Heatmap basemap: labels hidden (add `GOOGLE_MAPS_API_KEY` to Streamlit secrets for a labeled English map)."
        )
    st.pydeck_chart(deck, use_container_width=True)


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
    st.set_page_config(page_title="Talabat Area Intel", layout="wide")
    init_state()

    st.title("Talabat UAE Area Intel")
    st.caption("Visual map pin + radius scraping + new branch tracking + heatmap.")

    with st.sidebar:
        st.header("Scrape Controls")
        api_base_url = get_api_base_url()
        api_key = get_frontend_api_key()
        headers = {"X-API-Key": api_key} if api_key else {}
        geocode_query = st.text_input("Search place/address (UAE)", value="")
        geocode_btn = st.button("Set Pin from Search", use_container_width=True)
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
                else:
                    hint = payload.get("hint")
                    if hint:
                        st.warning(hint)
                    else:
                        st.warning(f"No geocoding result. {payload.get('error', 'no details')}")
            except Exception as exc:
                st.error(f"Geocode failed via backend: {exc}")

        radius_km = st.number_input("Radius (km)", min_value=1.0, max_value=30.0, value=10.0, step=0.5)

    st.subheader("Interactive search map")
    render_pin_map(radius_km=radius_km)

    # Manual pin override moved below the map as requested.
    st.subheader("Manual Pin Override")
    mp1, mp2 = st.columns(2)
    with mp1:
        st.session_state["pin_lat"] = st.number_input("Pin lat", value=float(st.session_state["pin_lat"]), format="%.6f")
    with mp2:
        st.session_state["pin_lng"] = st.number_input("Pin lng", value=float(st.session_state["pin_lng"]), format="%.6f")

    st.subheader("Run")
    st.write(f"Current pin: `{st.session_state['pin_lat']:.6f}, {st.session_state['pin_lng']:.6f}`")
    st.write(f"Radius: `{radius_km} km`")
    run = st.button("Start Scraping", type="primary", use_container_width=True)

    current_fingerprint = "|".join(
        [
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
                "pin_lat": float(st.session_state["pin_lat"]),
                "pin_lng": float(st.session_state["pin_lng"]),
                "radius_km": float(radius_km),
                "status_filter": _DEFAULT_STATUS_FILTER,
                "just_landed_only": _DEFAULT_JUST_LANDED_ONLY,
                "max_sample_points": _DEFAULT_MAX_SAMPLE_POINTS,
            }
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
            "**Pin or radius changed** since the table below was built. "
            "Click **Start Scraping** again to refresh results for the current map."
        )

    st.success(f"Collected {len(df):,} unique vendor rows (deduped by URL).")
    m1, m2 = st.columns(2)
    m1.metric("Total vendors", int(len(df)))
    m2.metric("Not closed", int((df["status"] != "closed").sum()))

    st.dataframe(df, use_container_width=True, height=420)
    render_heatmap(
        df,
        pin_lat=float(st.session_state["pin_lat"]),
        pin_lng=float(st.session_state["pin_lng"]),
        radius_km=float(radius_km),
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
