from __future__ import annotations

import os

import folium
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from streamlit_folium import st_folium

DEFAULT_PIN = (25.2048, 55.2708)


def init_state() -> None:
    st.session_state.setdefault("pin_lat", DEFAULT_PIN[0])
    st.session_state.setdefault("pin_lng", DEFAULT_PIN[1])
    st.session_state.setdefault("results_df", pd.DataFrame())
    st.session_state.setdefault("last_run_done", False)


def render_pin_map(radius_km: float) -> None:
    fmap = folium.Map(
        location=[st.session_state["pin_lat"], st.session_state["pin_lng"]],
        zoom_start=12,
        tiles=None,
    )
    # Use English-labeled tiles to avoid Arabic map labels.
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Esri WorldStreetMap",
        overlay=False,
        control=False,
    ).add_to(fmap)
    folium.Marker(
        [st.session_state["pin_lat"], st.session_state["pin_lng"]],
        tooltip="Current pin",
    ).add_to(fmap)
    folium.Circle(
        [st.session_state["pin_lat"], st.session_state["pin_lng"]],
        radius=radius_km * 1000.0,
        color="#1D4ED8",
        fill=True,
        fill_opacity=0.12,
    ).add_to(fmap)

    st.caption("Click on the map to update the pin.")
    out = st_folium(fmap, width=1400, height=560)
    if out and out.get("last_clicked"):
        st.session_state["pin_lat"] = float(out["last_clicked"]["lat"])
        st.session_state["pin_lng"] = float(out["last_clicked"]["lng"])
        st.success(f"Pin updated: {st.session_state['pin_lat']:.6f}, {st.session_state['pin_lng']:.6f}")


def render_heatmap(df: pd.DataFrame) -> None:
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
        latitude=float(view_df["lat"].mean()),
        longitude=float(view_df["lng"].mean()),
        zoom=11,
        pitch=25,
    )
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state), use_container_width=True)


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
        st.caption(f"Backend: `{api_base_url}`")
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
                    provider = payload.get("provider", "unknown")
                    st.success(f"Pin set from search ({provider}): {result.get('formatted_address', geocode_query)}")
                else:
                    st.warning(f"No geocoding result. Provider response: {payload.get('error', 'no details')}")
            except Exception as exc:
                st.error(f"Geocode failed via backend: {exc}")

        radius_km = st.number_input("Radius (km)", min_value=1.0, max_value=30.0, value=10.0, step=0.5)
        status_filter = st.radio("Status filter", ["all", "live", "closed"], horizontal=True)
        just_landed_only = st.checkbox("Just Landed only", value=False)

    # Main map section (full width)
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

    if run:
        progress = st.progress(0.0)
        status_box = st.empty()

        with st.spinner("Scraping..."):
            payload = {
                "pin_lat": float(st.session_state["pin_lat"]),
                "pin_lng": float(st.session_state["pin_lng"]),
                "radius_km": float(radius_km),
                "status_filter": status_filter,
                "just_landed_only": bool(just_landed_only),
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

    df = st.session_state.get("results_df", pd.DataFrame())
    if df is None or df.empty:
        if st.session_state.get("last_run_done"):
            st.warning(
                "Run completed but no rows were captured. Try radius 10 km, spacing 1.5-2.0 km, "
                "status=all, and Just Landed off."
            )
            st.caption(
                "If still empty, Talabat may have changed page structure or blocked automated listing reads."
            )
        else:
            st.info("No results yet. Set pin and click Start Scraping.")
        return

    st.success(f"Collected {len(df):,} unique branch records.")
    m1, m2 = st.columns(2)
    m1.metric("Total branches", int(len(df)))
    m2.metric("Live", int((df["status"] == "live").sum()))

    st.dataframe(df, use_container_width=True, height=420)
    render_heatmap(df)

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
