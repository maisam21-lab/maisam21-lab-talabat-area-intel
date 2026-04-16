from __future__ import annotations

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

import folium
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
from streamlit_folium import st_folium

from scrape_engine import run_area_scrape

DEFAULT_PIN = (25.2048, 55.2708)


def init_state() -> None:
    st.session_state.setdefault("pin_lat", DEFAULT_PIN[0])
    st.session_state.setdefault("pin_lng", DEFAULT_PIN[1])
    st.session_state.setdefault("results_df", pd.DataFrame())
    st.session_state.setdefault("last_run_done", False)


def run_async_safely(coro):
    """Run async Playwright code safely inside Streamlit on Windows."""

    def _runner():
        if sys.platform.startswith("win"):
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except Exception:
                pass
        return asyncio.run(coro)

    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(_runner).result()


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
    out = st_folium(fmap, width=760, height=430)
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


def main() -> None:
    st.set_page_config(page_title="Talabat Area Intel", layout="wide")
    init_state()

    st.title("Talabat UAE Area Intel")
    st.caption("Visual map pin + radius scraping + new branch tracking + heatmap.")

    with st.sidebar:
        st.header("Scrape Controls")
        run_mode = st.selectbox("Run mode", ["Local Playwright", "Remote API (Render)"])
        api_base_url = st.text_input("API base URL", value="http://127.0.0.1:8000")
        radius_choice = st.selectbox("Radius", ["5 km", "10 km", "Custom"])
        radius_km = 5.0 if radius_choice == "5 km" else 10.0 if radius_choice == "10 km" else st.number_input(
            "Custom radius (km)", min_value=1.0, max_value=30.0, value=7.0, step=0.5
        )
        spacing_km = st.slider("Sample spacing (km)", min_value=0.5, max_value=3.0, value=1.0, step=0.1)
        concurrency = st.slider("Concurrency", min_value=1, max_value=6, value=3)
        status_filter = st.selectbox("Status filter", ["all", "live", "closed"])
        just_landed_only = st.checkbox("Just Landed only", value=False)
        scroll_rounds = st.slider("Max scroll rounds", min_value=6, max_value=50, value=22)
        scroll_wait_ms = st.slider("Scroll wait ms", min_value=600, max_value=2500, value=1300, step=100)

        st.markdown("---")
        st.write("Manual pin override")
        st.session_state["pin_lat"] = st.number_input("Pin lat", value=float(st.session_state["pin_lat"]), format="%.6f")
        st.session_state["pin_lng"] = st.number_input("Pin lng", value=float(st.session_state["pin_lng"]), format="%.6f")

    col_left, col_right = st.columns([1.25, 1.0])
    with col_left:
        render_pin_map(radius_km=radius_km)
    with col_right:
        st.subheader("Run")
        st.write(f"Current pin: `{st.session_state['pin_lat']:.6f}, {st.session_state['pin_lng']:.6f}`")
        st.write(f"Radius: `{radius_km} km`")
        prev_csv = st.file_uploader("Previous scrape CSV (optional)", type=["csv"])
        run = st.button("Start Scraping", type="primary", use_container_width=True)

    if run:
        progress = st.progress(0.0)
        status_box = st.empty()
        log_box = st.empty()

        def progress_cb(done: int, total: int, lat: float, lng: float, rows: int) -> None:
            progress.progress(done / total)
            status_box.info(f"Processed {done}/{total} points")
            log_box.write(f"Last point ({lat:.5f}, {lng:.5f}) -> {rows} rows")

        with st.spinner("Scraping..."):
            if run_mode == "Local Playwright":
                df = run_async_safely(
                    run_area_scrape(
                        pin_lat=float(st.session_state["pin_lat"]),
                        pin_lng=float(st.session_state["pin_lng"]),
                        radius_km=float(radius_km),
                        spacing_km=float(spacing_km),
                        concurrency=int(concurrency),
                        status_filter=status_filter,
                        just_landed_only=just_landed_only,
                        scroll_rounds=int(scroll_rounds),
                        scroll_wait_ms=int(scroll_wait_ms),
                        progress_cb=progress_cb,
                    )
                )
            else:
                payload = {
                    "pin_lat": float(st.session_state["pin_lat"]),
                    "pin_lng": float(st.session_state["pin_lng"]),
                    "radius_km": float(radius_km),
                    "spacing_km": float(spacing_km),
                    "concurrency": int(concurrency),
                    "status_filter": status_filter,
                    "just_landed_only": bool(just_landed_only),
                    "scroll_rounds": int(scroll_rounds),
                    "scroll_wait_ms": int(scroll_wait_ms),
                }
                try:
                    response = requests.post(
                        f"{api_base_url.rstrip('/')}/scrape",
                        json=payload,
                        timeout=240,
                    )
                    response.raise_for_status()
                    api_data = response.json()
                    df = pd.DataFrame(api_data.get("records", []))
                    progress.progress(1.0)
                    status_box.info("Remote scrape completed")
                except Exception as exc:
                    st.error(f"Remote API scrape failed: {exc}")
                    df = pd.DataFrame()

        if not df.empty:
            df["is_new_since_last_scrape"] = False
            if prev_csv is not None:
                try:
                    prev_df = pd.read_csv(prev_csv)
                    if "branch_sku" in prev_df.columns:
                        prev_set = set(prev_df["branch_sku"].dropna().astype(str))
                        df["is_new_since_last_scrape"] = ~df["branch_sku"].astype(str).isin(prev_set)
                except Exception:
                    pass
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
    m1, m2, m3 = st.columns(3)
    m1.metric("Total branches", int(len(df)))
    m2.metric("Live", int((df["status"] == "live").sum()))
    m3.metric("New vs previous", int(df.get("is_new_since_last_scrape", pd.Series(dtype=bool)).sum()))

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
