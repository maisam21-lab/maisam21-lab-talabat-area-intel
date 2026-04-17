"""Single source of truth for the scrape pin in the Streamlit UI (map + manual + API payload)."""

from __future__ import annotations

from typing import Any

import streamlit as st

SCRAPE_LOCATION_KEY = "scrape_location"
# Last raw return from st_folium for mismatch diagnostics (center / bounds when available).
FOLIUM_LAST_KEY = "folium_last_map_payload"


def default_location(lat: float, lng: float, label: str) -> dict[str, Any]:
    return {
        "lat": float(lat),
        "lng": float(lng),
        "label": str(label)[:300],
        "source": "init",
    }


def ensure_scrape_location(
    *,
    default_lat: float,
    default_lng: float,
    default_label: str,
    migrate_from_legacy_keys: bool = True,
) -> dict[str, Any]:
    """Initialize ``scrape_location`` once; optionally migrate legacy ``pin_lat`` / ``pin_lng``."""
    if SCRAPE_LOCATION_KEY not in st.session_state:
        if migrate_from_legacy_keys and "pin_lat" in st.session_state and "pin_lng" in st.session_state:
            st.session_state[SCRAPE_LOCATION_KEY] = default_location(
                float(st.session_state["pin_lat"]),
                float(st.session_state["pin_lng"]),
                str(st.session_state.get("pin_label") or default_label),
            )
            st.session_state[SCRAPE_LOCATION_KEY]["source"] = "legacy_migrated"
        else:
            st.session_state[SCRAPE_LOCATION_KEY] = default_location(default_lat, default_lng, default_label)
    return st.session_state[SCRAPE_LOCATION_KEY]


def get_scrape_location() -> dict[str, Any]:
    if SCRAPE_LOCATION_KEY not in st.session_state:
        raise RuntimeError("scrape_location missing — call ensure_scrape_location() from init_state.")
    return st.session_state[SCRAPE_LOCATION_KEY]


def set_scrape_location(lat: float, lng: float, label: str, source: str) -> None:
    loc = get_scrape_location()
    loc["lat"] = float(lat)
    loc["lng"] = float(lng)
    loc["label"] = str(label)[:300]
    loc["source"] = str(source)[:40]


def seed_city_preset_if_changed(city_key: str, preset_lat: float, preset_lng: float, city_display: str) -> None:
    """When the user picks a different emirate, reset the pin to that preset centre."""
    prev = st.session_state.get("_scrape_city_seed_key")
    if prev != city_key:
        set_scrape_location(preset_lat, preset_lng, f"{city_display} (preset centre)", "city_preset")
        st.session_state["_scrape_city_seed_key"] = city_key


def sync_legacy_pin_mirror() -> None:
    """Keep deprecated keys aligned so old fingerprints / widgets that still read them do not drift."""
    loc = get_scrape_location()
    st.session_state["pin_lat"] = loc["lat"]
    st.session_state["pin_lng"] = loc["lng"]
    st.session_state["pin_label"] = loc["label"]


def store_folium_payload(payload: dict[str, Any] | None) -> None:
    st.session_state[FOLIUM_LAST_KEY] = dict(payload or {})


def get_folium_payload() -> dict[str, Any]:
    return dict(st.session_state.get(FOLIUM_LAST_KEY) or {})


def folium_center_vs_location_mismatch(loc: dict[str, Any], tol_deg: float = 0.002) -> tuple[bool, str]:
    """True if Folium viewport center differs materially from the authoritative pin."""
    pay = get_folium_payload()
    c = pay.get("center") or {}
    try:
        clat = float(c.get("lat"))
        clng = float(c.get("lng"))
    except (TypeError, ValueError):
        return False, ""
    dlat = abs(float(loc["lat"]) - clat)
    dlng = abs(float(loc["lng"]) - clng)
    if dlat > tol_deg or dlng > tol_deg:
        return True, (
            f"Map viewport center ({clat:.5f}, {clng:.5f}) differs from run pin "
            f"({float(loc['lat']):.5f}, {float(loc['lng']):.5f}). Pan/zoom then click the map to sync, "
            f"or adjust **Run pin** lat/lng (section above this map)."
        )
    return False, ""
