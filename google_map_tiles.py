"""Google Map Tiles API: session tokens for Folium / Leaflet XYZ layers."""

from __future__ import annotations

import time
from typing import Any, MutableMapping
from urllib.parse import quote

import requests

_CREATE_SESSION = "https://tile.googleapis.com/v1/createSession"


def _session_stale(entry: Any, now: float, renew_skew_sec: float = 600.0) -> bool:
    if not isinstance(entry, dict):
        return True
    sess = entry.get("session")
    exp = entry.get("expiry")
    if not sess or exp is None:
        return True
    try:
        exp_f = float(exp)
    except (TypeError, ValueError):
        return True
    return now >= exp_f - renew_skew_sec


def ensure_google_map_tile_sessions(
    api_key: str,
    cache: MutableMapping[str, Any],
    *,
    language: str = "en-US",
    region: str = "AE",
    timeout: float = 15.0,
) -> tuple[str | None, str | None]:
    """
    Return ``(roadmap_session, satellite_hybrid_session)`` for 2D tile URLs.

    Caches by map theme in ``cache`` under keys ``roadmap`` and ``satellite_hybrid``
    (each value: ``{"session": str, "expiry": float}``). Refreshes when near expiry.
    """
    if not api_key.strip():
        return None, None

    now = time.time()
    rm = cache.get("roadmap")
    sh = cache.get("satellite_hybrid")
    if not _session_stale(rm, now) and not _session_stale(sh, now):
        return str(rm["session"]), str(sh["session"])

    lang = (language or "en-US").strip() or "en-US"
    reg = (region or "AE").strip().upper()[:2] or "AE"

    def _post(body: dict[str, Any]) -> dict[str, Any]:
        r = requests.post(
            f"{_CREATE_SESSION}?key={quote(api_key, safe='')}",
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json()

    try:
        j_rm = _post({"mapType": "roadmap", "language": lang, "region": reg})
        j_sh = _post(
            {
                "mapType": "satellite",
                "language": lang,
                "region": reg,
                "layerTypes": ["layerRoadmap"],
                "overlay": False,
            }
        )
    except (requests.RequestException, ValueError, KeyError):
        cache.pop("roadmap", None)
        cache.pop("satellite_hybrid", None)
        return None, None

    def _pack(resp: dict[str, Any]) -> dict[str, Any]:
        return {
            "session": str(resp["session"]),
            "expiry": float(resp["expiry"]),
        }

    cache["roadmap"] = _pack(j_rm)
    cache["satellite_hybrid"] = _pack(j_sh)
    return str(cache["roadmap"]["session"]), str(cache["satellite_hybrid"]["session"])


def google_2d_tile_url_template(api_key: str, session_token: str) -> str:
    """XYZ template for Leaflet; ``{z}``, ``{x}``, ``{y}`` are substituted by Folium."""
    return (
        "https://tile.googleapis.com/v1/2dtiles/{z}/{x}/{y}"
        f"?session={quote(session_token, safe='')}&key={quote(api_key, safe='')}"
    )


def google_maps_tile_attribution() -> str:
    return 'Map data © <a href="https://www.google.com/maps">Google</a>'
