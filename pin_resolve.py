"""Resolve a human-readable area label for a pin (OpenStreetMap Nominatim reverse)."""

from __future__ import annotations

import os

import requests

_REVERSE = "https://nominatim.openstreetmap.org/reverse"
_DEFAULT_UA = "TalabatAreaIntel/1.0 (+https://github.com/maisam21-lab/maisam21-lab-talabat-area-intel)"


def resolve_pin_area_label(lat: float, lng: float) -> str:
    """Neighbourhood / suburb / city string for logging and API meta; empty if disabled or on failure."""
    if os.getenv("RESOLVE_PIN_AREA", "1").strip().lower() in ("0", "false", "no", "off"):
        return ""
    ua = (os.getenv("NOMINATIM_USER_AGENT") or "").strip() or _DEFAULT_UA
    try:
        r = requests.get(
            _REVERSE,
            params={
                "lat": lat,
                "lon": lng,
                "format": "json",
                "zoom": 14,
                "addressdetails": 1,
                "accept-language": "en",
            },
            headers={"User-Agent": ua, "Accept-Language": "en"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError, TypeError):
        return ""
    addr = data.get("address") or {}
    if isinstance(addr, dict):
        for k in ("neighbourhood", "suburb", "quarter", "city_district", "district", "village", "town", "city"):
            v = addr.get(k)
            if v:
                return str(v).strip()[:200]
    disp = str(data.get("display_name") or "").strip()
    return disp[:240] if disp else ""
