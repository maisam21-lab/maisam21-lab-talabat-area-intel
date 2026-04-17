"""Optional reverse-geocode (OpenStreetMap Nominatim) to fill neighbourhood / address text."""

from __future__ import annotations

import os
import time

import requests

from models import RestaurantRecord

REVERSE_URL = "https://nominatim.openstreetmap.org/reverse"
_DEFAULT_UA = "TalabatAreaIntel/1.0 (+https://github.com/maisam21-lab/maisam21-lab-talabat-area-intel)"


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def enrich_records_reverse_geocode(records: list[RestaurantRecord]) -> None:
    if not records or not _truthy(os.getenv("ENRICH_NOMINATIM_REVERSE")):
        return
    max_n = int(os.getenv("NOMINATIM_REVERSE_MAX", "40"))
    ua = (os.getenv("NOMINATIM_USER_AGENT") or "").strip() or _DEFAULT_UA
    headers = {"User-Agent": ua, "Accept-Language": "en"}
    session = requests.Session()
    done = 0
    for row in records:
        if done >= max_n:
            break
        if (row.reverse_geocode_address or "").strip():
            continue
        try:
            lat, lng = float(row.lat), float(row.lng)
        except (TypeError, ValueError):
            continue
        try:
            r = session.get(
                REVERSE_URL,
                params={
                    "lat": lat,
                    "lon": lng,
                    "format": "json",
                    "zoom": 18,
                    "addressdetails": 1,
                    "accept-language": "en",
                },
                headers=headers,
                timeout=12,
            )
            r.raise_for_status()
            data = r.json()
        except (requests.RequestException, ValueError):
            time.sleep(1.1)
            continue
        disp = str(data.get("display_name") or "").strip()
        if disp:
            row.reverse_geocode_address = disp[:500]
            addr = data.get("address") or {}
            if isinstance(addr, dict) and not (row.area_label or "").strip():
                neighbourhood = (
                    addr.get("neighbourhood")
                    or addr.get("suburb")
                    or addr.get("quarter")
                    or addr.get("city_district")
                    or addr.get("district")
                    or addr.get("hamlet")
                    or addr.get("village")
                )
                if neighbourhood:
                    row.area_label = str(neighbourhood).strip()[:200]
        done += 1
        time.sleep(1.05)
