"""UAE city centers for Talabat area scrapes (KitchenPark / cloud-kitchen expansion analytics).

Each preset: (center_lat, center_lng, default_radius_km). Radius is a starting point; callers may override.
"""

from __future__ import annotations

# Normalized keys: dubai, sharjah, abudhabi, alain, ajman
UAE_CITY_PRESETS: dict[str, tuple[float, float, float]] = {
    "dubai": (25.2048, 55.2708, 18.0),
    "sharjah": (25.3463, 55.4209, 15.0),
    "abudhabi": (24.4539, 54.3773, 22.0),
    "alain": (24.2075, 55.7447, 16.0),
    "ajman": (25.4052, 55.5136, 12.0),
}

UAE_CITY_DISPLAY: dict[str, str] = {
    "dubai": "Dubai",
    "sharjah": "Sharjah",
    "abudhabi": "Abu Dhabi",
    "alain": "Al Ain",
    "ajman": "Ajman",
}

_ALIAS_TO_KEY: dict[str, str] = {
    "dubai": "dubai",
    "sharjah": "sharjah",
    "abudhabi": "abudhabi",
    "abu dhabi": "abudhabi",
    "abu_dhabi": "abudhabi",
    "alain": "alain",
    "al ain": "alain",
    "al_ain": "alain",
    "ajman": "ajman",
}


def normalize_city_key(city: str | None) -> str | None:
    if not city or not str(city).strip():
        return None
    s = str(city).strip().lower().replace("-", " ")
    s = " ".join(s.split())
    if s in _ALIAS_TO_KEY:
        return _ALIAS_TO_KEY[s]
    s_compact = s.replace(" ", "")
    for alias, key in _ALIAS_TO_KEY.items():
        if alias.replace(" ", "") == s_compact:
            return key
    return None


def resolve_city(city: str | None) -> tuple[float, float, float, str] | None:
    """Return (lat, lng, default_radius_km, display_label) or None if unknown."""
    key = normalize_city_key(city)
    if not key or key not in UAE_CITY_PRESETS:
        return None
    lat, lng, r = UAE_CITY_PRESETS[key]
    return lat, lng, r, UAE_CITY_DISPLAY.get(key, key.title())
