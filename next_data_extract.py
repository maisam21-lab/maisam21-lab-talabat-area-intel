"""Extract restaurant URLs from Talabat Next.js __NEXT_DATA__ when DOM links are sparse."""

from __future__ import annotations

import json
import re
from typing import Any

from talabat_urls import UAE_VENDOR_URL_RE, canonical_uae_vendor_url, is_vendor_slug

# Legacy paths (some regions still use /restaurant/).
_REST_PATH_RE = re.compile(
    r"(?:https://(?:www\.)?talabat\.com)?(/uae/restaurant/[^\"'\\s<>]+|/restaurant/[^\"'\\s<>]+)",
    re.IGNORECASE,
)


def parse_next_data_script(text: str) -> dict[str, Any] | None:
    if not text or not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def collect_restaurant_paths_from_json(obj: Any, seen: set[str]) -> None:
    """Recursively find restaurant URLs inside arbitrary JSON."""
    if isinstance(obj, dict):
        for v in obj.values():
            collect_restaurant_paths_from_json(v, seen)
    elif isinstance(obj, list):
        for item in obj:
            collect_restaurant_paths_from_json(item, seen)
    elif isinstance(obj, str):
        for m in _REST_PATH_RE.finditer(obj):
            path = m.group(1).split("?")[0].rstrip("\\")
            if "/restaurant/" in path:
                seen.add(path)
        for m in UAE_VENDOR_URL_RE.finditer(obj):
            slug = m.group(1)
            if is_vendor_slug(slug):
                seen.add(canonical_uae_vendor_url(slug))


def paths_from_next_data_json(data: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    collect_restaurant_paths_from_json(data, seen)
    blob = json.dumps(data)
    for m in _REST_PATH_RE.finditer(blob):
        path = m.group(1).split("?")[0]
        if "/restaurant/" in path:
            seen.add(path)
    for m in UAE_VENDOR_URL_RE.finditer(blob):
        slug = m.group(1)
        if is_vendor_slug(slug):
            seen.add(canonical_uae_vendor_url(slug))
    return sorted(seen)


def normalize_talabat_url(path_or_url: str) -> str:
    p = path_or_url.strip()
    if p.startswith("http"):
        return p.split("?")[0]
    if not p.startswith("/"):
        p = "/" + p
    return f"https://www.talabat.com{p.split('?')[0]}"
