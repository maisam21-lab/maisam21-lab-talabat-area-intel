"""Extra signals from vendor HTML: mailto, meta tags, JSON-LD (Restaurant / LocalBusiness)."""

from __future__ import annotations

import json
import re
from html import unescape
from typing import Any


_META_CONTENT = re.compile(
    r'<meta\s[^>]*(?:name|property)=["\']([^"\']+)["\'][^>]*content=["\']([^"\']*)["\']',
    re.I,
)
_META_CONTENT_ALT = re.compile(
    r'<meta\s[^>]*content=["\']([^"\']*)["\'][^>]*(?:name|property)=["\']([^"\']+)["\']',
    re.I,
)
_MAILTO_RE = re.compile(r"""href=["']mailto:([^"'>\s?]+)""", re.I)
_CANONICAL_RE = re.compile(r"""<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']""", re.I)
_LD_JSON_BLOCK = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)


def _append_unique(acc: dict[str, list], key: str, val: str, cap: int = 500) -> None:
    v = (val or "").strip()
    if len(v) < 3:
        return
    v = v[:cap]
    acc.setdefault(key, []).append(v)


def _iter_ld_objects(blob: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(blob, dict):
        if "@graph" in blob and isinstance(blob["@graph"], list):
            for it in blob["@graph"]:
                if isinstance(it, dict):
                    out.append(it)
        else:
            out.append(blob)
    elif isinstance(blob, list):
        for it in blob:
            if isinstance(it, dict):
                out.append(it)
    return out


def _ld_is_placeish(d: dict[str, Any]) -> bool:
    t = d.get("@type")
    types = []
    if isinstance(t, str):
        types = [t]
    elif isinstance(t, list):
        types = [str(x) for x in t if x]
    good = {"Restaurant", "FoodEstablishment", "LocalBusiness", "Organization"}
    return any(x in good for x in types)


def _harvest_ld_node(d: dict[str, Any], acc: dict[str, list]) -> None:
    if not _ld_is_placeish(d):
        return
    for key in ("description", "slogan", "abstract"):
        v = d.get(key)
        if isinstance(v, str) and len(v.strip()) > 20:
            _append_unique(acc, "descriptions", unescape(v.strip())[:1200])
    url = d.get("url")
    if isinstance(url, str) and url.startswith("http"):
        _append_unique(acc, "external_websites", url)
    em = d.get("email")
    if isinstance(em, str) and "@" in em:
        _append_unique(acc, "emails", em[:200])
    same = d.get("sameAs")
    if isinstance(same, str) and same.startswith("http"):
        _append_unique(acc, "social_urls", same)
    elif isinstance(same, list):
        for s in same:
            if isinstance(s, str) and s.startswith("http"):
                _append_unique(acc, "social_urls", s)
    oh = d.get("openingHours") or d.get("openinghours")
    if isinstance(oh, str) and len(oh) > 5:
        _append_unique(acc, "opening_hours_snippets", oh[:600])
    elif isinstance(oh, list):
        joined = " | ".join(str(x) for x in oh if x)[:600]
        if joined:
            _append_unique(acc, "opening_hours_snippets", joined)
    addr = d.get("address")
    if isinstance(addr, dict):
        parts = [addr.get(k) for k in ("streetAddress", "addressLocality", "addressRegion") if addr.get(k)]
        blob = ", ".join(str(p) for p in parts if p)
        if len(blob) > 5:
            _append_unique(acc, "address_lines", blob[:400])


def merge_html_into_accumulator(html: str, acc: dict[str, list]) -> None:
    """Parse vendor HTML into the same accumulator shape used by __NEXT_DATA__ mining."""
    if not html:
        return
    for m in _MAILTO_RE.finditer(html):
        _append_unique(acc, "emails", unescape(m.group(1).strip())[:200])
    for m in _CANONICAL_RE.finditer(html):
        u = unescape(m.group(1).strip())
        if u.startswith("http") and "talabat.com" in u.lower():
            _append_unique(acc, "canonical_urls", u[:400])

    for m in _META_CONTENT.finditer(html):
        name, content = m.group(1).lower(), m.group(2)
        if "og:description" in name or name == "description":
            txt = unescape(content.strip())
            if len(txt) > 25:
                _append_unique(acc, "descriptions", txt[:1200])
        if name in ("og:url", "twitter:url") and content.strip().startswith("http"):
            _append_unique(acc, "external_websites", unescape(content.strip())[:400])
    for m in _META_CONTENT_ALT.finditer(html):
        content, name = m.group(1), m.group(2).lower()
        if "og:description" in name or name == "description":
            txt = unescape(content.strip())
            if len(txt) > 25:
                _append_unique(acc, "descriptions", txt[:1200])
        if name in ("og:url", "twitter:url") and content.strip().startswith("http"):
            _append_unique(acc, "external_websites", unescape(content.strip())[:400])

    for m in _LD_JSON_BLOCK.finditer(html):
        raw = m.group(1).strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for node in _iter_ld_objects(data):
            if isinstance(node, dict):
                _harvest_ld_node(node, acc)

    # Obvious social hrefs in raw HTML (fallback)
    for plat in ("instagram.com", "facebook.com", "twitter.com", "x.com", "tiktok.com"):
        for m in re.finditer(rf'https?://(?:www\.)?{re.escape(plat)}/[^"\'\s<>]+', html, re.I):
            _append_unique(acc, "social_urls", m.group(0)[:280])
