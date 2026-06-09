"""
Scrape Talabat restaurant detail pages for contact info.

Talabat uses Next.js — every page embeds all data as JSON in <script id="__NEXT_DATA__">.
No Playwright needed, pure HTTP GET.

Extracts: phone, WhatsApp, address, description, opening hours, cuisines.
Results are disk-cached (same path structure as website_scrape cache).
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

_CACHE_PATH = Path(os.getenv("TALABAT_VENDOR_CACHE_PATH", "/app/analyze_jobs/talabat_vendor_cache.json"))

_BASE = "https://www.talabat.com"
_NEXT_DATA_RE = re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.DOTALL)

_UAE_MOBILE_RE = re.compile(
    r'(?<!\d)(?:'
    r'\+971\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|00971\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|0\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r')(?!\d)'
)
_UAE_PHONE_RE = re.compile(
    r'(?<!\d)(?:'
    r'\+971[\s\-\.]?[2-9]\d[\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|00971[\s\-\.]?[2-9]\d[\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|0[2-9]\d[\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r')(?!\d)'
)
_WHATSAPP_RE = re.compile(
    r'(?:wa\.me|api\.whatsapp\.com/send[?]phone=|whatsapp\.com/send[?]phone=)'
    r'[/\?]?(?:phone=)?(\+?[0-9]{7,15})',
    re.IGNORECASE
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
}


def _load_cache() -> dict:
    try:
        if _CACHE_PATH.exists():
            return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_cache(cache: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass


def _normalise_phone(p: str) -> str:
    digits = re.sub(r"[\s\-\.\(\)]", "", p).strip()
    if digits.startswith("00971"):
        digits = "+" + digits[2:]
    if digits.startswith("971") and not digits.startswith("+"):
        digits = "+" + digits
    return digits


def _dig(obj, *keys, default=None):
    """Safe nested dict access."""
    for k in keys:
        if isinstance(obj, dict):
            obj = obj.get(k)
        elif isinstance(obj, list) and isinstance(k, int) and k < len(obj):
            obj = obj[k]
        else:
            return default
        if obj is None:
            return default
    return obj if obj is not None else default


def _extract_from_next_data(data: dict) -> dict:
    """Pull contacts from Talabat __NEXT_DATA__ JSON."""
    result: dict = {}

    # Navigate to vendor data — structure varies by page type
    # Try: props.pageProps.vendor or props.pageProps.data.vendor
    vendor = (
        _dig(data, "props", "pageProps", "vendor")
        or _dig(data, "props", "pageProps", "data", "vendor")
        or _dig(data, "props", "pageProps", "initialData", "vendor")
        or {}
    )

    # Phone — Talabat stores as vendor.phones or vendor.contactPhone
    phones = vendor.get("phones") or []
    if isinstance(phones, list) and phones:
        for p in phones:
            if isinstance(p, dict):
                num = (p.get("number") or p.get("phone") or "").strip()
            else:
                num = str(p).strip()
            if num:
                result["talabat_phone"] = _normalise_phone(num)
                break
    if not result.get("talabat_phone"):
        cp = (vendor.get("contactPhone") or vendor.get("phone") or "").strip()
        if cp:
            result["talabat_phone"] = _normalise_phone(cp)

    # WhatsApp
    wa = (vendor.get("whatsapp") or vendor.get("whatsappNumber") or "").strip()
    if wa:
        result["talabat_whatsapp"] = _normalise_phone(wa)

    # Address
    addr_obj = vendor.get("address") or {}
    if isinstance(addr_obj, dict):
        addr_parts = [
            str(addr_obj.get(k) or "").strip()
            for k in ("building", "street", "area", "city")
            if (addr_obj.get(k) or "").strip()
        ]
        if addr_parts:
            result["talabat_address"] = ", ".join(addr_parts)
    elif isinstance(addr_obj, str) and addr_obj.strip():
        result["talabat_address"] = addr_obj.strip()

    # Description
    desc = (vendor.get("description") or vendor.get("about") or "").strip()
    if desc:
        result["talabat_description"] = desc[:500]

    return result


def scrape_talabat_vendor(branch_url: str, *, session=None) -> dict:
    """
    Scrape a Talabat restaurant page and return contacts dict.
    branch_url: full URL like https://www.talabat.com/uae/restaurant/slug/12345
                or relative like /uae/restaurant/slug/12345
    """
    empty = {k: "" for k in ("talabat_phone", "talabat_whatsapp", "talabat_address", "talabat_description")}

    if not branch_url:
        return empty

    url = branch_url if branch_url.startswith("http") else f"{_BASE}{branch_url}"
    sess = session or requests.Session()

    try:
        r = sess.get(url, headers=_HEADERS, timeout=14, allow_redirects=True)
        r.raise_for_status()
        html = r.text
    except Exception:
        return empty

    # 1. Try __NEXT_DATA__ JSON (most reliable — contains structured vendor data)
    m = _NEXT_DATA_RE.search(html)
    if m:
        try:
            nd = json.loads(m.group(1))
            result = _extract_from_next_data(nd)
            if any(result.values()):
                # Also scan raw HTML for phones as supplement
                _add_phones_from_html(html, result)
                return {**empty, **result}
        except (json.JSONDecodeError, Exception):
            pass

    # 2. Fallback: regex-scan raw HTML for UAE phones and WhatsApp links
    result = {}
    _add_phones_from_html(html, result)
    return {**empty, **result}


def _add_phones_from_html(html: str, result: dict) -> None:
    """Supplement result dict with phones found in raw HTML."""
    mobiles = [_normalise_phone(m) for m in _UAE_MOBILE_RE.findall(html)]
    phones = [_normalise_phone(p) for p in _UAE_PHONE_RE.findall(html)]
    all_phones = list(dict.fromkeys(mobiles + [p for p in phones if p not in mobiles]))

    if all_phones and not result.get("talabat_phone"):
        result["talabat_phone"] = all_phones[0]

    # WhatsApp from links
    for match in _WHATSAPP_RE.finditer(html):
        num = _normalise_phone(match.group(1))
        if num and not result.get("talabat_whatsapp"):
            result["talabat_whatsapp"] = num
            break


def enrich_df_with_talabat_contacts(df, *, max_pages: int = 2000) -> None:
    """
    Add talabat_phone, talabat_whatsapp, talabat_address columns to df in-place.
    Scrapes each restaurant's own Talabat page.
    Deduplicates at restaurant_id level (one scrape per brand, not per branch).
    """
    NEW_COLS = ["talabat_phone", "talabat_whatsapp", "talabat_address", "talabat_description"]
    for col in NEW_COLS:
        if col not in df.columns:
            df[col] = ""

    # Build URL from branch_url or restaurant_slug + branch_id
    if "branch_url" not in df.columns and "restaurant_slug" not in df.columns:
        return

    cache = _load_cache()
    session = requests.Session()
    done = 0
    seen_rids: dict = {}  # restaurant_id → contacts (dedup same brand across branches)

    for idx in df.index:
        rid = df.at[idx, "restaurant_id"] if "restaurant_id" in df.columns else None

        # Dedup: same brand already seen this run
        if rid is not None and rid in seen_rids:
            contacts = seen_rids[rid]
            _apply_contacts(df, idx, contacts, NEW_COLS)
            continue

        # Disk cache hit
        cache_key = str(rid) if rid is not None else None
        if cache_key and cache_key in cache:
            contacts = cache[cache_key]
            seen_rids[rid] = contacts
            _apply_contacts(df, idx, contacts, NEW_COLS)
            continue

        if done >= max_pages:
            continue

        # Build URL
        burl = str(df.at[idx, "branch_url"] if "branch_url" in df.columns else "").strip()
        if not burl:
            rslug = str(df.at[idx, "restaurant_slug"] if "restaurant_slug" in df.columns else "").strip()
            bid = df.at[idx, "branch_id"] if "branch_id" in df.columns else None
            if rslug and bid:
                burl = f"{_BASE}/uae/restaurant/{rslug}/{bid}"
        if not burl:
            continue

        time.sleep(0.4)
        contacts = scrape_talabat_vendor(burl, session=session)
        seen_rids[rid] = contacts
        if cache_key:
            cache[cache_key] = contacts
            if done % 50 == 0:
                _save_cache(cache)

        _apply_contacts(df, idx, contacts, NEW_COLS)
        done += 1

    _save_cache(cache)


def _apply_contacts(df, idx, contacts: dict, cols: list) -> None:
    for col in cols:
        val = contacts.get(col, "")
        if val and not str(df.at[idx, col]).strip():
            df.at[idx, col] = val
