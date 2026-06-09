"""
Scrape Instagram profile bios for UAE phone numbers and WhatsApp links.
Many UAE restaurants put their WhatsApp/phone directly in their Instagram bio.

Uses public Instagram profile pages (no login, no API key needed).
Results cached to disk.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

_CACHE_PATH = Path(os.getenv("INSTAGRAM_BIO_CACHE_PATH", "/app/analyze_jobs/instagram_bio_cache.json"))

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
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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


def _extract_handle(ig_field: str) -> str:
    """Extract bare handle from 'instagram.com/handle' or '@handle' or 'handle'."""
    s = str(ig_field or "").strip()
    if "instagram.com/" in s:
        s = s.split("instagram.com/")[-1]
    s = s.lstrip("@").strip("/").split("?")[0].split("/")[0]
    return s.lower()


def scrape_instagram_bio(handle: str, *, session=None) -> dict:
    """
    Scrape public Instagram profile and return contacts found in bio.
    Returns dict with: ig_bio_mobile, ig_bio_phone, ig_bio_whatsapp, ig_bio_text
    """
    empty = {"ig_bio_mobile": "", "ig_bio_phone": "", "ig_bio_whatsapp": "", "ig_bio_text": ""}
    handle = _extract_handle(handle)
    if not handle or len(handle) < 2:
        return empty

    url = f"https://www.instagram.com/{handle}/"
    sess = session or requests.Session()

    try:
        r = sess.get(url, headers=_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code == 404:
            return empty
        r.raise_for_status()
        html = r.text
    except Exception:
        return empty

    # Try to extract bio from JSON embedded in page
    bio_text = ""

    # Method 1: look for shared_data JSON (old Instagram)
    m = re.search(r'window\._sharedData\s*=\s*({.*?});</script>', html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            user = (
                data.get("entry_data", {})
                    .get("ProfilePage", [{}])[0]
                    .get("graphql", {})
                    .get("user", {})
            )
            bio_text = user.get("biography", "")
        except Exception:
            pass

    # Method 2: look for meta description (always has bio snippet)
    if not bio_text:
        soup = BeautifulSoup(html, "html.parser")
        meta = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"})
        if meta:
            bio_text = meta.get("content", "")

    # Method 3: search raw HTML for phone patterns regardless
    full_text = bio_text + " " + html

    mobiles = [_normalise_phone(m) for m in _UAE_MOBILE_RE.findall(full_text)]
    phones = [_normalise_phone(p) for p in _UAE_PHONE_RE.findall(full_text)]
    all_phones = list(dict.fromkeys(mobiles + [p for p in phones if p not in mobiles]))

    wa_numbers = []
    for match in _WHATSAPP_RE.finditer(full_text):
        num = _normalise_phone(match.group(1))
        if num and num not in wa_numbers:
            wa_numbers.append(num)

    return {
        "ig_bio_mobile": mobiles[0] if mobiles else "",
        "ig_bio_phone": all_phones[0] if all_phones else "",
        "ig_bio_whatsapp": wa_numbers[0] if wa_numbers else "",
        "ig_bio_text": bio_text[:300] if bio_text else "",
    }


def enrich_df_with_instagram_bios(df, *, max_profiles: int = 500) -> None:
    """
    Scrape Instagram bios for phone numbers and add columns to df in-place.
    Uses website_instagram column (populated by website_scrape.py).
    """
    NEW_COLS = ["ig_bio_mobile", "ig_bio_phone", "ig_bio_whatsapp", "ig_bio_text"]
    for col in NEW_COLS:
        if col not in df.columns:
            df[col] = ""

    ig_col = None
    for candidate in ("website_instagram", "instagram"):
        if candidate in df.columns:
            ig_col = candidate
            break
    if ig_col is None:
        return

    cache = _load_cache()
    session = requests.Session()
    done = 0
    seen: dict = {}

    for idx in df.index:
        raw_ig = str(df.at[idx, ig_col] or "").strip()
        if not raw_ig:
            continue

        handle = _extract_handle(raw_ig)
        if not handle:
            continue

        # Memory cache
        if handle in seen:
            contacts = seen[handle]
        # Disk cache
        elif handle in cache:
            contacts = cache[handle]
            seen[handle] = contacts
        # Scrape
        elif done < max_profiles:
            time.sleep(0.8)  # Instagram rate limit
            contacts = scrape_instagram_bio(handle, session=session)
            seen[handle] = contacts
            cache[handle] = contacts
            done += 1
            if done % 25 == 0:
                _save_cache(cache)
        else:
            continue

        for col in NEW_COLS:
            val = contacts.get(col, "")
            if val and not str(df.at[idx, col]).strip():
                df.at[idx, col] = val

    _save_cache(cache)
