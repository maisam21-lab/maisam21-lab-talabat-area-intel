"""
Scrape restaurant websites for contact details: phone, email, WhatsApp, Instagram, Facebook.
Runs after Google Places enrichment (which provides the website URL).
Results are disk-cached so websites are only scraped once.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

_CACHE_PATH = Path(os.getenv("WEBSITE_SCRAPE_CACHE_PATH", "/app/analyze_jobs/website_scrape_cache.json"))

# UAE phone patterns
_UAE_MOBILE_RE = re.compile(
    r'(?<!\d)(?:'
    r'\+971\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'   # +971 5X XXX XXXX
    r'|00971\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'  # 00971 5X XXX XXXX
    r'|0\s*5[0-9][\s\-\.]?\d{3}[\s\-\.]?\d{4}'      # 05X XXX XXXX
    r')(?!\d)'
)
_UAE_PHONE_RE = re.compile(
    r'(?<!\d)(?:'
    r'\+971[\s\-\.]?[2-9]\d[\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|00971[\s\-\.]?[2-9]\d[\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r'|0[2-9]\d[\s\-\.]?\d{3}[\s\-\.]?\d{4}'
    r')(?!\d)'
)
_EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
_WHATSAPP_RE = re.compile(
    r'(?:wa\.me|api\.whatsapp\.com/send[?]phone=|whatsapp\.com/send[?]phone=)'
    r'[/\?]?(?:phone=)?(\+?[0-9]{7,15})',
    re.IGNORECASE
)
_INSTAGRAM_RE = re.compile(r'instagram\.com/([A-Za-z0-9_.]{1,30})/?', re.IGNORECASE)
_FACEBOOK_RE = re.compile(r'(?:facebook\.com|fb\.com)/([A-Za-z0-9_.%-]{3,60})/?', re.IGNORECASE)
_TIKTOK_RE = re.compile(r'tiktok\.com/@([A-Za-z0-9_.]{2,40})/?', re.IGNORECASE)

_SKIP_IG = {"p", "reel", "stories", "explore", "accounts", "share", "sharer", "hashtag"}
_SKIP_FB = {"sharer", "share", "photo", "video", "groups", "events", "pages", "watch", "hashtag", "profile.php"}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
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
    """Strip spaces/dashes, keep digits and leading +."""
    digits = re.sub(r"[\s\-\.\(\)]", "", p).strip()
    if digits.startswith("00971"):
        digits = "+" + digits[2:]
    if digits.startswith("971") and not digits.startswith("+"):
        digits = "+" + digits
    return digits


def _extract_contacts(html: str, base_url: str) -> dict:
    """Extract all contact signals from HTML string."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    full = html + " " + text  # check both raw HTML and visible text

    # Phones
    mobiles = [_normalise_phone(m) for m in _UAE_MOBILE_RE.findall(full)]
    phones = [_normalise_phone(p) for p in _UAE_PHONE_RE.findall(full)]
    all_phones = list(dict.fromkeys(mobiles + [p for p in phones if p not in mobiles]))

    # Email — skip generic/noreply
    emails = [e.lower() for e in _EMAIL_RE.findall(full)
              if not any(x in e.lower() for x in ("noreply", "no-reply", "example", "@sentry", "@w3"))]
    emails = list(dict.fromkeys(emails))

    # WhatsApp
    wa_numbers = []
    for match in _WHATSAPP_RE.finditer(full):
        num = _normalise_phone(match.group(1))
        if num not in wa_numbers:
            wa_numbers.append(num)
    # Also check href links
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if "whatsapp" in href.lower() or "wa.me" in href.lower():
            nums = re.findall(r'\d{7,15}', href)
            for n in nums:
                normed = _normalise_phone(n)
                if normed not in wa_numbers:
                    wa_numbers.append(normed)

    # Instagram
    ig_handles = []
    for m in _INSTAGRAM_RE.finditer(full):
        handle = m.group(1).lower()
        if handle not in _SKIP_IG and handle not in ig_handles:
            ig_handles.append(handle)

    # Facebook
    fb_pages = []
    for m in _FACEBOOK_RE.finditer(full):
        page = m.group(1).lower()
        if page not in _SKIP_FB and page not in fb_pages:
            fb_pages.append(page)

    # TikTok
    tt_handles = []
    for m in _TIKTOK_RE.finditer(full):
        handle = m.group(1).lower()
        if handle not in tt_handles:
            tt_handles.append(handle)

    return {
        "website_mobile": mobiles[0] if mobiles else "",
        "website_phone": all_phones[0] if all_phones else "",
        "website_email": emails[0] if emails else "",
        "website_whatsapp": wa_numbers[0] if wa_numbers else "",
        "website_instagram": f"instagram.com/{ig_handles[0]}" if ig_handles else "",
        "website_facebook": f"facebook.com/{fb_pages[0]}" if fb_pages else "",
        "website_tiktok": f"tiktok.com/@{tt_handles[0]}" if tt_handles else "",
        "all_emails": ", ".join(emails[:3]),
        "all_phones": ", ".join(all_phones[:3]),
    }


def scrape_website_contacts(url: str, *, timeout: int = 10, session=None) -> dict:
    """
    Scrape a restaurant website and return a contacts dict.
    Returns empty dict on failure (don't crash the pipeline).
    """
    empty = {k: "" for k in ("website_mobile", "website_phone", "website_email",
                              "website_whatsapp", "website_instagram", "website_facebook",
                              "website_tiktok", "all_emails", "all_phones")}
    if not url or not str(url).startswith("http"):
        return empty
    sess = session or requests.Session()
    try:
        r = sess.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return empty

    contacts = _extract_contacts(r.text, url)

    # If homepage has no contact info, try /contact or /contact-us page
    if not any(contacts[k] for k in ("website_mobile", "website_phone", "website_email", "website_whatsapp")):
        parsed = urlparse(url)
        for slug in ("/contact", "/contact-us", "/contactus", "/about", "/reach-us"):
            try:
                contact_url = f"{parsed.scheme}://{parsed.netloc}{slug}"
                rc = sess.get(contact_url, headers=_HEADERS, timeout=8, allow_redirects=True)
                if rc.status_code == 200:
                    more = _extract_contacts(rc.text, contact_url)
                    for k, v in more.items():
                        if v and not contacts[k]:
                            contacts[k] = v
                    if any(contacts[k] for k in ("website_mobile", "website_phone", "website_email")):
                        break
            except Exception:
                continue

    return contacts


def enrich_df_with_website_contacts(df, *, max_websites: int = 500) -> None:
    """
    Add website contact columns to df in-place.
    Scrapes up to max_websites unique URLs not already in the disk cache.
    """
    import pandas as pd
    NEW_COLS = ["website_mobile", "website_phone", "website_email",
                "website_whatsapp", "website_instagram", "website_facebook",
                "website_tiktok"]
    for col in NEW_COLS:
        if col not in df.columns:
            df[col] = ""

    # google_maps_link column holds website in some setups; check both
    url_col = None
    for candidate in ("website", "google_website", "website_url"):
        if candidate in df.columns:
            url_col = candidate
            break
    if url_col is None:
        return  # no website URLs available

    cache = _load_cache()
    session = requests.Session()
    done = 0

    seen_urls: dict = {}  # url → contacts (in-memory dedup this run)

    for idx in df.index:
        url = str(df.at[idx, url_col] or "").strip()
        if not url or not url.startswith("http"):
            continue

        # Normalise URL to domain level for caching
        parsed = urlparse(url)
        cache_key = f"{parsed.netloc}{parsed.path.rstrip('/')}".lower() or url

        # From memory cache
        if cache_key in seen_urls:
            contacts = seen_urls[cache_key]
        # From disk cache
        elif cache_key in cache:
            contacts = cache[cache_key]
            seen_urls[cache_key] = contacts
        # Need to scrape
        elif done < max_websites:
            time.sleep(0.3)
            contacts = scrape_website_contacts(url, session=session)
            seen_urls[cache_key] = contacts
            cache[cache_key] = contacts
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
