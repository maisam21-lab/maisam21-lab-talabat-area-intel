"""Optional second HTML source via managed scrapers (ScraperAPI, ZenRows, or custom URL template).

Set any provider key and merge runs automatically unless REMOTE_VENDOR_HTML=0.
Billing is on you — this module only fires when credentials exist (or REMOTE_VENDOR_HTML=1 with template).
"""

from __future__ import annotations

import os
import time
from urllib.parse import quote

import requests

_SCRAPERAPI = "http://api.scraperapi.com/"
_ZENROWS = "https://api.zenrows.com/v1/"
_DEFAULT_UA = "TalabatAreaIntel/1.0 (+https://github.com/maisam21-lab/maisam21-lab-talabat-area-intel)"


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _falsy_explicit(val: str | None) -> bool:
    return (val or "").strip().lower() in ("0", "false", "no", "n", "off")


def scraperapi_key() -> str:
    return (os.getenv("SCRAPERAPI_API_KEY") or os.getenv("SCRAPERAPI_KEY") or "").strip()


def zenrows_key() -> str:
    return (os.getenv("ZENROWS_API_KEY") or "").strip()


def remote_html_template() -> str:
    return (os.getenv("REMOTE_HTML_URL_TEMPLATE") or "").strip()


def remote_html_api_key() -> str:
    return (os.getenv("REMOTE_HTML_API_KEY") or scraperapi_key() or zenrows_key() or "").strip()


def remote_vendor_html_enabled() -> bool:
    flag = os.getenv("REMOTE_VENDOR_HTML", "").strip()
    if _falsy_explicit(flag):
        return False
    if _truthy(flag):
        return bool(remote_html_template() or scraperapi_key() or zenrows_key())
    # Unset: auto-on when any provider is configured (maximize data when keys exist).
    return bool(remote_html_template() or scraperapi_key() or zenrows_key())


def _fetch_via_template(url: str) -> str | None:
    tmpl = remote_html_template()
    if not tmpl or "{url}" not in tmpl:
        return None
    key = remote_html_api_key()
    built = tmpl.replace("{url}", quote(url, safe=""))
    built = built.replace("{url_raw}", url)
    if "{key}" in built:
        if not key:
            return None
        built = built.replace("{key}", quote(key, safe=""))
    try:
        r = requests.get(
            built,
            timeout=int(os.getenv("REMOTE_HTML_TIMEOUT_SEC", "90")),
            headers={"User-Agent": (os.getenv("REMOTE_HTML_USER_AGENT") or "").strip() or _DEFAULT_UA},
        )
        r.raise_for_status()
        return r.text if r.text and len(r.text) > 200 else None
    except (requests.RequestException, ValueError):
        return None


def _fetch_via_scraperapi(url: str) -> str | None:
    key = scraperapi_key()
    if not key:
        return None
    params: dict[str, str] = {"api_key": key, "url": url}
    if _truthy(os.getenv("SCRAPERAPI_RENDER")):
        params["render"] = "true"
    cc = (os.getenv("SCRAPERAPI_COUNTRY_CODE") or "").strip()
    if cc:
        params["country_code"] = cc
    try:
        r = requests.get(
            _SCRAPERAPI,
            params=params,
            timeout=int(os.getenv("SCRAPERAPI_TIMEOUT_SEC", "90")),
            headers={"User-Agent": _DEFAULT_UA},
        )
        r.raise_for_status()
        return r.text if r.text and len(r.text) > 200 else None
    except (requests.RequestException, ValueError):
        return None


def _fetch_via_zenrows(url: str) -> str | None:
    key = zenrows_key()
    if not key:
        return None
    params: dict[str, str] = {"apikey": key, "url": url}
    zjr = (os.getenv("ZENROWS_JS_RENDER", "true") or "").strip().lower()
    if zjr not in ("0", "false", "no", "off"):
        params["js_render"] = "true"
    try:
        r = requests.get(
            _ZENROWS,
            params=params,
            timeout=int(os.getenv("ZENROWS_TIMEOUT_SEC", "90")),
            headers={"User-Agent": _DEFAULT_UA},
        )
        r.raise_for_status()
        return r.text if r.text and len(r.text) > 200 else None
    except (requests.RequestException, ValueError):
        return None


def fetch_remote_vendor_html(url: str) -> str | None:
    """Return raw HTML for a Talabat vendor URL, or None."""
    if not remote_vendor_html_enabled():
        return None
    u = (url or "").strip()
    if not u or "talabat.com" not in u.lower():
        return None
    html: str | None = None
    if remote_html_template():
        html = _fetch_via_template(u)
    if html is None:
        html = _fetch_via_scraperapi(u)
    if html is None:
        html = _fetch_via_zenrows(u)
    pause = float(os.getenv("REMOTE_HTML_PAUSE_SEC", "0.35"))
    if pause > 0:
        time.sleep(pause)
    return html
