"""Country-wide Talabat listing → vendor URL harvest (Playwright, pagination + scroll).

Used by ``scripts/talabat_country_listing_harvest.py`` and ``POST /listing-harvest`` on the API.
"""

from __future__ import annotations

import logging
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright

logger = logging.getLogger("talabat_area_intel.listing_harvest")

_BASE = "https://www.talabat.com"

_DEFAULT_LISTING: dict[str, str] = {
    "uae": "https://www.talabat.com/en/uae/restaurants",
    "egypt": "https://www.talabat.com/egypt/restaurants",
}

_CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
]


def country_path_slug(country: str) -> str:
    c = country.strip().lower()
    if c in ("uae", "ae", "emirates"):
        return "uae"
    if c in ("egypt", "eg"):
        return "egypt"
    return c.replace(" ", "-")


def default_listing_url_for_slug(slug: str) -> str:
    return _DEFAULT_LISTING.get(slug, f"{_BASE}/en/{slug}/restaurants")


def _is_vendor_restaurant_url(href: str, slug: str) -> bool:
    if not href or "talabat.com" not in href:
        return False
    u = href.split("?", 1)[0].rstrip("/").lower()
    if f"/{slug}/" not in u:
        return False
    skip = {
        "restaurants",
        "groceries",
        "mart",
        "pharmacy",
        "flowers",
        "en",
        "ar",
        "cuisine",
        "login",
        "register",
        "faq",
        "terms",
        "privacy",
    }
    parts = [p for p in urlparse(u).path.split("/") if p]
    if slug in parts:
        idx = parts.index(slug)
        seg = parts[idx + 1] if idx + 1 < len(parts) else ""
        if not seg or seg.lower() in skip or len(seg) < 3:
            return False
        return True
    return False


async def _collect_links_from_page(page, slug: str) -> set[str]:
    out: set[str] = set()
    hrefs = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => a.getAttribute('href') || '').filter(Boolean)""",
    )
    for h in hrefs:
        if h.startswith("/"):
            h = urljoin(_BASE, h)
        if _is_vendor_restaurant_url(h, slug):
            out.add(h.split("?", 1)[0].rstrip("/"))
    return out


async def harvest_vendor_urls(
    listing_url: str,
    *,
    country_slug: str,
    max_next: int,
    headless: bool = True,
) -> list[str]:
    """Walk listing pages (scroll + rel=next) and return canonical vendor URLs for ``country_slug``."""
    all_links: set[str] = set()
    slug = country_slug.strip().lower()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=_CHROMIUM_ARGS)
        ctx = await browser.new_context(
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1400, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = await ctx.new_page()
        await page.goto(listing_url, wait_until="domcontentloaded", timeout=120000)
        await page.wait_for_timeout(2500)

        for round_i in range(max(0, max_next) + 1):
            for _ in range(12):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(800)
            batch = await _collect_links_from_page(page, slug)
            before = len(all_links)
            all_links |= batch
            logger.info(
                "listing_harvest round=%s new_links=%s total=%s",
                round_i,
                len(all_links) - before,
                len(all_links),
            )

            next_selectors = [
                "a[rel='next']",
                "a[tabindex='0'][rel='next']",
                "a[aria-label='Next']",
                "button[aria-label='Next']",
            ]
            clicked = False
            for sel in next_selectors:
                loc = page.locator(sel).first
                try:
                    if await loc.count() and await loc.is_enabled():
                        await loc.click(timeout=8000)
                        await page.wait_for_timeout(2000)
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                break
        await browser.close()
    return sorted(all_links)
