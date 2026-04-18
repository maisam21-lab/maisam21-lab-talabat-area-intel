#!/usr/bin/env python3
"""
Harvest Talabat vendor URLs from a country-wide "restaurants" listing by walking
"next" pagination — same idea as the Egypt Kaggle notebook, with country configurable:

  https://github.com/Marwan-xDiab/Talabat-Egypt-Restaurant-Web-Scraping-

Default is UAE (English listing). Talabat markup changes often; this script prefers
stable patterns: /uae/ vendor links and rel=next when present.

Usage:
  py -3 scripts/talabat_country_listing_harvest.py --country uae --out uae_vendor_links.csv
  py -3 scripts/talabat_country_listing_harvest.py --country egypt --listing-url https://www.talabat.com/egypt/restaurants

Requires: playwright (same as main scraper). Run once: playwright install chromium
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
from urllib.parse import urljoin, urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

_BASE = "https://www.talabat.com"

_DEFAULT_LISTING: dict[str, str] = {
    "uae": "https://www.talabat.com/en/uae/restaurants",
    "egypt": "https://www.talabat.com/egypt/restaurants",
}


def _country_path_slug(country: str) -> str:
    c = country.strip().lower()
    if c in ("uae", "ae", "emirates"):
        return "uae"
    if c in ("egypt", "eg"):
        return "egypt"
    return c.replace(" ", "-")


def _is_vendor_restaurant_url(href: str, slug: str) -> bool:
    if not href or "talabat.com" not in href:
        return False
    u = href.split("?", 1)[0].rstrip("/").lower()
    # /en/uae/foo or /uae/foo
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


async def harvest(
    listing_url: str,
    *,
    slug: str,
    max_next: int,
    headless: bool,
) -> list[str]:
    all_links: set[str] = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
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

        for round_i in range(max_next + 1):
            # Scroll to load lazy cards (similar spirit to the Egypt notebook loop)
            for _ in range(12):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(800)
            batch = await _collect_links_from_page(page, slug)
            before = len(all_links)
            all_links |= batch
            print(f"round={round_i} +{len(all_links) - before} links (total {len(all_links)})")

            # Egypt notebook: a[tabindex='0'][rel='next']
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Harvest Talabat listing vendor URLs (country-style notebook).")
    ap.add_argument("--country", default="uae", help="Country key: uae, egypt, or Talabat path slug (e.g. kuwait).")
    ap.add_argument(
        "--listing-url",
        default="",
        help="Override full listing URL (default: built-in per --country).",
    )
    ap.add_argument("--out", default="talabat_vendor_links.csv", help="Output CSV (one column: url).")
    ap.add_argument("--max-next", type=int, default=80, help="Max 'next' pagination clicks (safety cap).")
    ap.add_argument("--headed", action="store_true", help="Show browser window.")
    args = ap.parse_args()

    slug = _country_path_slug(args.country)
    listing = (args.listing_url or "").strip() or _DEFAULT_LISTING.get(slug, f"{_BASE}/en/{slug}/restaurants")

    links = asyncio.run(
        harvest(
            listing,
            slug=slug,
            max_next=max(0, args.max_next),
            headless=not args.headed,
        )
    )
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url", "country_slug"])
        for u in links:
            w.writerow([u, slug])
    print(f"Wrote {len(links)} rows to {args.out}")


if __name__ == "__main__":
    main()
