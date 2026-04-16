from __future__ import annotations

import asyncio
import math
import os
import re
from datetime import datetime, timezone

import pandas as pd
from playwright.async_api import async_playwright

from geo_utils import generate_points_in_radius
from models import RestaurantRecord, make_branch_sku
from next_data_extract import normalize_talabat_url, parse_next_data_script, paths_from_next_data_json

# English listing URL first (more consistent markup); fallback handled in scrape_one_point.
BASE_URL_PRIMARY = "https://www.talabat.com/en/uae/restaurants"
BASE_URL_FALLBACK = "https://www.talabat.com/uae/restaurants"

# Required for Chromium in Docker / Render (small /dev/shm, no user namespace sandbox).
CHROMIUM_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
]
CARD_SELECTORS = [
    '[data-testid*="restaurant"]',
    'a[href*="/restaurant/"]',
    'div:has(a[href*="/restaurant/"])',
    'article:has(a[href*="/restaurant/"])',
]
CLOSED_HINTS = ["closed", "temporarily closed", "not accepting"]
LIVE_HINTS = ["open now", "accepting orders", "live"]


def classify_status(blob: str) -> str:
    txt = (blob or "").strip().lower()
    if any(x in txt for x in CLOSED_HINTS):
        return "closed"
    if any(x in txt for x in LIVE_HINTS):
        return "live"
    return "unknown"


def parse_lat_lng(text: str) -> tuple[float | None, float | None]:
    m = re.search(r"([+-]?\d{1,2}\.\d{3,}),\s*([+-]?\d{1,3}\.\d{3,})", text or "")
    if not m:
        return None, None
    lat, lng = float(m.group(1)), float(m.group(2))
    if -90 <= lat <= 90 and -180 <= lng <= 180:
        return lat, lng
    return None, None


async def auto_scroll(page, rounds: int = 22, wait_ms: int = 1300) -> None:
    prev_height = 0
    steady_rounds = 0
    for _ in range(rounds):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(wait_ms)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            steady_rounds += 1
            if steady_rounds >= 2:
                break
        else:
            steady_rounds = 0
            prev_height = new_height


async def dismiss_common_overlays(page) -> None:
    """Close cookie / region banners that block listing hydration."""
    for selector in (
        'button:has-text("Accept")',
        'button:has-text("Accept all")',
        '[data-testid*="accept"]',
        'button[aria-label*="ccept"]',
    ):
        try:
            loc = page.locator(selector).first
            if await loc.count():
                await loc.click(timeout=2000)
                await page.wait_for_timeout(500)
                break
        except Exception:
            pass


async def click_just_landed_if_requested(page, just_landed_only: bool) -> None:
    if not just_landed_only:
        return
    candidates = ["text=Just Landed", "text=just landed", '[data-testid*="just"]', '[aria-label*="Just"]']
    for selector in candidates:
        btn = page.locator(selector).first
        if await btn.count():
            try:
                await btn.click(timeout=2000)
                await page.wait_for_timeout(1200)
                return
            except Exception:
                pass


async def extract_restaurants_from_anchor_links(
    page,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    sample_lat: float,
    sample_lng: float,
) -> list[RestaurantRecord]:
    """Primary extractor: UAE vendor links (/uae/{slug}) + legacy /restaurant/ links."""
    payload = await page.evaluate(
        """() => {
      const exclude = new Set([
        'restaurants','groceries','mart','pharmacy','flowers','en','ar','faq','terms','privacy',
        'privacy-policy','contact','contact-us','login','register','cart','checkout','cities',
        'blog','careers','corporate','about','sitemap','order','account','wallet','deals',
        'dineout','shops'
      ]);
      const seen = new Set();
      const out = [];
      const add = (u, name) => {
        const c = u.split('?')[0];
        if (seen.has(c)) return;
        seen.add(c);
        out.push({ url: c, name: (name || '').trim() });
      };
      for (const a of document.querySelectorAll('a[href]')) {
        let href = a.getAttribute('href') || '';
        if (!href || href === '#' || href.startsWith('javascript')) continue;
        if (href.startsWith('/')) href = 'https://www.talabat.com' + href;
        if (!href.includes('talabat.com')) continue;
        try {
          const u = new URL(href);
          let parts = u.pathname.split('/').filter(Boolean);
          if (parts[0] === 'en' || parts[0] === 'ar') parts = parts.slice(1);
          if (parts.length >= 2 && parts[0] === 'uae') {
            const seg = parts[1].toLowerCase();
            if (exclude.has(seg) || seg.length < 3) continue;
            const canon = 'https://www.talabat.com/uae/' + parts[1];
            const name = (a.innerText || '').trim().split('\\n')[0].trim();
            add(canon, name);
            continue;
          }
          if (href.includes('/restaurant/')) {
            add(href.split('?')[0], (a.innerText || '').trim().split('\\n')[0].trim());
          }
        } catch (e) { /* ignore */ }
      }
      return out;
    }"""
    )
    if not payload:
        return []
    now_utc = datetime.now(timezone.utc).isoformat()
    results: list[RestaurantRecord] = []
    for item in payload:
        url = str(item.get("url") or "").strip()
        name = str(item.get("name") or "").strip()
        if not url:
            continue
        slug_name = url.rstrip("/").split("/")[-1].replace("-", " ").title() if url else ""
        if not name:
            name = slug_name
        branch_name = ""
        if " - " in name:
            p1, p2 = name.split(" - ", 1)
            if p1.strip() and p2.strip():
                name, branch_name = p1.strip(), p2.strip()
        lat, lng = sample_lat, sample_lng
        sku = make_branch_sku(name=name, branch_name=branch_name, url=url, lat=lat, lng=lng)
        blob = name.lower()
        results.append(
            RestaurantRecord(
                scrape_ts_utc=now_utc,
                source_pin_lat=pin_lat,
                source_pin_lng=pin_lng,
                radius_km=radius_km,
                source_sample_lat=sample_lat,
                source_sample_lng=sample_lng,
                branch_sku=sku,
                restaurant_name=name,
                branch_name=branch_name,
                restaurant_url=url,
                cuisines="",
                rating="",
                eta="",
                delivery_fee="",
                min_order="",
                status=classify_status(blob),
                just_landed_flag="just landed" in blob,
                lat=lat,
                lng=lng,
            )
        )
    return results


async def extract_restaurants_from_next_data(
    page,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    sample_lat: float,
    sample_lng: float,
) -> list[RestaurantRecord]:
    raw = await page.evaluate(
        """() => {
      const el = document.getElementById('__NEXT_DATA__');
      return el ? el.textContent : null;
    }"""
    )
    data = parse_next_data_script(raw or "")
    if not data:
        return []
    paths = paths_from_next_data_json(data)
    if not paths:
        return []
    now_utc = datetime.now(timezone.utc).isoformat()
    results: list[RestaurantRecord] = []
    for path in paths:
        url = normalize_talabat_url(path)
        slug = url.rstrip("/").split("/")[-1]
        name = slug.replace("-", " ").title() if slug else "Unknown"
        sku = make_branch_sku(name=name, branch_name="", url=url, lat=sample_lat, lng=sample_lng)
        results.append(
            RestaurantRecord(
                scrape_ts_utc=now_utc,
                source_pin_lat=pin_lat,
                source_pin_lng=pin_lng,
                radius_km=radius_km,
                source_sample_lat=sample_lat,
                source_sample_lng=sample_lng,
                branch_sku=sku,
                restaurant_name=name,
                branch_name="",
                restaurant_url=url,
                cuisines="",
                rating="",
                eta="",
                delivery_fee="",
                min_order="",
                status="unknown",
                just_landed_flag=False,
                lat=sample_lat,
                lng=sample_lng,
            )
        )
    return results


async def extract_restaurants(
    page,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    sample_lat: float,
    sample_lng: float,
) -> list[RestaurantRecord]:
    # 1) All /restaurant/ links (most robust vs UI redesign)
    anchor_rows = await extract_restaurants_from_anchor_links(
        page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng
    )
    if anchor_rows:
        return anchor_rows

    # 2) Next.js embedded JSON
    next_rows = await extract_restaurants_from_next_data(
        page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng
    )
    if next_rows:
        return next_rows

    # 3) Legacy card selectors
    cards = None
    for selector in CARD_SELECTORS:
        loc = page.locator(selector)
        if await loc.count() > 0:
            cards = loc
            break
    if cards is None:
        return []

    now_utc = datetime.now(timezone.utc).isoformat()
    results: list[RestaurantRecord] = []
    count = await cards.count()

    for i in range(count):
        card = cards.nth(i)
        link = card.locator('a[href*="/restaurant/"]').first

        name = ""
        branch_name = ""
        url = ""
        cuisines = ""
        rating = ""
        eta = ""
        delivery_fee = ""
        min_order = ""

        # If the matched card is an anchor itself, use it directly.
        tag_name = ""
        try:
            tag_name = await card.evaluate("el => el.tagName.toLowerCase()")
        except Exception:
            tag_name = ""

        if tag_name == "a":
            href = await card.get_attribute("href") or ""
            if "/restaurant/" in href:
                url = f"https://www.talabat.com{href}" if href.startswith("/") else href
                link_text = (await card.inner_text()).strip()
                if link_text:
                    name = link_text.split("\n")[0].strip()

        if (not url or not name) and await link.count():
            href = await link.get_attribute("href") or ""
            url = f"https://www.talabat.com{href}" if href.startswith("/") else href
            link_text = (await link.inner_text()).strip()
            if link_text:
                name = link_text.split("\n")[0].strip()

        if not name:
            for fallback in ["h2", "h3", "strong", '[data-testid*="name"]']:
                node = card.locator(fallback).first
                if await node.count():
                    t = (await node.inner_text()).strip()
                    if t:
                        name = t
                        break

        blob = " ".join((await card.inner_text()).split())
        lower_blob = blob.lower()
        just_landed_flag = "just landed" in lower_blob
        status = classify_status(blob)

        tokens = [t.strip() for t in blob.split("•")]
        for t in tokens:
            tl = t.lower()
            if not cuisines and any(k in tl for k in ("pizza", "burger", "arabic", "shawarma", "indian")):
                cuisines = t
            if not eta and "min" in tl:
                eta = t
            if not delivery_fee and ("delivery" in tl or "aed" in tl):
                delivery_fee = t
            if not min_order and ("minimum" in tl or "min order" in tl):
                min_order = t
            if not rating and re.search(r"\b\d\.\d\b", t):
                rating = t

        if " - " in name:
            p1, p2 = name.split(" - ", 1)
            if p1.strip() and p2.strip():
                name, branch_name = p1.strip(), p2.strip()

        lat, lng = parse_lat_lng(f"{url} {blob}")
        if lat is None or lng is None:
            lat, lng = sample_lat, sample_lng

        sku = make_branch_sku(name=name, branch_name=branch_name, url=url, lat=lat, lng=lng)
        if name or url:
            results.append(
                RestaurantRecord(
                    scrape_ts_utc=now_utc,
                    source_pin_lat=pin_lat,
                    source_pin_lng=pin_lng,
                    radius_km=radius_km,
                    source_sample_lat=sample_lat,
                    source_sample_lng=sample_lng,
                    branch_sku=sku,
                    restaurant_name=name,
                    branch_name=branch_name,
                    restaurant_url=url,
                    cuisines=cuisines,
                    rating=rating,
                    eta=eta,
                    delivery_fee=delivery_fee,
                    min_order=min_order,
                    status=status,
                    just_landed_flag=just_landed_flag,
                    lat=lat,
                    lng=lng,
                )
            )
    return results


async def scrape_one_point(
    browser,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    sample_lat: float,
    sample_lng: float,
    just_landed_only: bool,
    scroll_rounds: int,
    scroll_wait_ms: int,
) -> list[RestaurantRecord]:
    context = await browser.new_context(
        geolocation={"latitude": sample_lat, "longitude": sample_lng},
        permissions=["geolocation"],
        locale="en-US",
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        viewport={"width": 1440, "height": 900},
    )
    page = await context.new_page()
    try:
        # Try English listing URL first, then non-prefixed path (Talabat sometimes varies by locale/route).
        for listing_url in (BASE_URL_PRIMARY, BASE_URL_FALLBACK):
            try:
                await page.goto(listing_url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(3500)
                await dismiss_common_overlays(page)
                await click_just_landed_if_requested(page, just_landed_only)
                await auto_scroll(page, rounds=scroll_rounds, wait_ms=scroll_wait_ms)
                rows = await extract_restaurants(page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng)
                if rows:
                    return rows
            except Exception:
                continue
        return []
    except Exception:
        return []
    finally:
        await context.close()


def _cap_sample_points(points: list[tuple[float, float]], max_pts: int) -> list[tuple[float, float]]:
    """Subsample grid points to stay within Render time/memory limits."""
    if max_pts < 1:
        max_pts = 1
    if len(points) <= max_pts:
        return points
    step = max(1, math.ceil(len(points) / max_pts))
    sampled = points[::step]
    if len(sampled) > max_pts:
        sampled = sampled[:max_pts]
    return list(dict.fromkeys(sampled))


async def run_area_scrape(
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    spacing_km: float,
    concurrency: int,
    status_filter: str,
    just_landed_only: bool,
    scroll_rounds: int,
    scroll_wait_ms: int,
    progress_cb=None,
) -> pd.DataFrame:
    points = generate_points_in_radius(pin_lat, pin_lng, radius_km, spacing_km)
    max_pts = int(os.getenv("MAX_SCRAPE_SAMPLE_POINTS", "8"))
    points = _cap_sample_points(points, max_pts)
    sem = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=CHROMIUM_LAUNCH_ARGS,
        )
        done = 0
        total = len(points)

        async def worker(pt: tuple[float, float]) -> list[RestaurantRecord]:
            nonlocal done
            lat, lng = pt
            async with sem:
                rows = await scrape_one_point(
                    browser=browser,
                    pin_lat=pin_lat,
                    pin_lng=pin_lng,
                    radius_km=radius_km,
                    sample_lat=lat,
                    sample_lng=lng,
                    just_landed_only=just_landed_only,
                    scroll_rounds=scroll_rounds,
                    scroll_wait_ms=scroll_wait_ms,
                )
            done += 1
            if progress_cb:
                progress_cb(done, total, lat, lng, len(rows))
            return rows

        batches = await asyncio.gather(*[worker(pt) for pt in points])
        await browser.close()

    dedupe: dict[str, RestaurantRecord] = {}
    for batch in batches:
        for r in batch:
            dedupe[r.branch_sku] = r

    df = pd.DataFrame([r.to_dict() for r in dedupe.values()])
    if df.empty:
        return df
    if status_filter in {"live", "closed"}:
        df = df[df["status"] == status_filter].copy()
    return df.reset_index(drop=True)
