from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import traceback
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from playwright.async_api import async_playwright

from geo_utils import generate_points_in_radius, haversine_km, haversine_series_km_from_pin, refine_grid_spacing
from listing_urls import capped_listing_urls
from pin_resolve import resolve_pin_area_label
from models import (
    RestaurantRecord,
    brand_display_name_from_listing,
    make_branch_sku,
    make_brand_id,
    talabat_listing_slug_from_url,
)
from html_enrichment import merge_html_into_accumulator
from remote_html_fetch import fetch_remote_vendor_html
from next_data_extract import normalize_talabat_url, parse_next_data_script, paths_from_next_data_json
from nominatim_enrich import enrich_records_reverse_geocode
from places_enrich import enrich_records_with_google_places, google_places_enrich_effective

logger = logging.getLogger("talabat_area_intel.scrape")

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


_JUST_LANDED_HINT = re.compile(r"just\s*landed", re.I)
_JL_DATE_HINT = re.compile(
    r"(?:\d{1,2}\s+[A-Za-z]{3,12}\s*'? ?\d{0,4})|(?:\d{4}-\d{2}-\d{2})|(?:\d+\s*(?:hours?|days?|weeks?|months?)\s*ago)",
    re.I,
)


def parse_just_landed_from_text(text: str) -> tuple[str, str]:
    """Return (yes|no, short date/detail next to the badge when present)."""
    raw = (text or "").strip()
    if not raw or not _JUST_LANDED_HINT.search(raw):
        return "no", ""
    detail = ""
    for m in _JUST_LANDED_HINT.finditer(raw):
        tail = raw[m.end() :].strip()
        for sep in ("·", "•", "–", "-", ":"):
            if tail.startswith(sep):
                tail = tail[1:].strip()
        line = tail.split("\n")[0].strip()
        if "·" in line:
            line = line.split("·", 1)[0].strip()
        if "•" in line:
            line = line.split("•", 1)[0].strip()
        if line:
            detail = line[:120]
            break
    if not detail:
        dm = _JL_DATE_HINT.search(raw)
        if dm:
            detail = dm.group(0).strip()[:120]
    return "yes", detail


def parse_lat_lng(text: str) -> tuple[float | None, float | None]:
    m = re.search(r"([+-]?\d{1,2}\.\d{3,}),\s*([+-]?\d{1,3}\.\d{3,})", text or "")
    if not m:
        return None, None
    lat, lng = float(m.group(1)), float(m.group(2))
    if -90 <= lat <= 90 and -180 <= lng <= 180:
        return lat, lng
    return None, None


async def auto_scroll(page, rounds: int = 22, wait_ms: int = 1300) -> None:
    """Scroll until listing links stop growing (Talabat uses infinite scroll; body height can plateau early)."""
    prev_height = 0
    steady_height = 0
    prev_link_count: int | None = None
    steady_links = 0
    need_link_steady = int(os.getenv("SCRAPER_SCROLL_STEADY_LINK_ROUNDS", "3"))
    for _ in range(rounds):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(wait_ms)
        new_height = await page.evaluate("document.body.scrollHeight")
        n_links = await page.evaluate(
            """() => {
              const s = new Set();
              for (const a of document.querySelectorAll('a[href]')) {
                let h = a.getAttribute('href') || '';
                if (h.startsWith('/')) h = 'https://www.talabat.com' + h;
                if (!h.includes('talabat.com')) continue;
                if (h.includes('/restaurant/') || h.includes('/uae/')) s.add(h.split('?')[0]);
              }
              return s.size;
            }"""
        )
        try:
            n_links = int(n_links)
        except (TypeError, ValueError):
            n_links = prev_link_count if prev_link_count is not None else 0
        if new_height == prev_height:
            steady_height += 1
        else:
            steady_height = 0
            prev_height = new_height
        if prev_link_count is None:
            prev_link_count = n_links
            steady_links = 0
        elif n_links == prev_link_count:
            steady_links += 1
        else:
            steady_links = 0
            prev_link_count = n_links
        if steady_links >= need_link_steady:
            break
        if steady_height >= 3 and prev_link_count is not None and prev_link_count > 0:
            break


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
      const cardSnippet = (a) => {
        let el = a.closest('[data-testid*="vendor"]') || a.closest('[data-testid*="restaurant"]')
          || a.closest('article') || a.parentElement;
        for (let i = 0; i < 5 && el; i++) {
          const t = (el.innerText || '').trim();
          if (t.length > 50) return t.slice(0, 900);
          el = el.parentElement;
        }
        const p = a.parentElement;
        return ((p && p.innerText) || a.innerText || '').trim().slice(0, 900);
      };
      const add = (u, name, snippet) => {
        const c = u.split('?')[0];
        if (seen.has(c)) return;
        seen.add(c);
        out.push({ url: c, name: (name || '').trim(), snippet: (snippet || '').trim() });
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
            add(canon, name, cardSnippet(a));
            continue;
          }
          if (href.includes('/restaurant/')) {
            const nm = (a.innerText || '').trim().split('\\n')[0].trim();
            add(href.split('?')[0], nm, cardSnippet(a));
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
        snippet = str(item.get("snippet") or "")
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
        bd = brand_display_name_from_listing(name, branch_name)
        bid = make_brand_id(bd)
        tslug = talabat_listing_slug_from_url(url)
        blob = f"{snippet}\n{name}".lower()
        jl, jld = parse_just_landed_from_text(f"{snippet}\n{name}")
        results.append(
            RestaurantRecord(
                scrape_ts_utc=now_utc,
                source_pin_lat=pin_lat,
                source_pin_lng=pin_lng,
                radius_km=radius_km,
                source_sample_lat=sample_lat,
                source_sample_lng=sample_lng,
                branch_sku=sku,
                brand_id=bid,
                brand_display_name=(bd or "")[:200],
                talabat_listing_slug=tslug,
                restaurant_name=name,
                legal_name="",
                branch_name=branch_name,
                restaurant_url=url,
                talabat_restaurant_id="",
                talabat_branch_id="",
                contact_phone="",
                cuisines="",
                rating="",
                reviews_count="",
                eta="",
                delivery_fee="",
                min_order="",
                area_label="",
                status=classify_status(blob),
                just_landed=jl,
                just_landed_date=jld,
                google_rating="",
                google_reviews_count="",
                rating_source="",
                highly_rated_google="",
                is_pro_vendor="",
                free_delivery="",
                delivered_by_talabat="",
                preorder_available="",
                payment_methods="",
                currency="",
                recently_added_90d="",
                has_offers="",
                estimated_orders="",
                google_place_id="",
                google_maps_name="",
                vendor_website="",
                vendor_email="",
                vendor_social="",
                vendor_description="",
                tax_or_license_hint="",
                opening_hours_snippet="",
                google_formatted_address="",
                google_business_website="",
                google_maps_link="",
                google_primary_type="",
                reverse_geocode_address="",
                scrape_city="",
                scrape_target_label="",
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
        path_slug = url.rstrip("/").split("/")[-1]
        name = path_slug.replace("-", " ").title() if path_slug else "Unknown"
        sku = make_branch_sku(name=name, branch_name="", url=url, lat=sample_lat, lng=sample_lng)
        bd = brand_display_name_from_listing(name, "")
        bid = make_brand_id(bd)
        tslug = talabat_listing_slug_from_url(url)
        results.append(
            RestaurantRecord(
                scrape_ts_utc=now_utc,
                source_pin_lat=pin_lat,
                source_pin_lng=pin_lng,
                radius_km=radius_km,
                source_sample_lat=sample_lat,
                source_sample_lng=sample_lng,
                branch_sku=sku,
                brand_id=bid,
                brand_display_name=(bd or "")[:200],
                talabat_listing_slug=tslug,
                restaurant_name=name,
                legal_name="",
                branch_name="",
                restaurant_url=url,
                talabat_restaurant_id="",
                talabat_branch_id="",
                contact_phone="",
                cuisines="",
                rating="",
                reviews_count="",
                eta="",
                delivery_fee="",
                min_order="",
                area_label="",
                status="unknown",
                just_landed="no",
                just_landed_date="",
                google_rating="",
                google_reviews_count="",
                rating_source="",
                highly_rated_google="",
                is_pro_vendor="",
                free_delivery="",
                delivered_by_talabat="",
                preorder_available="",
                payment_methods="",
                currency="",
                recently_added_90d="",
                has_offers="",
                estimated_orders="",
                google_place_id="",
                google_maps_name="",
                vendor_website="",
                vendor_email="",
                vendor_social="",
                vendor_description="",
                tax_or_license_hint="",
                opening_hours_snippet="",
                google_formatted_address="",
                google_business_website="",
                google_maps_link="",
                google_primary_type="",
                reverse_geocode_address="",
                scrape_city="",
                scrape_target_label="",
                lat=sample_lat,
                lng=sample_lng,
            )
        )
    return results


def _merge_restaurant_rows_by_url(*batches: list[RestaurantRecord]) -> list[RestaurantRecord]:
    """Union anchors + __NEXT_DATA__ paths; same Talabat URL often appears in both with different name quality."""

    def score(row: RestaurantRecord) -> tuple[int, int, int]:
        name = (row.restaurant_name or "").strip()
        branch = (row.branch_name or "").strip()
        blob = f"{name} {branch}".lower()
        jl_boost = 1 if row.just_landed == "yes" else 0
        status_boost = 1 if row.status and row.status != "unknown" else 0
        return (jl_boost + status_boost, len(branch), len(name))

    best: dict[str, RestaurantRecord] = {}
    for batch in batches:
        for r in batch:
            key = _canonical_vendor_url(r.restaurant_url)
            if not key:
                key = (r.branch_sku or "").strip().lower()
            cur = best.get(key)
            if cur is None:
                best[key] = r
            elif score(r) > score(cur):
                best[key] = r
    return list(best.values())


async def extract_restaurants(
    page,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    sample_lat: float,
    sample_lng: float,
) -> list[RestaurantRecord]:
    # 1–2) Anchors and __NEXT_DATA__ together (previously we returned anchors only and dropped JSON-only vendors).
    anchor_rows = await extract_restaurants_from_anchor_links(
        page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng
    )
    next_rows = await extract_restaurants_from_next_data(
        page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng
    )
    merged = _merge_restaurant_rows_by_url(anchor_rows, next_rows)
    if merged:
        return merged

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
        jl, jld = parse_just_landed_from_text(blob)
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
        bd = brand_display_name_from_listing(name, branch_name)
        bid = make_brand_id(bd)
        tslug = talabat_listing_slug_from_url(url)
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
                    brand_id=bid,
                    brand_display_name=(bd or "")[:200],
                    talabat_listing_slug=tslug,
                    restaurant_name=name,
                    legal_name="",
                    branch_name=branch_name,
                    restaurant_url=url,
                    talabat_restaurant_id="",
                    talabat_branch_id="",
                    contact_phone="",
                    cuisines=cuisines,
                    rating=rating,
                    reviews_count="",
                    eta=eta,
                    delivery_fee=delivery_fee,
                    min_order=min_order,
                    area_label="",
                    status=status,
                    just_landed=jl,
                    just_landed_date=jld,
                    google_rating="",
                    google_reviews_count="",
                    rating_source="",
                    highly_rated_google="",
                    is_pro_vendor="",
                    free_delivery="",
                    delivered_by_talabat="",
                    preorder_available="",
                    payment_methods="",
                    currency="",
                    recently_added_90d="",
                    has_offers="",
                    estimated_orders="",
                    google_place_id="",
                    google_maps_name="",
                    vendor_website="",
                    vendor_email="",
                    vendor_social="",
                    vendor_description="",
                    tax_or_license_hint="",
                    opening_hours_snippet="",
                    google_formatted_address="",
                    google_business_website="",
                    google_maps_link="",
                    google_primary_type="",
                    reverse_geocode_address="",
                    scrape_city="",
                    scrape_target_label="",
                    lat=lat,
                    lng=lng,
                )
            )
    return results


_TEL_HREF_RE = re.compile(r"""href=["']tel:([^"'\s>]+)""", re.I)
_NEXT_DATA_BLOCK = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
_CURRENCY_CODES = ("JOD", "AED", "SAR", "KWD", "BHD", "QAR", "USD", "EGP", "OMR")


def _infer_currency(*chunks: str) -> str:
    blob = " ".join(c for c in chunks if c).upper()
    for code in _CURRENCY_CODES:
        if code in blob:
            return code
    return ""


def _best_rating_string(cands: list[str]) -> str:
    best: float | None = None
    for s in cands:
        m = re.search(r"(\d+(?:\.\d+)?)", s or "")
        if not m:
            continue
        try:
            x = float(m.group(1))
            if 0 <= x <= 5 and (best is None or x > best):
                best = x
        except ValueError:
            continue
    if best is None:
        return ""
    out = f"{best:.2f}".rstrip("0").rstrip(".")
    return out


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _pick_best_phone(cands: list[str]) -> str:
    best = ""
    best_d = 0
    for c in cands:
        c = re.sub(r"\s+", " ", (c or "").strip())
        d = _digits_only(c)
        if len(d) >= 8 and len(d) >= best_d:
            best, best_d = c, len(d)
    return best


def _merge_vendor_html_into_accumulator(html: str | None, acc: dict[str, list]) -> None:
    """Tel links, HTML/JSON-LD/meta harvest, and __NEXT_DATA__ JSON walk into acc."""
    if not html:
        return
    for m in _TEL_HREF_RE.finditer(html):
        acc.setdefault("phones", []).append(m.group(1).strip())
    merge_html_into_accumulator(html, acc)
    m = _NEXT_DATA_BLOCK.search(html)
    if not m:
        return
    raw = m.group(1).strip()
    if not raw:
        return
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return
    if isinstance(data, dict):
        _walk_next_data_vendor_fields(data, acc)


def _pick_vendor_website(urls: list[str]) -> str:
    best = ""
    for u in urls:
        ul = u.lower()
        if "talabat." in ul:
            continue
        if "google." in ul and "maps" in ul:
            continue
        if len(u) > len(best):
            best = u
    return best[:500]


def _walk_next_data_vendor_fields(obj: Any, acc: dict[str, list]) -> None:
    """Collect phones, legal names, IDs, ratings, fees, etc. from embedded JSON."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = k.lower().replace("_", "")
            if isinstance(v, str):
                vs = v.strip()
                if not vs:
                    continue
                if any(
                    h in kl
                    for h in (
                        "phone",
                        "mobile",
                        "telephone",
                        "contactnumber",
                        "phonenumber",
                        "mobilenumber",
                        "contactphone",
                    )
                ):
                    if len(_digits_only(vs)) >= 8:
                        acc.setdefault("phones", []).append(vs)
                elif any(
                    h in kl
                    for h in (
                        "legalname",
                        "registeredname",
                        "businessname",
                        "companyname",
                        "tradelicense",
                        "restaurantlegal",
                        "commercialname",
                        "licenseholder",
                        "crnumber",
                    )
                ):
                    if 3 < len(vs) < 220:
                        acc.setdefault("legal", []).append(vs)
                elif "google" in kl and "rating" in kl and re.search(r"\d", vs):
                    acc.setdefault("google_ratings", []).append(vs[:24])
                elif "google" in kl and ("review" in kl or "ratingcount" in kl or "map" in kl):
                    acc.setdefault("google_review_counts", []).append(vs[:40])
                elif kl in ("currency", "currencycode", "ordercurrency") and len(vs) <= 6:
                    acc.setdefault("currencies", []).append(vs.upper()[:6])
                elif "payment" in kl and ("method" in kl or kl.endswith("payments")):
                    acc.setdefault("payments", []).append(vs[:400])
                elif "preorder" in kl or kl == "ispreorderenabled":
                    acc.setdefault("preorder_vals", []).append(vs[:40])
                elif "delivered" in kl and "talabat" in vs.lower():
                    acc.setdefault("delivered_talabat", [True])
                elif kl in ("isfreedelivery", "hasfreedelivery", "isfreedeliveryavailable") and vs.lower() in (
                    "true",
                    "yes",
                    "1",
                ):
                    acc.setdefault("free_delivery_yes", [True])
                elif kl in ("ispro", "isprorestaurant", "hasprosubscription", "talabatpro") and vs.lower() in (
                    "true",
                    "yes",
                    "1",
                ):
                    acc.setdefault("pro_yes", [True])
                elif ("offer" in kl or "promotion" in kl) and "count" in kl and vs.strip().isdigit():
                    if int(vs) > 0:
                        acc.setdefault("offers_yes", [True])
                elif kl in ("isrecentlyadded", "newlyadded", "within90days", "isnewonplatform") and vs.lower() in (
                    "true",
                    "yes",
                    "1",
                ):
                    acc.setdefault("recent_90_yes", [True])
                elif (
                    any(
                        x in kl
                        for x in (
                            "ordercount",
                            "totalorders",
                            "orderscount",
                            "lifetimeorders",
                            "completedorders",
                            "deliveredorders",
                            "vendororders",
                            "restaurantorders",
                        )
                    )
                    or ("order" in kl and "count" in kl and "id" not in kl)
                ) and re.fullmatch(r"[\d,]+", vs.replace(" ", "")):
                    try:
                        n = int(vs.replace(",", "").replace(" ", ""))
                        if 0 <= n < 10**12:
                            acc.setdefault("order_counts", []).append(n)
                    except ValueError:
                        pass
                elif "@" in vs and (
                    "email" in kl or kl.endswith("mail") or "contactemail" in kl or kl == "e-mail"
                ):
                    acc.setdefault("emails", []).append(vs[:200])
                elif vs.startswith("http") and any(
                    x in kl
                    for x in (
                        "website",
                        "homepage",
                        "externallink",
                        "vendorurl",
                        "restauranturl",
                        "brandurl",
                    )
                ):
                    if "talabat." not in vs.lower():
                        acc.setdefault("external_websites", []).append(vs[:500])
                elif any(x in kl for x in ("vat", "trn", "taxnumber", "taxid", "licensenumber", "tradereg")) or (
                    "tax" in kl and "rate" not in kl and "id" in kl
                ):
                    if 4 < len(vs) < 120 and re.search(r"\d", vs):
                        acc.setdefault("tax_hints", []).append(vs[:120])
                elif ("description" in kl or "about" in kl or "bio" in kl) and "short" not in kl and len(vs) > 35:
                    acc.setdefault("descriptions", []).append(vs[:1200])
                elif any(p in vs.lower() for p in ("instagram.com", "facebook.com", "tiktok.com", "twitter.com", "x.com")):
                    acc.setdefault("social_urls", []).append(vs[:280])
                elif "cuisine" in kl or kl in ("tags", "labels", "categories"):
                    if len(vs) > 1:
                        acc.setdefault("cuisines", []).append(vs[:800])
                elif any(h in kl for h in ("areaname", "neighborhood", "districtname", "addressarea")) or (
                    "area" in kl and "id" not in kl and "wide" not in kl
                ):
                    if 2 < len(vs) < 200:
                        acc.setdefault("areas", []).append(vs)
                elif "deliveryfee" in kl or (kl.endswith("fee") and "delivery" in kl):
                    acc.setdefault("fees", []).append(vs[:120])
                elif "minorder" in kl or "minimumorder" in kl or "minimumbasket" in kl or kl == "minbasket":
                    acc.setdefault("mins", []).append(vs[:120])
                elif any(
                    h in kl
                    for h in (
                        "deliverytime",
                        "estimateddelivery",
                        "deliveryduration",
                        "timeslot",
                        "preparationtime",
                    )
                ):
                    acc.setdefault("etas", []).append(vs[:80])
                elif ("just" in kl and "land" in kl) and any(
                    x in kl for x in ("date", "since", "from", "at", "time")
                ):
                    if 2 < len(vs) < 160:
                        acc.setdefault("jl_dates", []).append(vs[:120])
                elif kl in ("isjustlanded", "justlanded", "justland", "newlisting") and vs.lower() in (
                    "true",
                    "yes",
                    "y",
                    "1",
                ):
                    acc.setdefault("jl_yes", [True])
                elif re.fullmatch(r"\d+(\.\d+)?", vs) and ("rating" in kl or "average" in kl or kl.startswith("avg")):
                    try:
                        x = float(vs)
                        if 0 <= x <= 5:
                            acc.setdefault("ratings", []).append(vs)
                    except ValueError:
                        pass
            elif isinstance(v, bool):
                if v and (
                    ("just" in kl and "land" in kl)
                    or kl in ("isjustlanded", "justlanded", "isnewlisting", "newonplatform")
                ):
                    acc.setdefault("jl_yes", [True])
                elif v and kl in ("isfreedelivery", "hasfreedelivery", "isfreedeliveryfee"):
                    acc.setdefault("free_delivery_yes", [True])
                elif v and ("pro" in kl and ("restaurant" in kl or kl.endswith("vendor"))) and "coupon" not in kl:
                    acc.setdefault("pro_yes", [True])
                elif v and (kl.startswith("ispro") or kl == "talabatpro"):
                    acc.setdefault("pro_yes", [True])
                elif v and ("preorder" in kl or kl == "ispreorderenabled"):
                    acc.setdefault("preorder_yes", [True])
                elif v and (("offer" in kl and "has" in kl) or kl == "hasactiveoffers"):
                    acc.setdefault("offers_yes", [True])
                elif v and ("recent" in kl or "newlisting" in kl) and "90" in kl:
                    acc.setdefault("recent_90_yes", [True])
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                fv = float(v)
                if "google" in kl and "rating" in kl and 0 <= fv <= 5:
                    acc.setdefault("google_ratings", []).append(str(fv).rstrip("0").rstrip("."))
                elif "google" in kl and "review" in kl and fv >= 0 and fv == int(fv) and fv < 10_000_000:
                    acc.setdefault("google_review_counts", []).append(str(int(fv)))
                elif ("rating" in kl or "average" in kl or kl.endswith("rating")) and 0 <= fv <= 5:
                    acc.setdefault("ratings", []).append(str(fv).rstrip("0").rstrip("."))
                elif (
                    "review" in kl
                    and ("count" in kl or "total" in kl or kl.endswith("reviews"))
                    and fv >= 0
                    and fv < 1_000_000
                    and fv == int(fv)
                ):
                    acc.setdefault("review_counts", []).append(str(int(fv)))
                elif ("restaurant" in kl and "id" in kl) or kl in ("restaurantid", "vendorid", "chainid", "shopid"):
                    if fv > 0:
                        acc.setdefault("restaurant_ids", []).append(str(int(fv)))
                elif ("branch" in kl and "id" in kl) or kl.endswith("branchid"):
                    if fv > 0:
                        acc.setdefault("branch_ids", []).append(str(int(fv)))
                elif (
                    "order" in kl
                    and "id" not in kl
                    and any(x in kl for x in ("count", "total", "volume", "lifetime", "completed", "delivered"))
                    and fv >= 0
                    and fv < 10**12
                    and fv == int(fv)
                ):
                    acc.setdefault("order_counts", []).append(int(fv))
                elif kl in ("lat", "latitude") and -90 < fv < 90:
                    acc.setdefault("lats", []).append(fv)
                elif kl in ("lng", "lon", "longitude") and -180 < fv < 180:
                    acc.setdefault("lngs", []).append(fv)
            elif isinstance(v, list):
                if any(x in kl for x in ("cuisine", "tag", "label", "category")):
                    parts: list[str] = []
                    for it in v:
                        if isinstance(it, str) and it.strip():
                            parts.append(it.strip())
                        elif isinstance(it, dict):
                            nm = it.get("name") or it.get("title") or it.get("label")
                            if nm:
                                parts.append(str(nm).strip())
                    if parts:
                        acc.setdefault("cuisines", []).append(", ".join(parts)[:800])
                elif "payment" in kl:
                    pay_parts: list[str] = []
                    for it in v:
                        if isinstance(it, str) and it.strip():
                            pay_parts.append(it.strip()[:100])
                        elif isinstance(it, dict):
                            t = it.get("name") or it.get("type") or it.get("title") or it.get("label")
                            if t:
                                pay_parts.append(str(t).strip()[:100])
                    if pay_parts:
                        acc.setdefault("payments", []).append(", ".join(dict.fromkeys(pay_parts))[:500])
                for it in v:
                    _walk_next_data_vendor_fields(it, acc)
            elif isinstance(v, dict):
                _walk_next_data_vendor_fields(v, acc)
    elif isinstance(obj, list):
        for it in obj:
            _walk_next_data_vendor_fields(it, acc)


def _finalize_vendor_enrichment(acc: dict[str, list]) -> dict[str, str | float | None]:
    phones = acc.get("phones", [])
    legal = acc.get("legal", [])
    out: dict[str, str | float | None] = {
        "contact_phone": _pick_best_phone(phones),
        "legal_name": max(legal, key=len) if legal else "",
        "cuisines": "",
        "rating": "",
        "reviews_count": "",
        "eta": "",
        "delivery_fee": "",
        "min_order": "",
        "area_label": "",
        "talabat_restaurant_id": "",
        "talabat_branch_id": "",
        "just_landed": "",
        "just_landed_date": "",
        "google_rating": "",
        "google_reviews_count": "",
        "rating_source": "",
        "highly_rated_google": "",
        "is_pro_vendor": "",
        "free_delivery": "",
        "delivered_by_talabat": "",
        "preorder_available": "",
        "payment_methods": "",
        "currency": "",
        "recently_added_90d": "",
        "has_offers": "",
        "estimated_orders": "",
        "vendor_website": "",
        "vendor_email": "",
        "vendor_social": "",
        "vendor_description": "",
        "tax_or_license_hint": "",
        "opening_hours_snippet": "",
        "lat": None,
        "lng": None,
    }
    cuisines = acc.get("cuisines", [])
    if cuisines:
        out["cuisines"] = max(cuisines, key=len)
    ratings = acc.get("ratings", [])
    for r in ratings:
        try:
            x = float(str(r).replace(",", "."))
            if 0 <= x <= 5:
                s = f"{x:.2f}".rstrip("0").rstrip(".")
                out["rating"] = s
                break
        except ValueError:
            continue
    rc = acc.get("review_counts", [])
    best_n = -1
    for s in rc:
        try:
            n = int(float(s))
            if n > best_n:
                best_n = n
        except ValueError:
            continue
    if best_n >= 0:
        out["reviews_count"] = str(best_n)
    etas = acc.get("etas", [])
    if etas:
        out["eta"] = max(etas, key=len)
    fees = acc.get("fees", [])
    if fees:
        out["delivery_fee"] = max(fees, key=len)
    mins = acc.get("mins", [])
    if mins:
        out["min_order"] = max(mins, key=len)
    areas = acc.get("areas", [])
    if areas:
        out["area_label"] = max(areas, key=len)
    rids = acc.get("restaurant_ids", [])
    if rids:
        out["talabat_restaurant_id"] = rids[-1]
    bids = acc.get("branch_ids", [])
    if bids:
        out["talabat_branch_id"] = bids[-1]
    lats = acc.get("lats", [])
    lngs = acc.get("lngs", [])
    if lats and lngs:
        out["lat"] = float(lats[0])
        out["lng"] = float(lngs[0])
    if acc.get("jl_yes"):
        out["just_landed"] = "yes"
    jl_dates = acc.get("jl_dates", [])
    if jl_dates:
        out["just_landed_date"] = max(jl_dates, key=len)[:120]
        if not out["just_landed"]:
            out["just_landed"] = "yes"

    g_rat = acc.get("google_ratings", [])
    if g_rat:
        out["google_rating"] = _best_rating_string(g_rat)
    grc = acc.get("google_review_counts", [])
    if grc:
        out["google_reviews_count"] = max(grc, key=len)[:40].strip()

    has_g = bool(out.get("google_rating"))
    has_t = bool(out.get("rating"))
    if has_g and has_t:
        out["rating_source"] = "mixed"
    elif has_g:
        out["rating_source"] = "google"
    elif has_t:
        out["rating_source"] = "talabat"
    else:
        out["rating_source"] = ""

    if out["google_rating"]:
        try:
            gx = float(str(out["google_rating"]).replace(",", "."))
            out["highly_rated_google"] = "yes" if gx > 4.0 else "no"
        except ValueError:
            out["highly_rated_google"] = ""

    out["is_pro_vendor"] = "yes" if acc.get("pro_yes") else ""
    out["free_delivery"] = "yes" if acc.get("free_delivery_yes") else ""
    out["delivered_by_talabat"] = "yes" if acc.get("delivered_talabat") else ""
    out["has_offers"] = "yes" if acc.get("offers_yes") else ""
    out["recently_added_90d"] = "yes" if acc.get("recent_90_yes") else ""

    if acc.get("preorder_yes"):
        out["preorder_available"] = "yes"
    else:
        pvs = acc.get("preorder_vals", [])
        if pvs:
            pv0 = str(pvs[0]).strip().lower()
            if pv0 in ("true", "yes", "y", "1"):
                out["preorder_available"] = "yes"
            elif pv0 in ("false", "no", "n", "0"):
                out["preorder_available"] = "no"

    pays = acc.get("payments", [])
    if pays:
        out["payment_methods"] = max(pays, key=len)[:500]

    cur = ""
    if acc.get("currencies"):
        cur = str(acc["currencies"][-1])[:6]
    if not cur:
        cur = _infer_currency(
            str(out.get("delivery_fee") or ""),
            str(out.get("min_order") or ""),
            str(out.get("eta") or ""),
        )
    out["currency"] = cur

    oc = acc.get("order_counts", [])
    if oc:
        out["estimated_orders"] = str(max(oc))

    emails = acc.get("emails", [])
    if emails:
        out["vendor_email"] = max(emails, key=len)[:200]
    ext = acc.get("external_websites", [])
    if ext:
        out["vendor_website"] = _pick_vendor_website([str(x) for x in ext])
    soc = acc.get("social_urls", [])
    if soc:
        out["vendor_social"] = " | ".join(dict.fromkeys(str(x) for x in soc))[:800]
    desc = acc.get("descriptions", [])
    if desc:
        out["vendor_description"] = max(desc, key=len)[:1500]
    tax = acc.get("tax_hints", [])
    if tax:
        out["tax_or_license_hint"] = max(tax, key=len)[:200]
    oh = acc.get("opening_hours_snippets", [])
    if oh:
        out["opening_hours_snippet"] = max(oh, key=len)[:800]

    return out


async def _fetch_vendor_page_enrichment(browser, url: str) -> dict[str, str | float | None]:
    """Open vendor page (English) and mine __NEXT_DATA__, tel: links, and vendor metadata."""
    ctx = await browser.new_context(**_vendor_browser_context_kwargs())
    page = await ctx.new_page()
    acc: dict[str, list] = {}
    try:
        await page.goto(url, wait_until=_vendor_goto_wait_until(), timeout=25000)
        await page.wait_for_timeout(600 if not _env_truthy(os.getenv("SCRAPER_HUMANIZE")) else 1100)
        html = await page.content()
        _merge_vendor_html_into_accumulator(html, acc)
    except Exception:
        pass
    finally:
        await ctx.close()

    # Second pass: managed scraper HTML (different IP / rendering) merges into the same acc.
    try:
        html_remote = await asyncio.to_thread(fetch_remote_vendor_html, url)
        _merge_vendor_html_into_accumulator(html_remote, acc)
    except Exception:
        pass

    return _finalize_vendor_enrichment(acc)


async def enrich_vendor_detail_pages(
    browser,
    records: list[RestaurantRecord],
    *,
    max_urls: int,
) -> None:
    """Visit vendor URLs and fill phone, legal name, IDs, ratings, fees, area, etc."""
    flag = os.getenv("SCRAPER_ENRICH_DETAILS", "1").strip().lower()
    if flag in ("0", "false", "no") or max_urls < 1 or not records:
        return
    by_url: dict[str, list[RestaurantRecord]] = {}
    for r in records:
        u = (r.restaurant_url or "").strip()
        if not u or "talabat.com" not in u:
            continue
        by_url.setdefault(u, []).append(r)
    urls = list(by_url.keys())[:max_urls]
    sem = asyncio.Semaphore(3)
    done = 0
    total = len(urls)
    logger.info("enrichment_start urls=%s rows=%s max_urls=%s", total, len(records), max_urls)

    def _apply(row: RestaurantRecord, d: dict[str, str | float | None]) -> None:
        if d.get("contact_phone"):
            row.contact_phone = str(d["contact_phone"])
        if d.get("legal_name"):
            row.legal_name = str(d["legal_name"])
        if d.get("talabat_restaurant_id"):
            row.talabat_restaurant_id = str(d["talabat_restaurant_id"])
        if d.get("talabat_branch_id"):
            row.talabat_branch_id = str(d["talabat_branch_id"])
        if d.get("cuisines"):
            row.cuisines = str(d["cuisines"])
        if d.get("rating"):
            row.rating = str(d["rating"])
        if d.get("reviews_count"):
            row.reviews_count = str(d["reviews_count"])
        if d.get("eta"):
            row.eta = str(d["eta"])
        if d.get("delivery_fee"):
            row.delivery_fee = str(d["delivery_fee"])
        if d.get("min_order"):
            row.min_order = str(d["min_order"])
        if d.get("area_label"):
            row.area_label = str(d["area_label"])
        if d.get("just_landed") == "yes":
            row.just_landed = "yes"
        if d.get("just_landed_date"):
            nd = str(d["just_landed_date"]).strip()
            if len(nd) > len((row.just_landed_date or "").strip()):
                row.just_landed_date = nd
        if d.get("google_rating"):
            row.google_rating = str(d["google_rating"])
        if d.get("google_reviews_count"):
            row.google_reviews_count = str(d["google_reviews_count"])
        if d.get("rating_source"):
            row.rating_source = str(d["rating_source"])
        if d.get("highly_rated_google"):
            row.highly_rated_google = str(d["highly_rated_google"])
        if d.get("is_pro_vendor"):
            row.is_pro_vendor = str(d["is_pro_vendor"])
        if d.get("free_delivery"):
            row.free_delivery = str(d["free_delivery"])
        if d.get("delivered_by_talabat"):
            row.delivered_by_talabat = str(d["delivered_by_talabat"])
        if d.get("preorder_available"):
            row.preorder_available = str(d["preorder_available"])
        if d.get("payment_methods"):
            pm = str(d["payment_methods"])
            if len(pm) > len((row.payment_methods or "").strip()):
                row.payment_methods = pm
        if d.get("currency"):
            row.currency = str(d["currency"])
        if d.get("recently_added_90d"):
            row.recently_added_90d = str(d["recently_added_90d"])
        if d.get("has_offers"):
            row.has_offers = str(d["has_offers"])
        if d.get("estimated_orders"):
            row.estimated_orders = str(d["estimated_orders"])
        lat, lng = d.get("lat"), d.get("lng")
        if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
            if -90 < float(lat) < 90 and -180 < float(lng) < 180:
                row.lat = float(lat)
                row.lng = float(lng)
        if d.get("vendor_website"):
            vw = str(d["vendor_website"]).strip()
            if len(vw) > len((row.vendor_website or "").strip()):
                row.vendor_website = vw
        if d.get("vendor_email"):
            ve = str(d["vendor_email"]).strip()
            if len(ve) > len((row.vendor_email or "").strip()):
                row.vendor_email = ve
        if d.get("vendor_social"):
            vs = str(d["vendor_social"]).strip()
            if len(vs) > len((row.vendor_social or "").strip()):
                row.vendor_social = vs
        if d.get("vendor_description"):
            vd = str(d["vendor_description"]).strip()
            if len(vd) > len((row.vendor_description or "").strip()):
                row.vendor_description = vd
        if d.get("tax_or_license_hint"):
            th = str(d["tax_or_license_hint"]).strip()
            if len(th) > len((row.tax_or_license_hint or "").strip()):
                row.tax_or_license_hint = th
        if d.get("opening_hours_snippet"):
            ohs = str(d["opening_hours_snippet"]).strip()
            if len(ohs) > len((row.opening_hours_snippet or "").strip()):
                row.opening_hours_snippet = ohs

    async def one(u: str) -> None:
        nonlocal done
        async with sem:
            d = await _fetch_vendor_page_enrichment(browser, u)
            for row in by_url[u]:
                _apply(row, d)
            done += 1
            logger.info("enrichment_progress %s/%s url=%s", done, total, u)

    await asyncio.gather(*[one(u) for u in urls])
    logger.info("enrichment_done urls=%s rows=%s", total, len(records))


def _union_listing_batches(*batches: list[RestaurantRecord]) -> list[RestaurantRecord]:
    """Merge listing extracts from the same page visit (e.g. before + after scroll) without duplicate URLs."""
    seen: set[str] = set()
    out: list[RestaurantRecord] = []
    for batch in batches:
        for r in batch:
            u = (r.restaurant_url or "").strip().split("?")[0].rstrip("/").lower()
            key = u if u else r.branch_sku
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
    return out


def _listing_fast_path_enabled() -> bool:
    """If true, return as soon as the first DOM parse finds links (skips scroll — often ~40 rows only)."""
    return os.getenv("SCRAPER_LISTING_FAST_PATH", "0").strip().lower() in ("1", "true", "yes", "y", "on")


def _env_truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


_DEFAULT_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def _scraper_user_agent() -> str:
    return (os.getenv("SCRAPER_USER_AGENT") or "").strip() or _DEFAULT_CHROME_UA


def _extra_http_headers_merge() -> dict[str, str]:
    """Browser-like headers for listing/vendor navigation (override/extend via SCRAPER_EXTRA_HTTP_HEADERS_JSON)."""
    headers: dict[str, str] = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    raw = (os.getenv("SCRAPER_EXTRA_HTTP_HEADERS_JSON") or "").strip()
    if not raw:
        return headers
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, str):
                    headers[k] = v
    except json.JSONDecodeError:
        logger.warning("SCRAPER_EXTRA_HTTP_HEADERS_JSON is not valid JSON; ignoring")
    return headers


def _listing_goto_wait_until() -> str:
    v = (os.getenv("SCRAPER_LISTING_GOTO_WAIT_UNTIL") or "domcontentloaded").strip().lower()
    if v not in ("commit", "domcontentloaded", "load", "networkidle"):
        return "domcontentloaded"
    return v


def _vendor_goto_wait_until() -> str:
    v = (os.getenv("SCRAPER_VENDOR_GOTO_WAIT_UNTIL") or "domcontentloaded").strip().lower()
    if v not in ("commit", "domcontentloaded", "load", "networkidle"):
        return "domcontentloaded"
    return v


def _post_navigation_wait_ms() -> int:
    base = int(os.getenv("SCRAPER_POST_NAV_WAIT_MS", "2200"))
    if _env_truthy(os.getenv("SCRAPER_HUMANIZE")):
        base = max(base, int(os.getenv("SCRAPER_POST_NAV_WAIT_MS_HUMANIZE", "3800")))
        base += random.randint(0, max(0, int(os.getenv("SCRAPER_HUMANIZE_JITTER_MS", "900"))))
    return base


def _listing_browser_context_kwargs(sample_lat: float, sample_lng: float) -> dict[str, Any]:
    tz = (os.getenv("SCRAPER_TIMEZONE_ID") or "Asia/Dubai").strip()
    w = int(os.getenv("SCRAPER_VIEWPORT_W", "1440"))
    h = int(os.getenv("SCRAPER_VIEWPORT_H", "900"))
    return {
        "geolocation": {"latitude": float(sample_lat), "longitude": float(sample_lng)},
        "permissions": ["geolocation"],
        "locale": "en-US",
        "timezone_id": tz,
        "user_agent": _scraper_user_agent(),
        "viewport": {"width": w, "height": h},
        "device_scale_factor": float(os.getenv("SCRAPER_DEVICE_SCALE", "1") or "1"),
        "color_scheme": "light",
        "has_touch": False,
        "extra_http_headers": _extra_http_headers_merge(),
    }


def _vendor_browser_context_kwargs() -> dict[str, Any]:
    tz = (os.getenv("SCRAPER_TIMEZONE_ID") or "Asia/Dubai").strip()
    return {
        "locale": "en-US",
        "timezone_id": tz,
        "user_agent": _scraper_user_agent(),
        "viewport": {"width": 1280, "height": 800},
        "device_scale_factor": float(os.getenv("SCRAPER_DEVICE_SCALE", "1") or "1"),
        "color_scheme": "light",
        "has_touch": False,
        "extra_http_headers": _extra_http_headers_merge(),
    }


def _listing_url_with_page_param(base_url: str, page_num: int) -> str:
    """Append/replace ``page=`` query (same pattern as legacy area listing scrapers)."""
    if page_num <= 1:
        return base_url
    if "?" in base_url:
        if re.search(r"[?&]page=\d+", base_url):
            return re.sub(r"page=\d+", f"page={page_num}", base_url, count=1)
        return f"{base_url}&page={page_num}"
    return f"{base_url}?page={page_num}"


async def _detect_listing_last_page_number(page) -> int:
    """Read Talabat listing pagination (``ul[data-test='pagination']``) when present."""
    try:
        pagination = await page.query_selector("ul[data-test='pagination']")
        if not pagination:
            return 1
        items = await pagination.query_selector_all("li[data-testid='paginate-link']")
        if not items or len(items) < 2:
            return 1
        last_page_item = items[-2]
        link = await last_page_item.query_selector("a[page]")
        if not link:
            return 1
        attr = await link.get_attribute("page")
        if attr and str(attr).isdigit():
            return max(1, int(attr))
    except Exception:
        return 1
    return 1


def radius_slack_km(radius_km: float) -> float:
    return max(0.15, min(2.0, float(radius_km) * 0.04))


def compute_radius_stats(
    df: pd.DataFrame,
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
) -> tuple[pd.Series, dict[str, int | float]]:
    """Per-row distance from pin plus counts used in scrape_run_meta and tests."""
    slack = radius_slack_km(radius_km)
    if df.empty or "lat" not in df.columns or "lng" not in df.columns:
        return pd.Series(dtype=float), {
            "rows_with_coordinates": 0,
            "rows_missing_coordinates": 0,
            "inside_radius_row_count": 0,
            "outside_radius_row_count": 0,
            "radius_slack_km": round(slack, 4),
        }
    latnum = pd.to_numeric(df["lat"], errors="coerce")
    lngnum = pd.to_numeric(df["lng"], errors="coerce")
    valid = latnum.notna() & lngnum.notna()
    d = haversine_series_km_from_pin(float(pin_lat), float(pin_lng), latnum, lngnum)
    r = float(radius_km)
    inside = valid & (d <= r + slack)
    outside = valid & (d > r + slack)
    stats: dict[str, int | float] = {
        "rows_with_coordinates": int(valid.sum()),
        "rows_missing_coordinates": int((~valid).sum()),
        "inside_radius_row_count": int(inside.sum()),
        "outside_radius_row_count": int(outside.sum()),
        "radius_slack_km": round(slack, 4),
    }
    return d, stats


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
    *,
    listing_cuisine_sweep: bool = False,
) -> list[RestaurantRecord]:
    # Optional ``?page=`` pagination for hub listing URLs (similar idea to community listing scrapers that
    # walk ``ul[data-test='pagination']`` — e.g. github.com/Dataloops-code/Talabat-Restaurants1-Scraper-python).
    paginate_listings = _env_truthy(os.getenv("SCRAPER_LISTING_PAGE_PAGINATION"))
    max_listing_pages = max(1, int(os.getenv("SCRAPER_LISTING_MAX_PAGES", "25")))
    page_gap_sec = max(0.0, float(os.getenv("SCRAPER_LISTING_PAGE_GAP_SEC", "1.5")))
    context = await browser.new_context(**_listing_browser_context_kwargs(sample_lat, sample_lng))
    page = await context.new_page()
    listing_urls = capped_listing_urls(listing_cuisine_sweep)
    allow_fast = _listing_fast_path_enabled() and not listing_cuisine_sweep and not paginate_listings
    merged_all: list[RestaurantRecord] = []
    strict = _env_truthy(os.getenv("SCRAPER_STRICT_LISTING_ERRORS"))
    post_nav_ms = _post_navigation_wait_ms()
    goto_until = _listing_goto_wait_until()
    try:
        for listing_url in listing_urls:
            try:
                await page.goto(listing_url, wait_until=goto_until, timeout=60000)
                await page.wait_for_timeout(post_nav_ms)
                await dismiss_common_overlays(page)
                await click_just_landed_if_requested(page, just_landed_only)
                rows_pre = await extract_restaurants(page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng)
                if allow_fast and rows_pre:
                    return rows_pre
                await auto_scroll(page, rounds=scroll_rounds, wait_ms=scroll_wait_ms)
                rows_post = await extract_restaurants(page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng)
                block = _union_listing_batches(rows_pre, rows_post)
                merged_all = _union_listing_batches(merged_all, block)

                if paginate_listings:
                    last_page = await _detect_listing_last_page_number(page)
                    last_page = min(last_page, max_listing_pages)
                    if last_page > 1:
                        logger.info(
                            "listing_pagination url=%s sample=(%.5f,%.5f) pages=%s (cap=%s)",
                            listing_url,
                            sample_lat,
                            sample_lng,
                            last_page,
                            max_listing_pages,
                        )
                    for pnum in range(2, last_page + 1):
                        purl = _listing_url_with_page_param(listing_url, pnum)
                        await page.goto(purl, wait_until=goto_until, timeout=60000)
                        await page.wait_for_timeout(post_nav_ms)
                        await dismiss_common_overlays(page)
                        await click_just_landed_if_requested(page, just_landed_only)
                        rows_pre2 = await extract_restaurants(page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng)
                        await auto_scroll(page, rounds=scroll_rounds, wait_ms=scroll_wait_ms)
                        rows_post2 = await extract_restaurants(page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng)
                        block2 = _union_listing_batches(rows_pre2, rows_post2)
                        merged_all = _union_listing_batches(merged_all, block2)
                        if page_gap_sec > 0:
                            await asyncio.sleep(page_gap_sec)
            except Exception as exc:
                logger.warning("listing scrape failed url=%s sample=(%.5f,%.5f): %s", listing_url, sample_lat, sample_lng, exc)
                if strict:
                    raise
                continue
        return merged_all
    except Exception as exc:
        logger.error("scrape_one_point aborted sample=(%.5f,%.5f): %s", sample_lat, sample_lng, exc)
        if strict:
            raise
        return merged_all
    finally:
        await context.close()


def _canonical_vendor_url(url: str) -> str:
    u = (url or "").strip().split("?")[0].rstrip("/").lower()
    return u


def _pick_better_row(
    pin_lat: float,
    pin_lng: float,
    a: RestaurantRecord,
    b: RestaurantRecord,
) -> RestaurantRecord:
    """When the same vendor URL appears from multiple grid samples, keep one row for the pin."""
    da = haversine_km(pin_lat, pin_lng, a.source_sample_lat, a.source_sample_lng)
    db = haversine_km(pin_lat, pin_lng, b.source_sample_lat, b.source_sample_lng)
    if db + 1e-6 < da:
        return b
    if da + 1e-6 < db:
        return a

    def meta_score(r: RestaurantRecord) -> int:
        n = sum(
            1
            for x in (
                r.contact_phone,
                r.rating,
                r.cuisines,
                r.reviews_count,
                r.talabat_restaurant_id,
            )
            if (x or "").strip()
        )
        if r.just_landed == "yes":
            n += 1
        if (r.just_landed_date or "").strip():
            n += 1
        for x in (
            r.google_rating,
            r.google_reviews_count,
            r.is_pro_vendor,
            r.free_delivery,
            r.delivered_by_talabat,
            r.payment_methods,
            r.currency,
            r.estimated_orders,
            r.google_place_id,
            r.google_maps_name,
            r.vendor_website,
            r.vendor_email,
            r.vendor_description,
            r.reverse_geocode_address,
        ):
            if (x or "").strip():
                n += 1
        return n

    return b if meta_score(b) > meta_score(a) else a


def _listing_scroll_params(rounds: int, wait_ms: int) -> tuple[int, int]:
    """Tune scroll depth: env overrides, optional aggressive floor for more listing URLs (slower)."""
    r, w = rounds, wait_ms
    er = os.getenv("SCRAPER_LISTING_SCROLL_ROUNDS")
    if er:
        try:
            r = int(er.strip())
        except ValueError:
            pass
    ew = os.getenv("SCRAPER_LISTING_SCROLL_WAIT_MS")
    if ew:
        try:
            w = int(ew.strip())
        except ValueError:
            pass
    if os.getenv("SCRAPER_AGGRESSIVE_LISTING", "").strip().lower() in ("1", "true", "yes", "y", "on"):
        try:
            r = max(r, int(os.getenv("SCRAPER_LISTING_SCROLL_ROUNDS_AGGRESSIVE", "38")))
        except ValueError:
            r = max(r, 38)
        try:
            w = max(w, int(os.getenv("SCRAPER_LISTING_SCROLL_WAIT_MS_AGGRESSIVE", "1100")))
        except ValueError:
            w = max(w, 1100)
    return max(1, r), max(200, w)


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
    max_sample_points: int | None = None,
    *,
    dedupe_by_vendor_url: bool = False,
    scrape_city: str = "",
    high_volume: bool = False,
    scrape_target_label: str = "",
    meta_out: dict | None = None,
    google_places_enrich: bool | None = None,
) -> pd.DataFrame:
    last_step = "init"
    hv = bool(high_volume) or _env_truthy(os.getenv("SCRAPER_HIGH_VOLUME"))
    resolved_area = ""
    if meta_out is not None:
        meta_out.setdefault("effective_scrape_pin_lat", round(float(pin_lat), 6))
        meta_out.setdefault("effective_scrape_pin_lng", round(float(pin_lng), 6))
        meta_out.setdefault("effective_radius_km", float(radius_km))
        meta_out.setdefault("last_completed_step", last_step)
        meta_out["radius_km"] = float(radius_km)
        meta_out["scrape_city_label"] = (scrape_city or "").strip()
        meta_out["scrape_target_label"] = (scrape_target_label or "").strip()
        meta_out["listing_entry_urls_sample"] = capped_listing_urls(hv)[:6]
        meta_out["high_volume_mode"] = bool(hv)
        meta_out["scraper_humanize"] = _env_truthy(os.getenv("SCRAPER_HUMANIZE"))
        meta_out["google_places_enrich_request"] = google_places_enrich
        meta_out["google_places_enrich_effective"] = google_places_enrich_effective(google_places_enrich)
        try:
            resolved_area = await asyncio.to_thread(resolve_pin_area_label, float(pin_lat), float(pin_lng))
        except Exception:
            resolved_area = ""
        meta_out["resolved_area_nearest"] = resolved_area

    if max_sample_points is not None:
        max_pts = max_sample_points
    else:
        max_pts = int(os.getenv("MAX_SCRAPE_SAMPLE_POINTS", "6"))
    if hv:
        last_step = "build_dense_grid"
        if meta_out is not None:
            meta_out["last_completed_step"] = last_step
        cap_c = int(os.getenv("SCRAPER_MAX_SAMPLE_POINTS_CAP", "220"))
        max_pts = max(max_pts, int(os.getenv("SCRAPER_HIGH_VOLUME_MIN_SAMPLES", "90")))
        max_pts = min(max_pts, cap_c)
        target = int(os.getenv("SCRAPER_MIN_GRID_POINTS", "90"))
        floor = float(os.getenv("SCRAPER_SPACING_FLOOR_KM", "0.38"))
        start_sp = min(float(spacing_km), float(os.getenv("SCRAPER_HIGH_VOLUME_START_SPACING_KM", "1.05")))
        points = refine_grid_spacing(
            pin_lat,
            pin_lng,
            radius_km,
            start_sp,
            target_count=min(target, cap_c),
            spacing_floor=floor,
        )
    else:
        last_step = "build_grid"
        if meta_out is not None:
            meta_out["last_completed_step"] = last_step
        points = generate_points_in_radius(pin_lat, pin_lng, radius_km, spacing_km)
    # Multiple geolocation samples merge different "near me" listing slices (Talabat caps each view ~100–200).
    points = _cap_sample_points(points, max_pts)
    if meta_out is not None:
        meta_out["grid_size"] = int(len(points))
        meta_out["last_completed_step"] = "grid_capped"
    scroll_rounds, scroll_wait_ms = _listing_scroll_params(scroll_rounds, scroll_wait_ms)
    if _env_truthy(os.getenv("SCRAPER_HUMANIZE")):
        scroll_wait_ms = int(scroll_wait_ms * float(os.getenv("SCRAPER_HUMANIZE_SCROLL_WAIT_MULT", "1.12")))
    sem = asyncio.Semaphore(concurrency)
    records: list[RestaurantRecord] = []
    raw_listing_count = 0

    logger.info(
        "scrape pin=(%.5f,%.5f) r=%.2fkm grid_pts=%d resolved_area=%r hv=%s",
        pin_lat,
        pin_lng,
        radius_km,
        len(points),
        resolved_area,
        hv,
    )

    browser = None
    try:
        last_step = "launch_playwright"
        if meta_out is not None:
            meta_out["last_completed_step"] = last_step
        async with async_playwright() as p:
            last_step = "launch_browser"
            if meta_out is not None:
                meta_out["last_completed_step"] = last_step
            browser = await p.chromium.launch(
                headless=True,
                args=CHROMIUM_LAUNCH_ARGS,
            )
            done = 0
            total = len(points)
            per_point_timeout = max(
                30.0,
                float(os.getenv("SCRAPER_PER_POINT_TIMEOUT_SEC", "90")),
            )

            async def worker(pt: tuple[float, float]) -> list[RestaurantRecord]:
                nonlocal done
                lat, lng = pt
                async with sem:
                    try:
                        rows = await asyncio.wait_for(
                            scrape_one_point(
                                browser=browser,
                                pin_lat=pin_lat,
                                pin_lng=pin_lng,
                                radius_km=radius_km,
                                sample_lat=lat,
                                sample_lng=lng,
                                just_landed_only=just_landed_only,
                                scroll_rounds=scroll_rounds,
                                scroll_wait_ms=scroll_wait_ms,
                                listing_cuisine_sweep=hv,
                            ),
                            timeout=per_point_timeout,
                        )
                    except TimeoutError:
                        logger.warning(
                            "sample_timeout sample=(%.5f,%.5f) timeout=%ss",
                            lat,
                            lng,
                            per_point_timeout,
                        )
                        rows = []
                done += 1
                if progress_cb:
                    progress_cb(done, total, lat, lng, len(rows))
                return rows

            last_step = "scrape_grid_points"
            if meta_out is not None:
                meta_out["last_completed_step"] = last_step
            batches = await asyncio.gather(*[worker(pt) for pt in points])

            last_step = "merge_batches"
            if meta_out is not None:
                meta_out["last_completed_step"] = last_step
            if dedupe_by_vendor_url:
                dedupe: dict[str, RestaurantRecord] = {}
                for batch in batches:
                    for r in batch:
                        ck = _canonical_vendor_url(r.restaurant_url)
                        if not ck:
                            dedupe[r.branch_sku] = r
                            continue
                        cur = dedupe.get(ck)
                        if cur is None:
                            dedupe[ck] = r
                        else:
                            dedupe[ck] = _pick_better_row(pin_lat, pin_lng, cur, r)
                records = list(dedupe.values())
            else:
                records = []
                for batch in batches:
                    records.extend(batch)

            raw_listing_count = len(records)
            if meta_out is not None:
                meta_out["raw_listing_row_count"] = raw_listing_count

            city_tag = (scrape_city or "").strip()
            if city_tag:
                for r in records:
                    r.scrape_city = city_tag
            tgt = (scrape_target_label or "").strip()
            if tgt:
                tv = tgt[:240]
                for r in records:
                    r.scrape_target_label = tv
            # Scale down vendor-page enrichment when many grid points (each listing + N enrich URLs is costly on Render).
            enrich_cap = int(os.getenv("RESTAURANT_DETAIL_ENRICH_MAX", "12"))
            n_pts = max(1, len(points))
            budget = max(3, 22 // n_pts)
            if float(radius_km) >= float(os.getenv("SCRAPER_BIG_RADIUS_KM", "18")):
                budget = min(budget, int(os.getenv("SCRAPER_BIG_RADIUS_ENRICH_BUDGET", "2")))
            enrich_max = min(enrich_cap, budget)
            if meta_out is not None:
                meta_out["enrich_max_urls"] = int(enrich_max)
                meta_out["last_completed_step"] = "enrichment_start"
            await enrich_vendor_detail_pages(browser, records, max_urls=enrich_max)
            if meta_out is not None:
                meta_out["last_completed_step"] = "enrichment_done"
            await browser.close()
            browser = None
    except Exception as exc:
        if meta_out is not None:
            meta_out["last_completed_step"] = last_step
            meta_out["pipeline_error"] = str(exc)[:500]
        logger.error(
            "run_area_scrape_failed pin=(%.5f,%.5f) radius=%.2f grid=%s raw=%s step=%s\n%s",
            pin_lat,
            pin_lng,
            float(radius_km),
            (meta_out or {}).get("grid_size"),
            raw_listing_count,
            last_step,
            traceback.format_exc(),
        )
        raise
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                logger.warning("browser_close_failed step=%s", last_step)

    if meta_out is not None:
        meta_out["last_completed_step"] = "post_enrichment"
    enrich_records_with_google_places(records, force=google_places_enrich)
    if meta_out is not None:
        meta_out["last_completed_step"] = "google_places_done"
    enrich_records_reverse_geocode(records)
    if meta_out is not None:
        meta_out["last_completed_step"] = "reverse_geocode_done"

    df = pd.DataFrame([r.to_dict() for r in records])
    if df.empty:
        if meta_out is not None:
            meta_out["rows_before_radius_filter"] = 0
            meta_out["rows_after_radius_filter"] = 0
            meta_out["rows_excluded_outside_radius"] = 0
            meta_out["rows_with_coordinates"] = 0
            meta_out["rows_missing_coordinates"] = 0
            meta_out["inside_radius_row_count"] = 0
            meta_out["outside_radius_row_count"] = 0
        logger.info("scrape done empty raw_listing=%s", raw_listing_count)
        return df

    before_radius = int(len(df))
    dist_series, rad_stats = compute_radius_stats(df, pin_lat, pin_lng, radius_km)
    df = df.assign(distance_km_from_pin=dist_series)
    slack = float(rad_stats["radius_slack_km"])
    r_cap = float(radius_km) + slack
    inside_mask = pd.to_numeric(df["distance_km_from_pin"], errors="coerce") <= r_cap
    df_in = df.loc[inside_mask].copy()
    df_in["distance_km_from_pin"] = pd.to_numeric(df_in["distance_km_from_pin"], errors="coerce").round(3)

    latnum = pd.to_numeric(df["lat"], errors="coerce")
    lngnum = pd.to_numeric(df["lng"], errors="coerce")
    valid = latnum.notna() & lngnum.notna()
    sample_idx = df.index[valid][:8]
    for idx in sample_idx:
        row = df.loc[idx]
        try:
            dkm = float(row["distance_km_from_pin"])
            logger.info(
                "branch_coord_sample name=%r lat=%s lng=%s distance_km_from_pin=%.3f",
                (str(row.get("restaurant_name", "")) or "")[:80],
                row.get("lat"),
                row.get("lng"),
                dkm,
            )
        except (TypeError, ValueError):
            logger.info(
                "branch_coord_sample name=%r lat=%s lng=%s distance_km_from_pin=nan",
                (str(row.get("restaurant_name", "")) or "")[:80],
                row.get("lat"),
                row.get("lng"),
            )

    df = df_in
    if meta_out is not None:
        meta_out["rows_before_radius_filter"] = before_radius
        meta_out["radius_slack_km"] = slack
        meta_out["rows_after_radius_filter"] = int(len(df))
        meta_out["rows_excluded_outside_radius"] = int(before_radius - len(df))
        meta_out["rows_with_coordinates"] = rad_stats["rows_with_coordinates"]
        meta_out["rows_missing_coordinates"] = rad_stats["rows_missing_coordinates"]
        meta_out["inside_radius_row_count"] = rad_stats["inside_radius_row_count"]
        meta_out["outside_radius_row_count"] = rad_stats["outside_radius_row_count"]
    logger.info(
        "scrape raw_listing=%s before_radius=%s after_radius=%s excluded_radius=%s "
        "coords=%s inside=%s outside=%s missing_coords=%s",
        raw_listing_count,
        before_radius,
        len(df),
        before_radius - len(df),
        rad_stats["rows_with_coordinates"],
        rad_stats["inside_radius_row_count"],
        rad_stats["outside_radius_row_count"],
        rad_stats["rows_missing_coordinates"],
    )

    if just_landed_only:
        jl = df["just_landed"].astype(str).str.lower().eq("yes")
        ra = df["recently_added_90d"].astype(str).str.lower().eq("yes")
        df = df[jl | ra].copy()
        if df.empty:
            return df.reset_index(drop=True)
    if status_filter == "closed":
        df = df[df["status"] == "closed"].copy()
    elif status_filter == "live":
        # Listing scrape rarely yields status=="live"; treat "live" as "not closed".
        df = df[df["status"] != "closed"].copy()
    return df.reset_index(drop=True)
