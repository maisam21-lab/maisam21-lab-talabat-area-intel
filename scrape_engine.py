from __future__ import annotations

import asyncio
import json
import math
import os
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from playwright.async_api import async_playwright

from geo_utils import generate_points_in_radius, haversine_km
from models import RestaurantRecord, make_branch_sku
from next_data_extract import normalize_talabat_url, parse_next_data_script, paths_from_next_data_json
from places_enrich import enrich_records_with_google_places

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
                scrape_city="",
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
                scrape_city="",
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
                    scrape_city="",
                    lat=lat,
                    lng=lng,
                )
            )
    return results


_TEL_HREF_RE = re.compile(r"""href=["']tel:([^"'\s>]+)""", re.I)
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

    return out


async def _fetch_vendor_page_enrichment(browser, url: str) -> dict[str, str | float | None]:
    """Open vendor page (English) and mine __NEXT_DATA__, tel: links, and vendor metadata."""
    ctx = await browser.new_context(
        locale="en-US",
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        viewport={"width": 1280, "height": 800},
    )
    page = await ctx.new_page()
    acc: dict[str, list] = {}
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await page.wait_for_timeout(600)
        html = await page.content()
        for m in _TEL_HREF_RE.finditer(html):
            acc.setdefault("phones", []).append(m.group(1).strip())
        raw = await page.evaluate(
            """() => {
          const el = document.getElementById('__NEXT_DATA__');
          return el ? el.textContent : null;
        }"""
        )
        if raw:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = None
            if isinstance(data, dict):
                _walk_next_data_vendor_fields(data, acc)
    except Exception:
        pass
    finally:
        await ctx.close()
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

    async def one(u: str) -> None:
        async with sem:
            d = await _fetch_vendor_page_enrichment(browser, u)
            for row in by_url[u]:
                _apply(row, d)

    await asyncio.gather(*[one(u) for u in urls])


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
                await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(2200)
                await dismiss_common_overlays(page)
                await click_just_landed_if_requested(page, just_landed_only)
                # Fast path: vendor links often appear without long scroll (saves Render gateway timeouts).
                rows = await extract_restaurants(page, pin_lat, pin_lng, radius_km, sample_lat, sample_lng)
                if rows:
                    return rows
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
) -> pd.DataFrame:
    points = generate_points_in_radius(pin_lat, pin_lng, radius_km, spacing_km)
    # Multiple samples merge different listing slices; default 3 balances area coverage vs Render timeouts.
    if max_sample_points is not None:
        max_pts = max_sample_points
    else:
        max_pts = int(os.getenv("MAX_SCRAPE_SAMPLE_POINTS", "2"))
    points = _cap_sample_points(points, max_pts)
    scroll_rounds, scroll_wait_ms = _listing_scroll_params(scroll_rounds, scroll_wait_ms)
    sem = asyncio.Semaphore(concurrency)
    records: list[RestaurantRecord] = []

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

        city_tag = (scrape_city or "").strip()
        if city_tag:
            for r in records:
                r.scrape_city = city_tag
        # Scale down vendor-page enrichment when many grid points (each listing + N enrich URLs is costly on Render).
        enrich_cap = int(os.getenv("RESTAURANT_DETAIL_ENRICH_MAX", "12"))
        n_pts = max(1, len(points))
        budget = max(3, 22 // n_pts)
        enrich_max = min(enrich_cap, budget)
        await enrich_vendor_detail_pages(browser, records, max_urls=enrich_max)
        await browser.close()

    enrich_records_with_google_places(records)

    df = pd.DataFrame([r.to_dict() for r in records])
    if df.empty:
        return df
    if status_filter == "closed":
        df = df[df["status"] == "closed"].copy()
    elif status_filter == "live":
        # Listing scrape rarely yields status=="live"; treat "live" as "not closed".
        df = df[df["status"] != "closed"].copy()
    return df.reset_index(drop=True)
