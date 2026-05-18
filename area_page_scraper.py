"""
Talabat area-based restaurant scraper using __NEXT_DATA__ pagination.

No Playwright required — each page is a plain HTTP GET that returns server-rendered JSON
in a <script id="__NEXT_DATA__"> tag.

URL pattern:
  https://www.talabat.com/{country}/restaurants/{area_id}/{area_slug}?page={n}

Each page returns 15 vendors. Total vendor count is in __NEXT_DATA__ → props.pageProps.data.totalVendors.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from typing import Any

import requests

from geo_utils import haversine_km

logger = logging.getLogger("talabat_area_intel.area_page_scraper")

_BASE = "https://www.talabat.com"
_NEXT_DATA_RE = re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.DOTALL)
_PAGE_SIZE = 15  # Talabat hard-codes 15 vendors per page regardless of size param

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Known Talabat area registry for UAE.
# Format: key -> (area_id, slug, center_lat, center_lng)
# Extend this table as new area IDs are discovered (use discover_area_id_for_name()).
UAE_AREA_REGISTRY: dict[str, tuple[int, str, float, float]] = {
    # Dubai — confirmed area IDs from scan
    "business_bay":          (1252, "business-bay",              25.18252, 55.27431),
    "bur_dubai":             (1250, "bur-dubai",                 25.22976, 55.30612),
    "difc":                  (1256, "difc",                      25.21045, 55.27738),
    "downtown_dubai":        (1258, "downtown-burj-khalifa",     25.19483, 55.27511),
    "deira":                 (1255, "deira",                     25.27709, 55.32801),
    "dubai_festival_city":   (1263, "dubai-festival-city",       25.21751, 55.35699),
    "dubai_healthcare_city": (1264, "dubai-healthcare-city",     25.23265, 55.32245),
    "dubai_internet_city":   (1266, "dubai-internet-city-dic",   25.09577, 55.16272),
    "dubai_marina":          (1272, "dubai-marina",              25.08529, 55.14476),
    "dubai_motor_city":      (1276, "dubai-motor-city",          25.04738, 55.23851),
    "dubai_silicon_oasis":   (1277, "dubai-silicon-oasis",       25.12194, 55.38277),
    "impz":                  (1291, "impz",                      25.03498, 55.18968),
    "international_city":    (1292, "international-city",        25.16455, 55.40980),
    "jlt":                   (1308, "jumeirah-lakes-towers-jlt", 25.07466, 55.14393),
    "jumeirah":              (1301, "jumeirah",                  25.20983, 55.24832),
    "knowledge_village":     (1270, "knowledge-village",         25.10367, 55.16297),
    "mirdif":                (1315, "mirdif",                    25.22425, 55.42002),
    "satwa":                 (1341, "satwa",                     25.22170, 55.27447),
    "barsha_heights":        (1350, "barsha-heights-tecom",      25.09639, 55.17603),
    "um_al_sheif":           (1353, "um-al-sheif",               25.13245, 55.20486),
    "umm_suqeim":            (1356, "umm-suqeim",                25.15367, 55.20560),
    "warsan_1":              (1363, "warsan-1",                  25.15406, 55.46135),
    "jebel_ali_1":           (1293, "jebel-ali-1",               25.02641, 55.12178),
    "studio_city":           (1349, "dubai-studio-city",         25.04197, 55.25218),
    # Sharjah — confirmed area IDs
    "sharjah_al_majaz":      (1530, "al-majaz-1",                25.33753, 55.38714),
    "sharjah_muwaileh":      (1581, "muwaileh-commercial",       25.29911, 55.45621),
    "sharjah_al_nahda":      (1540, "al-nahda",                  25.30273, 55.37457),
    "sharjah_al_taawun":     (1562, "al-taawun",                 25.30882, 55.37333),
    # Abu Dhabi — confirmed area IDs
    "auh_reem_island":       (1492, "reem-island",               24.49326, 54.40687),  # COL / Addax Tower
    "auh_al_wahdah":         (1467, "al-wahdah",                 24.46592, 54.37820),  # Al Nahyan
    "auh_al_markaziyah":     (1448, "al-markaziyah",             24.49255, 54.36395),
    "auh_al_khalidiyah":     (1441, "al-khalidiyah",             24.46580, 54.34829),
    "auh_al_karamah":        (1440, "al-karamah",                24.46693, 54.36919),
    "auh_zayed_sports_city": (1443, "zayed-sports-city",         24.41794, 54.45562),
    "auh_madinat_khalifa_a": (1479, "madinat-khalifa-a",         24.42138, 54.56823),  # nearest to Raha (1)-EK
    "auh_al_raha_bandar":    (2060, "al-bandar-al-raha",         24.45129, 54.59969),
    "auh_al_raha_muneera":   (2059, "al-muneera-al-raha",        24.45045, 54.60514),
    "auh_al_shamkha":        (1464, "al-shamkha",                24.38824, 54.70281),  # Shamkha (2)
    "auh_al_shawamekh":      (1465, "al-shawamekh",              24.35509, 54.65750),
    "auh_gate_city":         (1487, "abu-dhabi-gate-city",       24.39418, 54.49991),
    # Al Ain — area IDs not yet discovered (scanning additional ranges)
}

# Aliases so callers can pass common name variants
_AREA_ALIASES: dict[str, str] = {
    "business bay":         "business_bay",
    "businessbay":          "business_bay",
    "downtown":             "downtown_dubai",
    "downtown dubai":       "downtown_dubai",
    "marina":               "dubai_marina",
    "dubai marina":         "dubai_marina",
    "jumeirah lake towers": "jlt",
    "jlt":                  "jlt",
    "difc":                 "difc",
    "deira":                "deira",
    "jumeirah":             "jumeirah",
    "al barsha":            "al_barsha",
    "albarsha":             "al_barsha",
    "dubai south":          "dubai_south",
    "sharjah":              "sharjah_central",
}


def _make_session(scrape_do_token: str | None = None) -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    if scrape_do_token:
        s.headers["X-ScrapeNinja-Token"] = scrape_do_token
    return s


def _area_listing_url(country: str, area_id: int, area_slug: str, page: int) -> str:
    return f"{_BASE}/{country}/restaurants/{area_id}/{area_slug}?page={page}"


def _parse_next_data(html: str) -> dict | None:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except (json.JSONDecodeError, ValueError):
        return None


def fetch_area_page(
    area_id: int,
    area_slug: str,
    page: int,
    *,
    country: str = "uae",
    session: requests.Session | None = None,
    timeout: float = 30.0,
    max_retries: int = 4,
) -> dict | None:
    """
    Fetch one page of vendors for a Talabat area.
    Returns the parsed __NEXT_DATA__ dict, or None on failure.
    Retries on connection errors with exponential backoff.
    """
    sess = session or _make_session()
    url = _area_listing_url(country, area_id, area_slug, page)
    for attempt in range(max_retries):
        try:
            resp = sess.get(url, timeout=timeout, allow_redirects=True)
            # 4xx are definitive failures — don't retry
            if 400 <= resp.status_code < 500:
                logger.warning("area_page_fetch_failed area=%s/%s page=%d status=%s", area_id, area_slug, page, resp.status_code)
                return None
            resp.raise_for_status()
            data = _parse_next_data(resp.text)
            if not data:
                logger.warning("area_page_no_next_data area=%s/%s page=%d status=%s", area_id, area_slug, page, resp.status_code)
            return data
        except requests.RequestException as exc:
            wait = 2 ** attempt * 3  # 3s, 6s, 12s, 24s
            if attempt < max_retries - 1:
                logger.warning(
                    "area_page_fetch_retry area=%s/%s page=%d attempt=%d wait=%ds: %s",
                    area_id, area_slug, page, attempt + 1, wait, exc,
                )
                time.sleep(wait)
                # Recreate session to reset connection pool after network errors
                sess = _make_session()
            else:
                logger.warning("area_page_fetch_failed area=%s/%s page=%d: %s", area_id, area_slug, page, exc)
    return None


def _extract_vendors_from_next_data(next_data: dict) -> tuple[list[dict], int]:
    """
    Return (vendors_on_page, total_vendor_count) from parsed __NEXT_DATA__.
    """
    try:
        page_data: dict = next_data["props"]["pageProps"]["data"]
        vendors: list[dict] = page_data.get("vendors") or []
        total: int = int(page_data.get("totalVendors") or 0)
        return vendors, total
    except (KeyError, TypeError, ValueError):
        return [], 0


def _extract_area_meta_from_next_data(next_data: dict) -> dict | None:
    """Return area object (id, lat, lng, name, slug) from page 1 __NEXT_DATA__."""
    try:
        return next_data["props"]["pageProps"]["data"].get("area")
    except (KeyError, TypeError):
        return None


def scrape_area_vendors(
    area_id: int,
    area_slug: str,
    *,
    country: str = "uae",
    page_delay: float = 0.4,
    max_pages: int | None = None,
    scrape_do_token: str | None = None,
    timeout: float = 30.0,
) -> tuple[list[dict], dict]:
    """
    Fetch all vendors for a Talabat area by iterating ?page=N.

    Returns (vendors, meta) where:
    - vendors: flat list of raw vendor dicts (all pages combined)
    - meta: {area_id, area_slug, total_vendors, pages_fetched, area_info}
    """
    session = _make_session(scrape_do_token)
    all_vendors: list[dict] = []
    area_info: dict | None = None
    total_vendors = 0

    # Fetch page 1 first to get totalVendors and area info
    first = fetch_area_page(area_id, area_slug, 1, country=country, session=session, timeout=timeout)
    if not first:
        logger.error("Failed to fetch page 1 for area %s/%s", area_id, area_slug)
        return [], {"area_id": area_id, "area_slug": area_slug, "error": "page_1_failed"}

    vendors, total_vendors = _extract_vendors_from_next_data(first)
    area_info = _extract_area_meta_from_next_data(first)
    all_vendors.extend(vendors)

    if total_vendors == 0:
        logger.warning("area=%s/%s reported 0 total vendors", area_id, area_slug)
        return all_vendors, {
            "area_id": area_id,
            "area_slug": area_slug,
            "total_vendors": 0,
            "pages_fetched": 1,
            "area_info": area_info,
        }

    total_pages = math.ceil(total_vendors / _PAGE_SIZE)
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    logger.info(
        "area_scrape_start area=%s/%s total_vendors=%d total_pages=%d",
        area_id, area_slug, total_vendors, total_pages,
    )

    for page in range(2, total_pages + 1):
        if page_delay > 0:
            time.sleep(page_delay)
        nd = fetch_area_page(area_id, area_slug, page, country=country, session=session, timeout=timeout)
        if nd is None:
            logger.warning("area=%s/%s page=%d fetch failed — stopping early", area_id, area_slug, page)
            break
        page_vendors, _ = _extract_vendors_from_next_data(nd)
        if not page_vendors:
            logger.info("area=%s/%s page=%d returned 0 vendors — stopping", area_id, area_slug, page)
            break
        all_vendors.extend(page_vendors)
        if page % 20 == 0 or page == total_pages:
            logger.info(
                "area_scrape_progress area=%s/%s page=%d/%d vendors_so_far=%d",
                area_id, area_slug, page, total_pages, len(all_vendors),
            )

    meta = {
        "area_id": area_id,
        "area_slug": area_slug,
        "total_vendors_reported": total_vendors,
        "vendors_collected": len(all_vendors),
        "pages_fetched": min(total_pages, len(all_vendors) // _PAGE_SIZE + 1),
        "area_info": area_info,
    }
    logger.info(
        "area_scrape_done area=%s/%s collected=%d reported=%d",
        area_id, area_slug, len(all_vendors), total_vendors,
    )
    return all_vendors, meta


def find_nearest_registry_area(lat: float, lng: float) -> tuple[str, int, str, float] | None:
    """
    Find the nearest area in UAE_AREA_REGISTRY to (lat, lng).
    Returns (key, area_id, area_slug, distance_km) or None if registry is empty.
    """
    best_key: str | None = None
    best_id: int = 0
    best_slug: str = ""
    best_dist = float("inf")

    for key, (area_id, slug, clat, clng) in UAE_AREA_REGISTRY.items():
        d = haversine_km(lat, lng, clat, clng)
        if d < best_dist:
            best_dist = d
            best_key = key
            best_id = area_id
            best_slug = slug

    if best_key is None:
        return None
    return best_key, best_id, best_slug, best_dist


def discover_area_id_for_name(
    area_name: str,
    *,
    country: str = "uae",
    id_lo: int = 1000,
    id_hi: int = 2000,
    timeout: float = 20.0,
) -> tuple[int, str] | None:
    """
    Probe Talabat to find the area_id for a given area name (e.g. "dubai-marina").
    Uses a linear scan of IDs within [id_lo, id_hi] — slow but reliable.

    Only needed for areas not yet in UAE_AREA_REGISTRY.
    Returns (area_id, area_slug) or None.
    """
    slug_target = area_name.strip().lower().replace(" ", "-")
    session = _make_session()
    for area_id in range(id_lo, id_hi + 1):
        url = f"{_BASE}/{country}/restaurants/{area_id}/{slug_target}?page=1"
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=False)
            if resp.status_code in (200, 301, 302):
                nd = _parse_next_data(resp.text) if resp.status_code == 200 else None
                if nd:
                    area_meta = _extract_area_meta_from_next_data(nd)
                    if area_meta:
                        found_id = int(area_meta.get("id") or area_id)
                        found_slug = str(area_meta.get("slug") or slug_target)
                        logger.info("discovered area_id=%d slug=%s for query %r", found_id, found_slug, area_name)
                        return found_id, found_slug
        except requests.RequestException:
            continue
        time.sleep(0.1)
    return None


def scrape_vendors_near_pin(
    pin_lat: float,
    pin_lng: float,
    radius_km: float,
    *,
    area_id: int | None = None,
    area_slug: str | None = None,
    country: str = "uae",
    page_delay: float = 0.4,
    max_pages: int | None = None,
    scrape_do_token: str | None = None,
) -> tuple[list[dict], dict]:
    """
    Fetch all vendors within radius_km of (pin_lat, pin_lng).

    Strategy:
    1. Resolve area_id + area_slug from the registry nearest to the pin (unless provided).
    2. Scrape ALL pages for that area.
    3. Filter vendors by haversine distance <= radius_km using each vendor's lat/lng.
    4. Deduplicate by branchId.

    Returns (filtered_vendors, meta).
    """
    if area_id is None or area_slug is None:
        result = find_nearest_registry_area(pin_lat, pin_lng)
        if result is None:
            raise ValueError("No areas in UAE_AREA_REGISTRY — pass area_id and area_slug explicitly.")
        area_key, area_id, area_slug, dist_km = result
        logger.info(
            "resolved area key=%s id=%d slug=%s dist_to_pin=%.2fkm",
            area_key, area_id, area_slug, dist_km,
        )

    all_vendors, meta = scrape_area_vendors(
        area_id,
        area_slug,
        country=country,
        page_delay=page_delay,
        max_pages=max_pages,
        scrape_do_token=scrape_do_token,
    )

    # Filter by distance + deduplicate
    seen_branch_ids: set = set()
    filtered: list[dict] = []
    for v in all_vendors:
        try:
            vlat = float(v.get("latitude") or 0)
            vlng = float(v.get("longitude") or 0)
        except (TypeError, ValueError):
            vlat = vlng = 0.0

        if vlat == 0.0 and vlng == 0.0:
            continue

        dist = haversine_km(pin_lat, pin_lng, vlat, vlng)
        if dist > radius_km:
            continue

        branch_id = v.get("branchId") or v.get("id")
        if branch_id in seen_branch_ids:
            continue
        seen_branch_ids.add(branch_id)

        v["_distance_km"] = round(dist, 4)
        filtered.append(v)

    filtered.sort(key=lambda x: x.get("_distance_km", 99999))

    meta["pin_lat"] = pin_lat
    meta["pin_lng"] = pin_lng
    meta["radius_km"] = radius_km
    meta["vendors_in_radius"] = len(filtered)
    meta["vendors_outside_radius"] = len(all_vendors) - len(seen_branch_ids) - (len(all_vendors) - len(filtered))

    logger.info(
        "pin_filter pin=(%.5f,%.5f) radius=%.1fkm total_scraped=%d in_radius=%d",
        pin_lat, pin_lng, radius_km, len(all_vendors), len(filtered),
    )
    return filtered, meta


def vendor_to_row(v: dict, *, pin_lat: float = 0.0, pin_lng: float = 0.0) -> dict:
    """
    Flatten a raw Talabat vendor dict to a clean row suitable for DataFrame / CSV export.
    """
    cuisines_raw = v.get("cuisines") or []
    cuisine_names = [c.get("name") or c.get("slug") or "" for c in cuisines_raw if isinstance(c, dict)]

    row: dict[str, Any] = {
        "branch_id":            v.get("branchId"),
        "restaurant_id":        v.get("restaurantId"),
        "name":                 v.get("name"),
        "branch_name":          v.get("branchName"),
        "area":                 v.get("areaName") or v.get("shopArea"),
        "city":                 v.get("shopCity"),
        "latitude":             v.get("latitude"),
        "longitude":            v.get("longitude"),
        "distance_km":          v.get("_distance_km"),
        "status":               v.get("status"),
        "status_code":          v.get("statusCode"),
        "is_grocery":           v.get("isGrocery"),
        "is_darkstore":         v.get("isDarkstore"),
        "vertical_type":        v.get("verticalType"),
        "cuisine_string":       v.get("cuisineString"),
        "cuisines":             ", ".join(c for c in cuisine_names if c),
        "rating":               v.get("rate"),
        "total_ratings":        v.get("totalRatings"),
        "total_reviews":        v.get("totalReviews"),
        "delivery_fee":         v.get("deliveryFee"),
        "min_order":            v.get("minimumOrderAmount"),
        "avg_delivery_min":     v.get("avgDeliveryTime"),
        "delivery_time_min":    (v.get("deliveryTime") or {}).get("min") if isinstance(v.get("deliveryTime"), dict) else None,
        "delivery_time_max":    (v.get("deliveryTime") or {}).get("max") if isinstance(v.get("deliveryTime"), dict) else None,
        "is_sponsored":         v.get("Sponsored") or v.get("isShopSponcered"),
        "is_new":               v.get("isNew"),
        "is_talabat_go":        v.get("isTalabatGO"),
        "created_at":           v.get("createdAt"),
        "branch_slug":          v.get("branchSlug"),
        "restaurant_slug":      v.get("restaurantSlug"),
        "branch_url":           v.get("branchUrl"),
        "area_id":              v.get("areaId"),
        "delivery_area_id":     v.get("deliveryAreaId"),
        "summary":              v.get("summary"),
        "promotion_text":       v.get("promotionText"),
        "discount_text":        v.get("discountText"),
        "shop_type":            v.get("shopType"),
        "shop_position":        v.get("shopPosition"),
    }
    return row
