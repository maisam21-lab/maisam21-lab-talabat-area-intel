from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from hashlib import sha1


def talabat_listing_slug_from_url(url: str) -> str:
    """Last path segment of the vendor URL (Talabat's listing identity for that row)."""
    u = (url or "").strip().split("?")[0].rstrip("/")
    if not u:
        return ""
    return u.split("/")[-1].lower()[:400]


def brand_display_name_from_listing(restaurant_name: str, branch_name: str) -> str:
    """Human-facing brand label: strip branch suffix when the listing uses 'Brand - Branch'."""
    n = (restaurant_name or "").strip()
    if not n:
        return ""
    if " - " in n:
        return n.split(" - ", 1)[0].strip()
    b = (branch_name or "").strip()
    if b and len(n) > len(b) + 1 and n.lower().endswith(b.lower()):
        return n[: -len(b)].rstrip(" -–").strip() or n
    return n


def make_brand_id(brand_display_name: str) -> str:
    """Stable 14-char id per normalized display brand (not Talabat's internal parent company id)."""
    s = re.sub(r"\s+", " ", (brand_display_name or "").strip().lower())
    if not s:
        return ""
    return sha1(s.encode("utf-8")).hexdigest()[:14].upper()


@dataclass
class RestaurantRecord:
    scrape_ts_utc: str
    source_pin_lat: float
    source_pin_lng: float
    radius_km: float
    source_sample_lat: float
    source_sample_lng: float
    branch_sku: str
    # Stable id for brand rollups (derived from listing name; see brand_display_name)
    brand_id: str
    brand_display_name: str
    talabat_listing_slug: str
    restaurant_name: str
    legal_name: str
    branch_name: str
    restaurant_url: str
    talabat_restaurant_id: str
    talabat_branch_id: str
    contact_phone: str
    cuisines: str
    rating: str
    reviews_count: str
    eta: str
    delivery_fee: str
    min_order: str
    area_label: str
    status: str
    just_landed: str
    just_landed_date: str
    # Extra slicers (vendor page / __NEXT_DATA__ when available)
    google_rating: str
    google_reviews_count: str
    rating_source: str
    highly_rated_google: str
    is_pro_vendor: str
    free_delivery: str
    delivered_by_talabat: str
    preorder_available: str
    payment_methods: str
    currency: str
    recently_added_90d: str
    has_offers: str
    # From __NEXT_DATA__ when Talabat exposes counts; otherwise empty
    estimated_orders: str
    # Google Places (Text Search + Details) when GOOGLE_PLACES_ENRICH is enabled
    google_place_id: str
    google_maps_name: str
    # Vendor page HTML / JSON-LD / __NEXT_DATA__ extras
    vendor_website: str
    vendor_email: str
    vendor_social: str
    vendor_description: str
    tax_or_license_hint: str
    opening_hours_snippet: str
    # Google Places Details (when GOOGLE_PLACES_ENRICH runs)
    google_formatted_address: str
    google_business_website: str
    google_maps_link: str
    google_primary_type: str
    # OpenStreetMap reverse-geocode when ENRICH_NOMINATIM_REVERSE=1
    reverse_geocode_address: str
    # UAE city preset used for this run (e.g. Dubai); empty if custom pin-only scrape
    scrape_city: str
    # Optional client label for targeted-area runs (neighbourhood / polygon name, etc.)
    scrape_target_label: str
    lat: float
    lng: float

    def to_dict(self) -> dict:
        return asdict(self)


def make_branch_sku(name: str, branch_name: str, url: str, lat: float, lng: float) -> str:
    payload = "|".join(
        [
            (name or "").strip().lower(),
            (branch_name or "").strip().lower(),
            (url or "").strip().lower(),
            f"{lat:.5f}",
            f"{lng:.5f}",
        ]
    )
    return sha1(payload.encode("utf-8")).hexdigest()[:16].upper()
