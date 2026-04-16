from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha1


@dataclass
class RestaurantRecord:
    scrape_ts_utc: str
    source_pin_lat: float
    source_pin_lng: float
    radius_km: float
    source_sample_lat: float
    source_sample_lng: float
    branch_sku: str
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
