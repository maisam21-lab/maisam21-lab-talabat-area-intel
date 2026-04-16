"""Talabat URL rules: listing pages use /uae/{vendor-slug}, not only /restaurant/..."""

from __future__ import annotations

import re
from typing import FrozenSet

# Path segments under /uae/ that are not a restaurant vendor page.
EXCLUDED_UAE_SLUGS: FrozenSet[str] = frozenset(
    {
        "restaurants",
        "groceries",
        "mart",
        "shops",
        "pharmacy",
        "flowers",
        "en",
        "ar",
        "faq",
        "terms",
        "privacy",
        "privacy-policy",
        "contact",
        "contact-us",
        "login",
        "register",
        "cart",
        "checkout",
        "offers",
        "cities",
        "blog",
        "careers",
        "corporate",
        "about",
        "sitemap",
        "order",
        "account",
        "wallet",
        "deals",
        "dineout",
    }
)

# Match vendor URLs in HTML or JSON blobs.
UAE_VENDOR_URL_RE = re.compile(
    r"https://(?:www\.)?talabat\.com/(?:en/)?uae/([a-z0-9][a-z0-9\-]*)",
    re.IGNORECASE,
)


def is_vendor_slug(slug: str) -> bool:
    s = slug.strip().lower()
    if len(s) < 3:
        return False
    return s not in EXCLUDED_UAE_SLUGS


def canonical_uae_vendor_url(slug: str) -> str:
    return f"https://www.talabat.com/uae/{slug.strip()}"
