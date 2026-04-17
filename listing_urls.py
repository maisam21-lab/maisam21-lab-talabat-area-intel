"""Talabat listing entry URLs — general + cuisine pages surface different vendor sets (for high-volume scrapes)."""

from __future__ import annotations

import os

# Kebab-case segments under /en/uae/cuisine/{slug} (also valid without /en/).
_CUISINE_SLUGS: tuple[str, ...] = (
    "arabic",
    "middle-eastern",
    "american",
    "burgers",
    "pizza",
    "italian",
    "indian",
    "chinese",
    "japanese",
    "mexican",
    "thai",
    "seafood",
    "breakfast",
    "cafe",
    "desserts",
    "healthy",
    "sandwiches",
    "korean",
    "turkish",
    "lebanese",
    "grill",
    "fried-chicken",
    "bakery",
    "pakistani",
    "filipino",
    "persian",
    "vegetarian",
    "coffee",
    "pasta",
    "egyptian",
    "emirati",
)


def build_listing_url_list(*, include_cuisine_sweep: bool) -> list[str]:
    """Ordered roster: main restaurants listing first, then cuisine hubs (deduped)."""
    urls: list[str] = [
        "https://www.talabat.com/en/uae/restaurants",
        "https://www.talabat.com/uae/restaurants",
    ]
    if include_cuisine_sweep:
        for slug in _CUISINE_SLUGS:
            urls.append(f"https://www.talabat.com/en/uae/cuisine/{slug}")
    return list(dict.fromkeys(urls))


def capped_listing_urls(include_cuisine_sweep: bool) -> list[str]:
    """Limit pages per grid point so one scrape can finish within wall-clock budgets."""
    raw = build_listing_url_list(include_cuisine_sweep=include_cuisine_sweep)
    if not include_cuisine_sweep:
        return raw[:2]
    cap = int(os.getenv("SCRAPER_MAX_LISTING_PAGES_PER_POINT", "14"))
    cap = max(2, min(cap, 80))
    return raw[:cap]
