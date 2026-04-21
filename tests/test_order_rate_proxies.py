from __future__ import annotations

import pandas as pd

from scrape_engine import add_rating_and_order_rate_proxies


def test_add_rating_and_order_rate_proxies() -> None:
    df = pd.DataFrame(
        [
            {
                "rating": "4.2",
                "google_rating": "",
                "estimated_orders": "365",
                "recently_added_90d": "",
            },
            {
                "rating": "",
                "google_rating": "4.6",
                "estimated_orders": "90",
                "recently_added_90d": "yes",
            },
        ]
    )
    out = add_rating_and_order_rate_proxies(df)
    assert "rating_effective" in out.columns
    assert "estimated_orders_per_day" in out.columns
    assert "estimated_orders_per_week" in out.columns

    assert float(out.loc[0, "rating_effective"]) == 4.2
    assert float(out.loc[0, "estimated_orders_per_day"]) == 1.0
    assert float(out.loc[0, "estimated_orders_per_week"]) == 7.0

    assert float(out.loc[1, "rating_effective"]) == 4.6
    assert float(out.loc[1, "estimated_orders_per_day"]) == 1.0
    assert float(out.loc[1, "estimated_orders_per_week"]) == 7.0

