"""Tests for supply / Kitchen Park CSV normalization."""

from __future__ import annotations

import pandas as pd

from supply_overlay import normalize_supply_overlay_df


def test_normalize_lat_lng_and_label() -> None:
    df = pd.DataFrame(
        {
            "Latitude": [25.2, 25.3],
            "longitude": [55.3, 55.4],
            "name": ["A", "B"],
        }
    )
    out = normalize_supply_overlay_df(df)
    assert out is not None
    assert list(out.columns) == ["lat", "lng", "label"]
    assert len(out) == 2
    assert out["label"].tolist() == ["A", "B"]


def test_normalize_synonym_columns() -> None:
    df = pd.DataFrame({"lat": [25.0], "lon": [55.0]})
    out = normalize_supply_overlay_df(df)
    assert out is not None
    assert len(out) == 1


def test_invalid_or_empty_returns_none() -> None:
    assert normalize_supply_overlay_df(None) is None
    assert normalize_supply_overlay_df(pd.DataFrame()) is None
    assert normalize_supply_overlay_df(pd.DataFrame({"foo": [1]})) is None
