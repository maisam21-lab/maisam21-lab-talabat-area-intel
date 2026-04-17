from __future__ import annotations

import math

import numpy as np
import pandas as pd


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth_radius_km = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * earth_radius_km * math.asin(math.sqrt(a))


def haversine_series_km_from_pin(
    pin_lat: float,
    pin_lng: float,
    lat_col: pd.Series,
    lng_col: pd.Series,
) -> pd.Series:
    """Vectorized great-circle distance (km) from one pin to each row; NaN where coords invalid."""
    lat1 = np.radians(float(pin_lat))
    lng1 = np.radians(float(pin_lng))
    lat2 = np.radians(pd.to_numeric(lat_col, errors="coerce"))
    lng2 = np.radians(pd.to_numeric(lng_col, errors="coerce"))
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))
    return pd.Series(6371.0 * c, index=lat_col.index)


def km_to_lat_deg(km: float) -> float:
    return km / 110.574


def km_to_lng_deg(km: float, lat: float) -> float:
    return km / (111.320 * math.cos(math.radians(lat)) + 1e-12)


def generate_points_in_radius(center_lat: float, center_lng: float, radius_km: float, spacing_km: float) -> list[tuple[float, float]]:
    min_lat = center_lat - km_to_lat_deg(radius_km)
    max_lat = center_lat + km_to_lat_deg(radius_km)
    points: list[tuple[float, float]] = []

    lat = min_lat
    while lat <= max_lat:
        min_lng = center_lng - km_to_lng_deg(radius_km, lat)
        max_lng = center_lng + km_to_lng_deg(radius_km, lat)
        lng_step = km_to_lng_deg(spacing_km, lat)
        lng = min_lng
        while lng <= max_lng:
            if haversine_km(center_lat, center_lng, lat, lng) <= radius_km:
                points.append((round(lat, 6), round(lng, 6)))
            lng += lng_step
        lat += km_to_lat_deg(spacing_km)

    # Pin must be first: single-sample runs must use the user's location, not an arbitrary grid corner.
    center_pt = (round(center_lat, 6), round(center_lng, 6))
    ordered = [center_pt]
    for p in points:
        if p != center_pt:
            ordered.append(p)
    return list(dict.fromkeys(ordered))


def refine_grid_spacing(
    center_lat: float,
    center_lng: float,
    radius_km: float,
    spacing_km: float,
    *,
    target_count: int,
    spacing_floor: float = 0.35,
    max_iterations: int = 28,
) -> list[tuple[float, float]]:
    """Shrink spacing until the circle contains at least ``target_count`` sample points (or floor reached)."""
    sp = max(spacing_floor, float(spacing_km))
    floor = max(0.25, float(spacing_floor))
    target = max(1, int(target_count))
    best = generate_points_in_radius(center_lat, center_lng, radius_km, sp)
    for _ in range(max_iterations):
        pts = generate_points_in_radius(center_lat, center_lng, radius_km, sp)
        if len(pts) >= target:
            return pts
        best = pts
        new_sp = max(floor, sp * 0.86)
        if abs(new_sp - sp) < 1e-6:
            return best
        sp = new_sp
    return best
