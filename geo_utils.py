from __future__ import annotations

import math


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth_radius_km = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * earth_radius_km * math.asin(math.sqrt(a))


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

    points.append((round(center_lat, 6), round(center_lng, 6)))
    return list(dict.fromkeys(points))
