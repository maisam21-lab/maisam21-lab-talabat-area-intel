from __future__ import annotations

from typing import Any

import pandas as pd
import requests


def run_batch_scrape_via_api(
    *,
    api_base_url: str,
    headers: dict[str, str],
    locations_df: pd.DataFrame,
    base_payload: dict[str, Any],
    timeout_sec: float,
) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    for i, row in locations_df.reset_index(drop=True).iterrows():
        payload = dict(base_payload)
        payload["pin_lat"] = float(row["lat"])
        payload["pin_lng"] = float(row["lng"])
        payload["client_asserted_pin_lat"] = float(row["lat"])
        payload["client_asserted_pin_lng"] = float(row["lng"])
        if not payload.get("scrape_target_label"):
            lab = str(row.get("label") or "").strip()
            payload["scrape_target_label"] = lab or None

        req_headers = dict(headers)
        req_headers["X-Request-ID"] = req_headers.get("X-Request-ID") or f"batch-{i+1}"
        try:
            r = requests.post(
                f"{api_base_url.rstrip('/')}/scrape",
                json=payload,
                headers=req_headers,
                timeout=timeout_sec,
            )
            if r.status_code >= 400:
                errors.append(f"Location {i+1}: HTTP {r.status_code}")
                continue
            data = r.json() if r.content else {}
            dfi = pd.DataFrame((data or {}).get("records", []))
            if dfi.empty:
                continue
            dfi["batch_location_label"] = str(row.get("label") or f"loc_{i+1}")
            frames.append(dfi)
        except Exception as exc:
            errors.append(f"Location {i+1}: {exc}")

    if not frames:
        return pd.DataFrame(), errors

    out = pd.concat(frames, ignore_index=True)
    return out.reset_index(drop=True), errors
