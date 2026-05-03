from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests
import time

_POST_TIMEOUT_CAP = float(os.getenv("SCRAPER_API_POST_TIMEOUT_SEC", "180"))


def _poll_result(
    *,
    api_base_url: str,
    headers: dict[str, str],
    request_id: str,
    timeout_sec: float,
    poll_every_sec: float = 10.0,
) -> dict[str, Any]:
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        r = requests.get(
            f"{api_base_url.rstrip('/')}/result/{request_id}",
            headers=headers,
            timeout=30,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Result poll failed: HTTP {r.status_code}")
        data = r.json() if r.content else {}
        status = str((data or {}).get("status") or "").lower()
        if status == "complete":
            return data
        if status == "failed":
            err = str((data or {}).get("error") or "Scrape job failed")
            raise RuntimeError(err)
        time.sleep(poll_every_sec)
    raise RuntimeError("Result poll timed out before completion")


def run_dual_area_scrape_via_api(
    *,
    api_base_url: str,
    headers: dict[str, str],
    locations_df: pd.DataFrame,
    base_payload: dict[str, Any],
    timeout_sec: float,
) -> tuple[pd.DataFrame, list[str]]:
    result_frames: list[pd.DataFrame] = []
    request_errors: list[str] = []

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
        req_headers["X-Request-ID"] = req_headers.get("X-Request-ID") or f"dual-{i+1}"
        try:
            r = requests.post(
                f"{api_base_url.rstrip('/')}/scrape",
                json=payload,
                headers=req_headers,
                timeout=min(float(timeout_sec), _POST_TIMEOUT_CAP),
            )
            if r.status_code >= 400:
                request_errors.append(f"Location {i+1}: HTTP {r.status_code}")
                continue
            enqueue = r.json() if r.content else {}
            # Backward compatibility: older API versions return final scrape payload directly from /scrape.
            if isinstance(enqueue, dict) and "records" in enqueue:
                data = enqueue
            else:
                rid = str((enqueue or {}).get("request_id") or req_headers.get("X-Request-ID") or "").strip()
                if not rid:
                    request_errors.append(f"Location {i+1}: Missing request_id from /scrape")
                    continue
                data = _poll_result(
                    api_base_url=api_base_url,
                    headers=req_headers,
                    request_id=rid,
                    timeout_sec=timeout_sec,
                    poll_every_sec=10.0,
                )
            location_df = pd.DataFrame((data or {}).get("records", []))
            if location_df.empty:
                continue
            location_df["batch_location_label"] = str(row.get("label") or f"loc_{i+1}")
            slot = row.get("area_slot")
            if slot is not None and not (isinstance(slot, float) and pd.isna(slot)):
                location_df["dual_area"] = str(slot).strip().upper()[:8] or str(slot)
            result_frames.append(location_df)
        except Exception as exc:
            request_errors.append(f"Location {i+1}: {exc}")

    if not result_frames:
        return pd.DataFrame(), request_errors

    out = pd.concat(result_frames, ignore_index=True)
    return out.reset_index(drop=True), request_errors


def run_batch_scrape_via_api(
    *,
    api_base_url: str,
    headers: dict[str, str],
    locations_df: pd.DataFrame,
    base_payload: dict[str, Any],
    timeout_sec: float,
) -> tuple[pd.DataFrame, list[str]]:
    """Backward-compatible wrapper for older batch naming."""
    return run_dual_area_scrape_via_api(
        api_base_url=api_base_url,
        headers=headers,
        locations_df=locations_df,
        base_payload=base_payload,
        timeout_sec=timeout_sec,
    )
