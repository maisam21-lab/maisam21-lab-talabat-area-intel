"""
Salesforce tenant + kitchen data.
Uses OAuth 2.0 Client Credentials flow.
Queries Kitchen_Number__c directly — the most accurate source for who is
actually occupying each kitchen right now.
Results cached 24h.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_CACHE_PATH = Path(os.getenv("SF_TENANTS_CACHE_PATH", "/app/analyze_jobs/sf_tenants_cache.json"))
_CACHE_TTL_SEC = 86400  # 24 hours


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_access_token(client_id: str, client_secret: str, instance_url: str) -> str:
    url = f"{instance_url.rstrip('/')}/services/oauth2/token"
    r = requests.post(url, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def _soql_query(instance_url: str, token: str, soql: str) -> list[dict]:
    url = f"{instance_url.rstrip('/')}/services/data/v59.0/query"
    records, params = [], {"q": soql}
    while True:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        next_url = data.get("nextRecordsUrl")
        if not next_url:
            break
        url = f"{instance_url.rstrip('/')}{next_url}"
        params = {}
    return records


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise(name: str) -> str:
    """Lowercase + strip punctuation for fuzzy name matching."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _load_cache() -> dict | None:
    try:
        if _CACHE_PATH.exists():
            return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None


def _save_cache(data: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


# ── Main fetch ────────────────────────────────────────────────────────────────

def fetch_sf_data(*, force_refresh: bool = False) -> dict:
    """
    Fetch kitchen + tenant data from SF.
    Returns:
      {
        "tenant_names": set[str],          # normalised account names of active tenants
        "kitchens": [                       # for map display
          {
            "kitchen_name": str,            # e.g. "K-001"
            "status": str,                  # Occupied / Vacant / Sold / etc.
            "kitchen_type": str,
            "facility_name": str,
            "facility_country": str,
            "facility_lat": float | None,
            "facility_lng": float | None,
            "tenant_name": str | None,      # account name if occupied
          }
        ]
      }
    """
    client_id     = os.getenv("SF_CLIENT_ID", "").strip()
    client_secret = os.getenv("SF_CLIENT_SECRET", "").strip()
    instance_url  = os.getenv("SF_INSTANCE_URL", "").strip()

    empty = {"tenant_names": set(), "kitchens": []}
    if not all([client_id, client_secret, instance_url]):
        return empty

    # Check cache
    if not force_refresh:
        cached = _load_cache()
        if cached and time.time() - cached.get("ts", 0) < _CACHE_TTL_SEC:
            return {
                "tenant_names": set(cached.get("tenant_names", [])),
                "kitchens": cached.get("kitchens", []),
            }

    try:
        token = _get_access_token(client_id, client_secret, instance_url)

        # Query Kitchen_Number__c — ground truth for who's in each kitchen
        soql = """
            SELECT
                Name,
                Status__c,
                Kitchen_Type__c,
                Facility_Country__c,
                Facility_Kitchen_City__c,
                Facility__r.Name,
                Facility__r.BillingLatitude,
                Facility__r.BillingLongitude,
                Currently_Occupied_Opportunity__r.Account.Name
            FROM Kitchen_Number__c
            WHERE Facility_Country__c IN ('UAE', 'Saudi Arabia', 'Kuwait', 'Qatar', 'Bahrain')
        """
        records = _soql_query(instance_url, token, soql.strip())

        kitchens = []
        tenant_names: set[str] = set()

        for r in records:
            facility     = r.get("Facility__r") or {}
            opp          = r.get("Currently_Occupied_Opportunity__r") or {}
            account      = opp.get("Account") or {}
            tenant_name  = account.get("Name") or None

            lat = facility.get("BillingLatitude")
            lng = facility.get("BillingLongitude")

            kitchens.append({
                "kitchen_name":    r.get("Name", ""),
                "status":          r.get("Status__c", ""),
                "kitchen_type":    r.get("Kitchen_Type__c", ""),
                "facility_name":   facility.get("Name", ""),
                "facility_country": r.get("Facility_Country__c", ""),
                "facility_city":   r.get("Facility_Kitchen_City__c", ""),
                "facility_lat":    float(lat) if lat is not None else None,
                "facility_lng":    float(lng) if lng is not None else None,
                "tenant_name":     tenant_name,
            })

            if tenant_name:
                tenant_names.add(_normalise(tenant_name))

        _save_cache({
            "ts": time.time(),
            "tenant_names": list(tenant_names),
            "kitchens": kitchens,
        })

        return {"tenant_names": tenant_names, "kitchens": kitchens}

    except Exception as exc:
        logger.warning("sf_tenants fetch failed: %s", exc)
        cached = _load_cache()
        if cached:
            return {
                "tenant_names": set(cached.get("tenant_names", [])),
                "kitchens": cached.get("kitchens", []),
            }
        return empty


# ── Convenience wrappers ──────────────────────────────────────────────────────

def fetch_sf_tenants(*, force_refresh: bool = False) -> set[str]:
    """Return normalised tenant name set (for lead scoring)."""
    return fetch_sf_data(force_refresh=force_refresh)["tenant_names"]


def fetch_sf_kitchens(*, force_refresh: bool = False) -> list[dict]:
    """Return kitchen list (for map display)."""
    return fetch_sf_data(force_refresh=force_refresh)["kitchens"]


def is_sf_tenant(brand_name: str, tenant_names: set[str]) -> bool:
    return _normalise(brand_name) in tenant_names
