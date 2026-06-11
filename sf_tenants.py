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


# ── Verified UAE KP facility coordinates (lat, lng) ──────────────────────────
# Salesforce BillingLatitude/BillingLongitude is often missing on Facility records.
# This lookup provides authoritative coordinates keyed by normalised SF facility name.
_UAE_KP_COORDS: dict[str, tuple[float, float]] = {
    # DXB
    "uaedxbjlt1":            (25.0802475, 55.1512831),
    "uaedxbjlt2":            (25.0663430, 55.1376235),
    "uaedxbbusinessbay1":    (25.1894021, 55.2892571),
    "uaedxbbusinessbay2":    (25.1894021, 55.2892571),
    "uaedxbmotorcity1":      (25.0469210, 55.2303106),
    "uaedxbmotorcity2":      (25.0469210, 55.2303106),
    "uaedxbarjan1":          (25.0645000, 55.2393000),
    "uaedxbarjan3ek":        (25.0656802, 55.2354685),
    "uaedxbdso":             (25.1282034, 55.3922505),
    "uaedxbburdubai":        (25.2460615, 55.2759454),
    "uaedxbimpz1":           (25.0383700, 55.1861500),
    "uaedxbmirdif":          (25.2347469, 55.4310875),
    "uaedxbuptownmirdif":    (25.2347469, 55.4310875),
    "uaedxbsufouh":          (25.1103251, 55.1780541),
    "uaedxbdeira":           (25.2698737, 55.3323663),
    "uaedxbwafi":            (25.2297643, 55.3189516),
    "uaedxbquoz1":           (25.1403704, 55.2446225),
    "uaedxbhessa2ek":        (25.0831785, 55.2018668),
    "uaedxbdic":             (25.0930000, 55.1528000),
    "uaedxbjabalali":        (24.9903930, 55.1427240),
    # Abu Dhabi
    "uaeadcityoflight":      (24.4389739, 54.5742641),
    "uaeadcol":              (24.4989329, 54.4031167),
    "uaeadraha1ek":          (24.4389739, 54.5742641),
    "uaeadnahyan":           (24.3917000, 54.5117000),
    "uaeadjimi":             (24.2281000, 55.7614000),
    "uaeadfalah":            (24.4167000, 54.3833000),
    "uaeadshamkha":          (24.2667000, 54.2583000),
    # Sharjah
    "uaeshjsharjahcentre":   (25.3376961, 55.4008590),
    "uaeshjmuwailehek":      (25.3045405, 55.4698694),
}

def _lookup_coords(facility_name: str) -> tuple[float, float] | None:
    """Return verified (lat, lng) for a UAE KP facility, or None if unknown."""
    key = _normalise(facility_name)
    # Direct match
    if key in _UAE_KP_COORDS:
        return _UAE_KP_COORDS[key]
    # Partial match — find the longest matching key
    best: tuple[float, float] | None = None
    best_len = 0
    for k, v in _UAE_KP_COORDS.items():
        if k in key and len(k) > best_len:
            best, best_len = v, len(k)
    return best


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

def _read_env_file() -> dict[str, str]:
    """Read key=value pairs from .env file — used when env vars aren't set in process."""
    env_path = Path(__file__).parent / ".env"
    result: dict[str, str] = {}
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip()
    except Exception:
        pass
    return result


def _get_sf_creds() -> tuple[str, str, str]:
    """Get SF credentials from env vars, falling back to .env file."""
    client_id     = os.getenv("SF_CLIENT_ID", "").strip()
    client_secret = os.getenv("SF_CLIENT_SECRET", "").strip()
    instance_url  = os.getenv("SF_INSTANCE_URL", "").strip()
    if not all([client_id, client_secret, instance_url]):
        env = _read_env_file()
        client_id     = client_id     or env.get("SF_CLIENT_ID", "")
        client_secret = client_secret or env.get("SF_CLIENT_SECRET", "")
        instance_url  = instance_url  or env.get("SF_INSTANCE_URL", "")
    return client_id, client_secret, instance_url


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
    client_id, client_secret, instance_url = _get_sf_creds()

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
            fac_name = facility.get("Name", "")
            country  = r.get("Facility_Country__c", "")

            # SF BillingLatitude is often missing for UAE KP facilities.
            # Fall back to verified hardcoded lookup.
            if country == "UAE" and (lat is None or lng is None):
                coords = _lookup_coords(fac_name)
                if coords:
                    lat, lng = coords

            kitchens.append({
                "kitchen_name":    r.get("Name", ""),
                "status":          r.get("Status__c", ""),
                "kitchen_type":    r.get("Kitchen_Type__c", ""),
                "facility_name":   fac_name,
                "facility_country": country,
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
