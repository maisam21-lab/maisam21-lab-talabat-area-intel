"""
Salesforce tenant lookup — pulls active CK accounts in UAE/EMEA.
Uses OAuth 2.0 Client Credentials flow (no username/password needed).
Results cached for 24 hours to avoid hammering SF on every run.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import requests

_CACHE_PATH = Path(os.getenv("SF_TENANTS_CACHE_PATH", "/app/analyze_jobs/sf_tenants_cache.json"))
_CACHE_TTL_SEC = 86400  # 24 hours


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
    records = []
    params = {"q": soql}
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


def _normalise(name: str) -> str:
    """Lowercase + strip punctuation for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def fetch_sf_tenants(*, force_refresh: bool = False) -> set[str]:
    """
    Return a set of normalised account names that are active CK tenants in UAE/EMEA.
    Cached to disk for 24h.
    """
    client_id     = os.getenv("SF_CLIENT_ID", "").strip()
    client_secret = os.getenv("SF_CLIENT_SECRET", "").strip()
    instance_url  = os.getenv("SF_INSTANCE_URL", "").strip()

    if not all([client_id, client_secret, instance_url]):
        return set()

    # Check cache
    if not force_refresh and _CACHE_PATH.exists():
        try:
            cached = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
            if time.time() - cached.get("ts", 0) < _CACHE_TTL_SEC:
                return set(cached["names"])
        except Exception:
            pass

    try:
        token = _get_access_token(client_id, client_secret, instance_url)

        soql = """
            SELECT Account.Name
            FROM Opportunity
            WHERE StageName IN ('Closed Won', 'Active')
              AND Facility_Country__c IN ('UAE', 'AE', 'QA', 'BH', 'KW', 'SA')
              AND Kitchen_Type__c NOT IN ('CloudRetail', 'Virtual')
              AND EMEA_Transfer_Status__c != 'Member Transfer'
        """
        records = _soql_query(instance_url, token, soql.strip())
        names = {_normalise(r["Account"]["Name"]) for r in records if r.get("Account")}

        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(
            json.dumps({"ts": time.time(), "names": list(names)}, ensure_ascii=False),
            encoding="utf-8"
        )
        return names

    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("sf_tenants fetch failed: %s", exc)
        # Return stale cache if available
        if _CACHE_PATH.exists():
            try:
                cached = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
                return set(cached["names"])
            except Exception:
                pass
        return set()


def is_sf_tenant(brand_name: str, tenant_names: set[str]) -> bool:
    """Check if a brand is already a CK tenant."""
    return _normalise(brand_name) in tenant_names
