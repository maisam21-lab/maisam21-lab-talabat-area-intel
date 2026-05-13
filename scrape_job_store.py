"""Persist completed/failed scrape jobs so ``GET /result/{id}`` survives API container restarts."""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger("talabat_area_intel.job_store")

_JOB_ID_FILE_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)


def job_store_dir() -> Path:
    raw = (os.getenv("SCRAPER_JOB_STORE_DIR") or "/app/data/scrape_jobs").strip()
    p = Path(raw)
    p.mkdir(parents=True, exist_ok=True)
    return p


def job_id_file_stem(request_id: str) -> str | None:
    rid = (request_id or "").strip().lower()
    if not _JOB_ID_FILE_RE.match(rid):
        return None
    return rid


def persist_job_record(request_id: str, job: dict) -> None:
    if str(job.get("status") or "") not in ("complete", "failed"):
        return
    stem = job_id_file_stem(request_id)
    if not stem:
        return
    path = job_store_dir() / f"{stem}.json"
    try:
        path.write_text(json.dumps(job, ensure_ascii=False, default=str), encoding="utf-8")
    except OSError as exc:
        logger.warning("persist_job_record_failed request_id=%s err=%s", request_id, exc)


def load_job_record(request_id: str) -> dict | None:
    stem = job_id_file_stem(request_id)
    if not stem:
        return None
    path = job_store_dir() / f"{stem}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("load_job_record_failed request_id=%s err=%s", request_id, exc)
        return None
