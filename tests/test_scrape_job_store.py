"""Tests for scrape_job_store disk persistence."""

from __future__ import annotations

from unittest.mock import patch

from scrape_job_store import job_id_file_stem, load_job_record, persist_job_record


def test_job_id_file_stem_accepts_uuid_hex() -> None:
    rid = "7bec2659cd44428da9620c3c61ca102d"
    assert job_id_file_stem(rid) == rid.lower()


def test_job_id_file_stem_rejects_path_traversal() -> None:
    assert job_id_file_stem("../../../etc/passwd") is None
    assert job_id_file_stem("short") is None


def test_persist_load_roundtrip(tmp_path) -> None:
    rid = "7bec2659cd44428da9620c3c61ca102d"
    job = {"status": "complete", "request_id": rid, "result": {"ok": True, "count": 2}, "submitted_at": 1.0}
    with patch.dict("os.environ", {"SCRAPER_JOB_STORE_DIR": str(tmp_path)}, clear=False):
        persist_job_record(rid, job)
        assert (tmp_path / f"{rid}.json").is_file()
        loaded = load_job_record(rid)
    assert loaded == job


def test_persist_skips_non_terminal_status(tmp_path) -> None:
    rid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    with patch.dict("os.environ", {"SCRAPER_JOB_STORE_DIR": str(tmp_path)}, clear=False):
        persist_job_record(rid, {"status": "running", "request_id": rid})
    assert not (tmp_path / f"{rid}.json").is_file()
