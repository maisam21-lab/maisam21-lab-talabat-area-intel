"""Tests for resilient HTTP helpers in batch_scrape_client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from batch_scrape_client import format_connection_error_hint, http_get_with_connection_retries


def test_format_connection_error_hint_adds_docker_note() -> None:
    exc = requests.ConnectionError("HTTPConnectionPool(host='api', port=8000): Max retries exceeded")
    out = format_connection_error_hint(exc, "http://api:8000")
    assert "docker compose" in out.lower()
    assert "api:8000" in out


def test_format_connection_error_hint_pass_through_other() -> None:
    assert format_connection_error_hint(ValueError("bad payload"), "") == "bad payload"


def test_http_get_with_connection_retries_succeeds_after_failures() -> None:
    ok = MagicMock()
    ok.status_code = 200
    side = [
        requests.ConnectionError("refused"),
        requests.ConnectionError("refused"),
        ok,
    ]
    with patch("batch_scrape_client.requests.get", side_effect=side) as mock_get:
        with patch("batch_scrape_client.time.sleep"):
            r = http_get_with_connection_retries(
                "http://example/result/x",
                headers={"h": "1"},
                timeout=10,
            )
    assert r is ok
    assert mock_get.call_count == 3


def test_http_get_with_connection_retries_raises_after_max() -> None:
    with patch("batch_scrape_client.requests.get", side_effect=requests.ConnectionError("refused")):
        with patch("batch_scrape_client.time.sleep"):
            with patch.dict("os.environ", {"SCRAPER_RESULT_POLL_CONNECT_RETRIES": "3"}, clear=False):
                with pytest.raises(requests.ConnectionError):
                    http_get_with_connection_retries("http://x", headers={})
