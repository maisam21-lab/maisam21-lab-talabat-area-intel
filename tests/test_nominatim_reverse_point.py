"""Unit tests for single-point Nominatim reverse geocode helper."""

from __future__ import annotations

from unittest.mock import MagicMock

from nominatim_enrich import reverse_geocode_display_name


def test_reverse_geocode_display_name_success() -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"display_name": "Sheikh Zayed Road, Dubai, UAE"}
    mock_resp.raise_for_status = MagicMock()
    sess = MagicMock()
    sess.get.return_value = mock_resp
    out = reverse_geocode_display_name(25.2, 55.27, session=sess)
    assert out == "Sheikh Zayed Road, Dubai, UAE"
    sess.get.assert_called_once()
    assert str(sess.get.call_args[0][0]).endswith("/reverse")


def test_reverse_geocode_display_name_http_error() -> None:
    sess = MagicMock()
    sess.get.side_effect = OSError("network")
    assert reverse_geocode_display_name(1.0, 2.0, session=sess) is None


def test_reverse_geocode_display_name_empty_display() -> None:
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"display_name": ""}
    mock_resp.raise_for_status = MagicMock()
    sess = MagicMock()
    sess.get.return_value = mock_resp
    assert reverse_geocode_display_name(1.0, 2.0, session=sess) is None
