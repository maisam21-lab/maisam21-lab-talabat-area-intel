"""Tests for scrape_network proxy URL helpers."""

from __future__ import annotations

from unittest.mock import patch

from scrape_network import playwright_proxy_from_env, proxy_url_from_env, requests_proxies_from_env


def test_proxy_url_scraper_wins_over_http_proxy() -> None:
    with patch.dict(
        "os.environ",
        {"HTTP_PROXY": "http://old:1", "SCRAPER_HTTP_PROXY": "http://new:2"},
        clear=False,
    ):
        assert proxy_url_from_env() == "http://new:2"


def test_proxy_url_scrape_do_when_no_explicit_proxy() -> None:
    env = {
        "SCRAPER_HTTP_PROXY": "",
        "SCRAPER_ALL_PROXY": "",
        "ALL_PROXY": "",
        "HTTPS_PROXY": "",
        "HTTP_PROXY": "",
        "SCRAPE_DO_TOKEN": "mytoken",
        "SCRAPE_DO_PROXY_PASSWORD": "customHeaders=false",
    }
    with patch.dict("os.environ", env, clear=True):
        url = proxy_url_from_env()
    assert "proxy.scrape.do:8080" in url
    assert "mytoken" in url
    assert "customHeaders%3Dfalse" in url or "customHeaders=" in url


def test_proxy_url_explicit_wins_over_scrape_do_token() -> None:
    with patch.dict(
        "os.environ",
        {
            "SCRAPER_HTTP_PROXY": "http://explicit:9@host:1",
            "SCRAPE_DO_TOKEN": "ignored",
        },
        clear=False,
    ):
        assert proxy_url_from_env() == "http://explicit:9@host:1"


def test_requests_proxies_none_when_unset() -> None:
    with patch("scrape_network.proxy_url_from_env", return_value=""):
        assert requests_proxies_from_env() is None


def test_playwright_proxy_user_pass() -> None:
    with patch(
        "scrape_network.proxy_url_from_env",
        return_value="http://user:p%40ss@proxy.example.com:8888",
    ):
        px = playwright_proxy_from_env()
    assert px is not None
    assert px["server"] == "http://proxy.example.com:8888"
    assert px["username"] == "user"
    assert px["password"] == "p@ss"
