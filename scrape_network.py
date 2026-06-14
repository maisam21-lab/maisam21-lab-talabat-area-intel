"""Outbound proxy resolution for ``requests`` and Playwright (same URL, operator-controlled).

Set **one** of (first wins):

- ``SCRAPER_HTTP_PROXY`` — recommended for scraper-only routing without changing global ``HTTP_PROXY``.
- ``ALL_PROXY`` / ``HTTPS_PROXY`` / ``HTTP_PROXY`` — standard process-wide variables.

If none of the above are set, you can enable **Scrape.do** proxy mode only::

    SCRAPE_DO_TOKEN=your_api_token
    # optional overrides:
    # SCRAPE_DO_PROXY_PASSWORD=customHeaders=false
    # SCRAPE_DO_PROXY_HOST=proxy.scrape.do
    # SCRAPE_DO_PROXY_PORT=8080

Example::

    SCRAPER_HTTP_PROXY=http://user:pass@proxy.example.com:8888

Playwright receives ``{"server", "username"?, "password"?}``; ``requests`` uses ``{"http": url, "https": url}``.

Respect Talabat terms of use: proxies are for reliability and approved infrastructure only.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote, unquote, urlparse


def _read_dot_env_key(key: str) -> str:
    """Read a single key from the .env file beside this module (supports runtime updates)."""
    try:
        env_path = Path(__file__).parent / ".env"
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


def _scrape_do_proxy_url_from_env() -> str:
    """Build Scrape.do rotating-proxy URL when ``SCRAPE_DO_TOKEN`` is set (proxy mode)."""
    # Read from .env file first so /admin/set-env changes take effect without restart
    token = _read_dot_env_key("SCRAPE_DO_TOKEN") or (os.getenv("SCRAPE_DO_TOKEN") or "").strip().strip('"').strip("'")
    if not token:
        return ""
    host = (os.getenv("SCRAPE_DO_PROXY_HOST") or "proxy.scrape.do").strip()
    port_s = (os.getenv("SCRAPE_DO_PROXY_PORT") or "8080").strip()
    try:
        port = int(port_s)
    except ValueError:
        port = 8080
    password = (os.getenv("SCRAPE_DO_PROXY_PASSWORD") or "customHeaders=false").strip()
    user_q = quote(token, safe="")
    pass_q = quote(password, safe="")
    return f"http://{user_q}:{pass_q}@{host}:{port}"


def outbound_proxy_source() -> str:
    """Which env key supplies the outbound proxy (same precedence as ``proxy_url_from_env``). Empty if none."""
    for key in (
        "SCRAPER_HTTP_PROXY",
        "SCRAPER_ALL_PROXY",
        "ALL_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
    ):
        v = (os.getenv(key) or "").strip().strip('"').strip("'")
        if v:
            return key
    token = (os.getenv("SCRAPE_DO_TOKEN") or "").strip().strip('"').strip("'")
    if token:
        return "SCRAPE_DO_TOKEN"
    return ""


def proxy_url_from_env() -> str:
    for key in (
        "SCRAPER_HTTP_PROXY",
        "SCRAPER_ALL_PROXY",
        "ALL_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
    ):
        v = (os.getenv(key) or "").strip().strip('"').strip("'")
        if v:
            return v
    return _scrape_do_proxy_url_from_env()


def requests_proxies_from_env() -> dict[str, str] | None:
    url = proxy_url_from_env()
    if not url:
        return None
    return {"http": url, "https": url}


def playwright_proxy_from_env() -> dict[str, str] | None:
    """Return Playwright ``proxy`` dict for ``browser.new_context`` / ``new_page`` routing."""
    url = proxy_url_from_env()
    if not url:
        return None
    parsed = urlparse(url)
    if not parsed.hostname:
        return None
    scheme = (parsed.scheme or "http").lower()
    if scheme not in ("http", "https", "socks5"):
        scheme = "http"
    host = parsed.hostname
    port = parsed.port
    if port:
        server = f"{scheme}://{host}:{port}"
    else:
        server = f"{scheme}://{host}"
    cfg: dict[str, str] = {"server": server}
    if parsed.username:
        cfg["username"] = unquote(parsed.username)
    if parsed.password is not None:
        cfg["password"] = unquote(parsed.password)
    return cfg
