# Put HTTPS + a hostname in front of the scraper API (Caddy on Hetzner)

This fixes flaky **Streamlit Cloud → `http://` raw IP** enqueue timeouts by giving the API a normal **`https://api…`** URL.

## 0. Prerequisites

- VPS public IP (e.g. `178.105.56.187`).
- A **domain** you control (e.g. `yourdomain.com`).
- DNS access to create a record.

## 1. DNS

Create an **A record**:

| Type | Name | Value            | TTL |
|------|------|------------------|-----|
| A    | `api` | your VPS IPv4   | 300–600s |

Result: **`api.yourdomain.com`** → your server.  
Wait until `nslookup api.yourdomain.com` returns that IP (can take a few minutes).

## 2. Bind Docker to localhost only

So **only Caddy** talks to the API on the host (not the whole internet on :8000).

On the VPS:

```bash
cd /opt/talabat_area_intel
nano docker-compose.worker.yml
```

Change:

```yaml
    ports:
      - "8000:8000"
```

to:

```yaml
    ports:
      - "127.0.0.1:8000:8000"
```

Recreate:

```bash
docker compose -f docker-compose.worker.yml up -d --force-recreate
curl -sS http://127.0.0.1:8000/health
```

`curl` **from the internet** to `:8000` should **fail** now; that is expected.

## 3. Hetzner firewall

- **Remove** inbound **TCP 8000** (public).
- **Allow** **TCP 80** and **TCP 443** (Let’s Encrypt HTTP-01 / redirects).
- Keep **TCP 22** (SSH) from your IP.

## 4. Install Caddy on Ubuntu (host, not in Docker)

Official install: [https://caddyserver.com/docs/install#debian-ubuntu-raspbian](https://caddyserver.com/docs/install#debian-ubuntu-raspbian)

Example (Ubuntu):

```bash
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install -y caddy
```

## 5. Caddyfile

```bash
sudo nano /etc/caddy/Caddyfile
```

Minimal content (replace with **your** hostname):

```caddyfile
{
	email you@yourdomain.com
}

api.yourdomain.com {
	reverse_proxy 127.0.0.1:8000
}
```

- **`email`** — Let’s Encrypt registration; use a real mailbox you read.
- **`api.yourdomain.com`** — must match the DNS **A** record.

Validate and reload:

```bash
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
sudo systemctl status caddy --no-pager
```

## 6. Test HTTPS

From your laptop:

```powershell
curl.exe -sS https://api.yourdomain.com/health
curl.exe -sS -H "X-API-Key: YOUR_KEY" https://api.yourdomain.com/health/scrape-config
```

## 7. Streamlit Cloud secrets

```toml
API_BASE_URL = "https://api.yourdomain.com"
SCRAPER_API_KEY = "same key as env.worker.local on the VPS"
```

Redeploy the Streamlit app.

## Troubleshooting

- **Certificate pending / 502** — DNS not propagated yet, or ports **80/443** blocked; check `sudo journalctl -u caddy -n 80 --no-pager`.
- **502 Bad Gateway** — Docker not listening on `127.0.0.1:8000` (typo in `docker-compose.worker.yml` or container down): `docker compose -f docker-compose.worker.yml ps`.
- **Still want HTTP for debugging** — temporarily open `127.0.0.1:8000` mapping wrong; must be **127.0.0.1** only when Caddy fronts TLS.

See also `deploy/Caddyfile.example` in this repo (same idea, shorter).
