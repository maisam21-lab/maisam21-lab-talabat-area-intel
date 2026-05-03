# Deploy the Talabat scraper API on Hetzner Cloud (Docker)

Use this when **Streamlit Cloud** (or many users) should call **one** backend with better egress than shared datacenter hosts.

---

## Part A — Hetzner Cloud (browser, step by step)

### A1. Account

1. Go to [https://console.hetzner.cloud/](https://console.hetzner.cloud/).
2. Sign up or log in.
3. Add a **payment method** when prompted (Hetzner bills monthly; amounts are small for a 2 GB server).

### A2. SSH key (so you can log in)

1. On your **own PC** (Windows: PowerShell), check if you already have a key:

   ```powershell
   dir $env:USERPROFILE\.ssh
   ```

2. If you do **not** have `id_ed25519.pub` (or `id_rsa.pub`), create one:

   ```powershell
   ssh-keygen -t ed25519 -C "hetzner-talabat-worker" -f $env:USERPROFILE\.ssh\id_ed25519
   ```

   Press Enter for empty passphrase (ok for a throwaway dev key) or set one.

3. Show the **public** key (you will paste this into Hetzner):

   ```powershell
   Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub
   ```

4. In Hetzner Cloud Console: **Security** → **SSH Keys** → **Add SSH key**.  
   - Paste the **full** line starting with `ssh-ed25519`.  
   - Name it e.g. `laptop-2026`.

### A3. Create the server

1. **Projects** → select your project (or create one, e.g. `kitchenpark`).
2. Click **Add Server**.
3. **Location:** pick any EU datacenter (e.g. **Nuremberg** or **Helsinki**); fine for Talabat UAE.
4. **Image:** **Ubuntu 22.04**.
5. **Type:** under **Shared vCPU**, choose a server with **at least 2 GB RAM** (e.g. **CX22** or current equivalent — name may change; look at RAM).
6. **Networking:** leave **IPv4** (and IPv6 if offered) enabled.
7. **SSH keys:** tick your key from A2.
8. **Volumes:** skip.
9. **Firewalls:** leave empty for now (we add a firewall in A4), *or* create A4 first and attach it here.
10. **Name:** e.g. `talabat-scraper-api`.
11. Click **Create & Buy now**.

Wait until status is **Running**. Note the **IPv4 address** (e.g. `95.x.x.x`).

### A4. Firewall (recommended)

1. In the console: **Firewalls** → **Create Firewall**.
2. **Inbound rules:**
   - **SSH**, TCP port **22**, source **Your IP** (or a narrow range).  
     - Hetzner can fill “My IP” if you use that option.
   - For a **quick first deploy:** TCP **8000**, source **Any IPv4** (you can remove this later and use HTTPS only).
3. Save. Open the firewall → **Resources** → **Apply to Server** → select `talabat-scraper-api`.

### A5. First SSH login

On your PC:

```powershell
ssh root@YOUR_SERVER_IPV4
```

Type `yes` if asked about host key fingerprint. You should get a root shell on Ubuntu.

---

## Part B — On the server (install Docker + app)

### B1. Update system

```bash
apt-get update && apt-get upgrade -y
```

### B2. Install Docker Engine + Compose plugin

```bash
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

(Optional but nice: use a non-root user with `docker` group; for a first pass, running as `root` is common on a single-purpose VPS.)

### B3. Clone the repo

Replace the URL with **your** GitHub repo (must include the `docker-compose.worker.yml` and `scripts/` from this project):

```bash
apt-get install -y git
cd /opt
git clone https://github.com/YOUR_ORG/YOUR_REPO.git talabat_area_intel
cd talabat_area_intel
```

If the repo is **private**, use a [GitHub deploy key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/managing-deploy-keys) or HTTPS with a PAT.

### B4. Create env and set secrets

```bash
bash scripts/bootstrap_worker_env.sh
nano env.worker.local
```

Set at least:

- **`SCRAPER_API_KEY`** — long random string (e.g. 32+ chars). Streamlit will send the same value as **`X-API-Key`**.
- **`GOOGLE_MAPS_API_KEY`** — optional; for Places / geocode / maps if you use them.

Save: **Ctrl+O**, Enter, **Ctrl+X**.

### B5. Start the API

```bash
bash scripts/run_worker_docker.sh
```

Check:

```bash
docker compose -f docker-compose.worker.yml ps
docker compose -f docker-compose.worker.yml logs --tail 80
```

### B6. Smoke test (from your laptop)

```powershell
curl -sS "http://YOUR_SERVER_IPV4:8000/health"
curl -sS -H "X-API-Key: YOUR_SCRAPER_API_KEY" "http://YOUR_SERVER_IPV4:8000/health/scrape-config"
```

You should see JSON including `"ok": true` and `"scraper_vendor_page_enrich": true` when `SCRAPER_VENDOR_PAGE_ENRICH=1` is in `env.worker.local`.

---

## Part C — Streamlit (all users)

1. **Streamlit Cloud** → your app → **Settings → Secrets**.
2. Set:

   ```toml
   API_BASE_URL = "http://YOUR_SERVER_IPV4:8000"
   SCRAPER_API_KEY = "same value as SCRAPER_API_KEY on the VPS"
   ```

3. **Redeploy** the app (or “Reboot”).
4. In the app sidebar, choose profile **Worker (vendor pages)** for long runs with **`enrich: true`**.

---

## Part D — HTTPS (optional, recommended later)

1. Point a DNS name (e.g. `api.yourdomain.com`) **A record** to the server IPv4.
2. Install **Caddy** on the host and use `deploy/Caddyfile.example` as a template (replace `api.example.com`).
3. Change `docker-compose.worker.yml` ports to bind **only localhost**:

   ```yaml
   ports:
     - "127.0.0.1:8000:8000"
   ```

4. Recreate the container, then set Streamlit **`API_BASE_URL`** to `https://api.yourdomain.com`.
5. Remove **8000** from the Hetzner firewall inbound rules; allow **443** from the world.

---

## Troubleshooting

- **`scraper_vendor_page_enrich`: false** in `/health/scrape-config` → set `SCRAPER_VENDOR_PAGE_ENRICH=1` in `env.worker.local`, then `docker compose -f docker-compose.worker.yml up -d --force-recreate`.
- **Cannot SSH** → check firewall allows **22** from your current IP; Hetzner console also offers **Rescue** / **Web console**.
- **0 Talabat rows** → check `docker compose -f docker-compose.worker.yml logs --tail 200`; try another Hetzner **location** or plan for a **proxy** if Talabat blocks that IP range.
- **Out of memory** → upgrade to **4 GB RAM** or lower `MAX_SCRAPE_SAMPLE_POINTS` / `SCRAPER_PLAYWRIGHT_CONCURRENCY` in `env.worker.local`.
