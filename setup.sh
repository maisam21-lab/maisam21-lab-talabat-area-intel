#!/usr/bin/env bash
# One-shot bootstrap for Ubuntu on a Hetzner VM: Docker + Compose + TLS + stack.
# Usage: bash setup.sh
#
# After first run, edit .env (SCRAPER_API_KEY, optional GOOGLE_MAPS_API_KEY), then:
#   bash setup.sh
#
# Open: https://<VM_IP>/  (browser will warn on self-signed cert until you replace certs)
# External API (optional): https://<VM_IP>/api/health  (same X-API-Key as in .env)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ "${EUID}" -eq 0 ]]; then
  echo "Run as a normal user with sudo (not as root). Docker post-install adds your user to the docker group."
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Installing Docker Engine (official convenience script)…"
  sudo apt-get update -y
  sudo apt-get install -y ca-certificates curl
  curl -fsSL https://get.docker.com | sudo sh
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER" || true
  echo "Docker installed. Log out and back in if 'docker ps' says permission denied, then re-run: bash setup.sh"
  exit 0
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Installing docker compose plugin…"
  sudo apt-get update -y
  sudo apt-get install -y docker-compose-plugin
fi

mkdir -p deploy/ssl
if [[ ! -f deploy/ssl/cert.pem || ! -f deploy/ssl/key.pem ]]; then
  echo "Generating self-signed TLS cert for nginx (replace with real certs for production)…"
  openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
    -keyout deploy/ssl/key.pem \
    -out deploy/ssl/cert.pem \
    -subj "/CN=178.105.56.187"
  chmod 640 deploy/ssl/key.pem
fi

if [[ ! -f .env ]]; then
  echo "Created .env from .env.example — set SCRAPER_API_KEY, then run: bash setup.sh"
  cp .env.example .env
  exit 1
fi

echo "Building and starting api + streamlit + nginx…"
compose() {
  if docker info >/dev/null 2>&1; then
    docker compose "$@"
  else
    sudo docker compose "$@"
  fi
}
compose -f docker-compose.yml up -d --build

echo "Done. Open https://$(hostname -I | awk '{print $1}')/ (or your VM public IP, e.g. 178.105.56.187)."
echo "Ensure cloud firewall allows TCP 443 (and 80 for redirect)."
echo "Optional API check (from the VM): curl -sk https://127.0.0.1/api/health -H \"X-API-Key: <same as .env>\""
