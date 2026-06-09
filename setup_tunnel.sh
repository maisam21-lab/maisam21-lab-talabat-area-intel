#!/bin/bash
# Sets up cloudflared as a systemd service so the tunnel survives reboots and restarts.
# Run once: curl -sL https://raw.githubusercontent.com/maisam21-lab/maisam21-lab-talabat-area-intel/main/setup_tunnel.sh | bash

set -e

pkill cloudflared 2>/dev/null || true
sleep 2

cat > /etc/systemd/system/cloudflared.service << 'UNIT'
[Unit]
Description=Cloudflare Quick Tunnel
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
ExecStart=/usr/local/bin/cloudflared tunnel --url https://localhost:443 --no-tls-verify
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable cloudflared
systemctl restart cloudflared

echo "Waiting for tunnel URL..."
sleep 12
URL=$(journalctl -u cloudflared -n 50 --no-pager 2>/dev/null | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1)
echo ""
echo "========================================="
echo "  TUNNEL URL: $URL"
echo "  APP URL:    $URL/api/ui/"
echo "========================================="
echo "$URL" > /tmp/tunnel_url.txt
echo "URL saved to /tmp/tunnel_url.txt"
