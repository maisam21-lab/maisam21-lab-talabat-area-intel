#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ ! -f env.worker.local ]]; then
  echo "Missing env.worker.local — run: bash scripts/bootstrap_worker_env.sh"
  exit 1
fi
exec docker compose -f docker-compose.worker.yml up --build -d
