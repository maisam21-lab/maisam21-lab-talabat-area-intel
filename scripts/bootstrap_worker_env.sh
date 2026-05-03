#!/usr/bin/env bash
# Create env.worker.local from the template if missing (secrets are gitignored).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ ! -f env.worker.local ]]; then
  cp env.worker.example env.worker.local
  echo "Created env.worker.local — edit SCRAPER_API_KEY (and GOOGLE_MAPS_API_KEY if used), then run:"
  echo "  bash scripts/run_worker_docker.sh"
  exit 1
fi
echo "env.worker.local already exists."
exit 0
