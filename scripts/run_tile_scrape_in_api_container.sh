#!/usr/bin/env bash
# Run tile_radius_merged_scrape.py inside the API container (no docker-compose.yml needed).
#
# On the VPS (after git pull), set coords then:
#   bash scripts/run_tile_scrape_in_api_container.sh
#
# Env overrides:
#   API_CONTAINER   default: maisam21-lab-talabat-area-intel-api-1
#   CENTER_LAT      required unless already exported
#   CENTER_LNG      required unless already exported
#   RADIUS_KM       default 10
#   TILE_SPACING_KM default 3.5
#   MAX_PINS        default 8
#   OUT_CSV         default: /app/data/scrape_jobs/tile_merged.csv
set -euo pipefail

API_CONTAINER="${API_CONTAINER:-maisam21-lab-talabat-area-intel-api-1}"
RADIUS_KM="${RADIUS_KM:-10}"
TILE_SPACING_KM="${TILE_SPACING_KM:-3.5}"
MAX_PINS="${MAX_PINS:-8}"
OUT_CSV="${OUT_CSV:-/app/data/scrape_jobs/tile_merged.csv}"

if [[ -z "${CENTER_LAT:-}" || -z "${CENTER_LNG:-}" ]]; then
  echo "Set CENTER_LAT and CENTER_LNG (e.g. export CENTER_LAT=25.08 CENTER_LNG=55.14)" >&2
  exit 1
fi

docker exec \
  -e API_BASE_URL=http://127.0.0.1:8000 \
  "${API_CONTAINER}" \
  python /app/scripts/tile_radius_merged_scrape.py \
  --center-lat "${CENTER_LAT}" \
  --center-lng "${CENTER_LNG}" \
  --radius-km "${RADIUS_KM}" \
  --tile-spacing-km "${TILE_SPACING_KM}" \
  --max-pins "${MAX_PINS}" \
  -o "${OUT_CSV}"

echo "Done. CSV in container at ${OUT_CSV}"
echo "If ./data/scrape_jobs is mounted on the host, copy from that host path under your compose project."
