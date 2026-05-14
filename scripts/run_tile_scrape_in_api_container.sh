#!/usr/bin/env bash
# Run tile_radius_merged_scrape.py inside the API container (no docker-compose.yml needed).
#
# On the VPS: set coords, then either run from a git clone or:
#   curl -fsSL -o /tmp/run_tile.sh …/run_tile_scrape_in_api_container.sh && bash /tmp/run_tile.sh
# If the API image predates tile_radius_merged_scrape.py, this script downloads it and docker-cp's it in.
#
# Env overrides:
#   API_CONTAINER   default: maisam21-lab-talabat-area-intel-api-1
#   CENTER_LAT      required unless already exported
#   CENTER_LNG      required unless already exported
#   RADIUS_KM       default 10
#   TILE_SPACING_KM default 3.5
#   MAX_PINS        default 8
#   OUT_CSV         default: /app/data/scrape_jobs/tile_merged.csv
#   TILE_SCRIPT_URL optional: fetch script into container if missing (old images)
set -euo pipefail

API_CONTAINER="${API_CONTAINER:-maisam21-lab-talabat-area-intel-api-1}"
RADIUS_KM="${RADIUS_KM:-10}"
TILE_SPACING_KM="${TILE_SPACING_KM:-3.5}"
MAX_PINS="${MAX_PINS:-8}"
OUT_CSV="${OUT_CSV:-/app/data/scrape_jobs/tile_merged.csv}"
TILE_SCRIPT_URL="${TILE_SCRIPT_URL:-https://raw.githubusercontent.com/maisam21-lab/maisam21-lab-talabat-area-intel/main/scripts/tile_radius_merged_scrape.py}"

if [[ -z "${CENTER_LAT:-}" || -z "${CENTER_LNG:-}" ]]; then
  echo "Set CENTER_LAT and CENTER_LNG (e.g. export CENTER_LAT=25.08 CENTER_LNG=55.14)" >&2
  exit 1
fi

TILE_PY="/app/scripts/tile_radius_merged_scrape.py"
if ! docker exec "${API_CONTAINER}" test -f "${TILE_PY}" 2>/dev/null; then
  echo "Script missing in image; hot-patching from ${TILE_SCRIPT_URL} …" >&2
  tmp="$(mktemp)"
  curl -fsSL -o "${tmp}" "${TILE_SCRIPT_URL}"
  docker exec "${API_CONTAINER}" mkdir -p /app/scripts
  docker cp "${tmp}" "${API_CONTAINER}:${TILE_PY}"
  rm -f "${tmp}"
  echo "Patched ${TILE_PY} into ${API_CONTAINER}." >&2
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
