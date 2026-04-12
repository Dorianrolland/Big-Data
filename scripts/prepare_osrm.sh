#!/usr/bin/env bash
# ── Prepare OSRM data for New York routing ───────────────────────────────────
# Downloads the New York State OSM extract and pre-processes it for OSRM.
# Run this ONCE before starting docker-compose with the OSRM service.
#
# Usage:  bash scripts/prepare_osrm.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

OSRM_DIR="$(pwd)/data/osrm"
REGION_URL="https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf"
PBF_FILE="$OSRM_DIR/new-york-latest.osm.pbf"
PROFILE="car"

mkdir -p "$OSRM_DIR"

echo "==> Downloading New York State OSM extract..."
if [ ! -f "$PBF_FILE" ]; then
    curl -L -o "$PBF_FILE" "$REGION_URL"
    echo "    Downloaded to $PBF_FILE"
else
    echo "    Already exists: $PBF_FILE"
fi

echo "==> Extracting OSRM data (profile: $PROFILE)..."
docker run --rm -v "$OSRM_DIR:/data" osrm/osrm-backend:v5.27.1 \
    osrm-extract -p /opt/$PROFILE.lua /data/new-york-latest.osm.pbf

echo "==> Partitioning..."
docker run --rm -v "$OSRM_DIR:/data" osrm/osrm-backend:v5.27.1 \
    osrm-partition /data/new-york-latest.osrm

echo "==> Customizing..."
docker run --rm -v "$OSRM_DIR:/data" osrm/osrm-backend:v5.27.1 \
    osrm-customize /data/new-york-latest.osrm

echo "==> OSRM data ready in $OSRM_DIR"
echo "    Start the routing service with: docker compose --profile routing up osrm"
