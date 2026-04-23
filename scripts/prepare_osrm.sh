#!/usr/bin/env bash
# ── Prepare OSRM data for New York routing ───────────────────────────────────
# Downloads the New York State OSM extract and pre-processes it for OSRM.
# Run this ONCE before starting docker-compose with the OSRM service.
#
# Usage:  bash scripts/prepare_osrm.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# MSYS/Git-Bash on Windows rewrites Unix-looking paths when passing them to
# docker.exe, which breaks the volume mount. Disable that rewrite.
export MSYS_NO_PATHCONV=1

REGION_URL="https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf"
PROFILE="car"
IMAGE="ghcr.io/project-osrm/osrm-backend:v5.27.1"

# Resolve an absolute host path that both Unix shells and Docker Desktop for
# Windows accept. `pwd -W` gives a Windows-style path on MSYS (C:/...); on
# Linux/macOS it's not supported, fall back to plain pwd.
if host_root="$(pwd -W 2>/dev/null)"; then
    :
else
    host_root="$(pwd)"
fi
OSRM_DIR="${host_root}/data/osrm"
PBF_FILE="$OSRM_DIR/new-york-latest.osm.pbf"

mkdir -p "$OSRM_DIR"

echo "==> Downloading New York State OSM extract..."
if [ ! -f "$PBF_FILE" ]; then
    curl -L -o "$PBF_FILE" "$REGION_URL"
    echo "    Downloaded to $PBF_FILE"
else
    echo "    Already exists: $PBF_FILE"
fi

echo "==> Extracting OSRM data (profile: $PROFILE)..."
docker run --rm -v "${OSRM_DIR}:/data" "$IMAGE" \
    osrm-extract -p /opt/$PROFILE.lua /data/new-york-latest.osm.pbf

echo "==> Partitioning..."
docker run --rm -v "${OSRM_DIR}:/data" "$IMAGE" \
    osrm-partition /data/new-york-latest.osrm

echo "==> Customizing..."
docker run --rm -v "${OSRM_DIR}:/data" "$IMAGE" \
    osrm-customize /data/new-york-latest.osrm

echo "==> OSRM data ready in $OSRM_DIR"
echo "    Start the routing service with: docker compose --profile routing up osrm"
