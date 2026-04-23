# ── Build stage ────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY api/requirements.txt ./requirements.txt
# Installe dans un préfixe isolé pour le multi-stage copy
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Recupere seulement les packages installes, pas les outils de build
COPY --from=builder /install /usr/local

# duckdb + pyarrow ont besoin de libgomp
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY api/. .
RUN mkdir -p /app/context_poller /app/tlc_replay
COPY context_poller/nyc_zone_centroids.json /app/context_poller/nyc_zone_centroids.json
COPY tlc_replay/nyc_zone_centroids.json /app/tlc_replay/nyc_zone_centroids.json

EXPOSE 8000

# 1 worker par défaut ; configurable via UVICORN_WORKERS pour les démos chargées.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS:-1}"]
