"""
FleetStream — Serving Layer API  (v2 — Senior)
===============================================
Bridge Lambda Architecture : Hot Path (Redis) ↔ Cold Path (DuckDB/Parquet).

Hot Path  (<10ms) : Redis Stack GEOSEARCH
Cold Path (analytics) : DuckDB sur fichiers Parquet hive-partitionnés

Monitoring : Prometheus via /metrics (prometheus-fastapi-instrumentator)
             + métriques custom fleet_active_livreurs, fleet_messages_processed

Docs interactives : http://localhost:8001/docs
"""
import asyncio
import logging
import os
import statistics
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, Optional

import duckdb
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("api")

# ── Config ──────────────────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
DATA_PATH = Path(os.getenv("DATA_PATH", "/data/parquet"))

GEO_KEY          = "fleet:geo"
HASH_PREFIX      = "fleet:livreur:"
STATS_MSGS_KEY   = "fleet:stats:total_messages"

# ── Prometheus custom metrics ───────────────────────────────────────────────────
gauge_active_livreurs = Gauge(
    "fleet_active_livreurs",
    "Nombre de livreurs actifs dans Redis (TTL 30s)",
)
gauge_messages_processed = Gauge(
    "fleet_messages_processed",
    "Total de messages GPS traités par le hot consumer",
)

# ── Prometheus HTTP instrumentator (configure avant app) ────────────────────────
instrumentator = Instrumentator(
    should_group_status_codes=False,
    should_ignore_untemplated=True,
    excluded_handlers=["/metrics", "/health"],
)


# ── Lifespan ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connexion Redis
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    # Expose /metrics AVANT de lancer la boucle d'events
    instrumentator.expose(app, endpoint="/metrics", include_in_schema=False)

    # Tâche de fond : mise à jour des gauges Prometheus toutes les 5s
    async def _update_fleet_metrics() -> None:
        while True:
            try:
                r: aioredis.Redis = app.state.redis
                active = await r.zcard(GEO_KEY)
                total  = int(await r.get(STATS_MSGS_KEY) or 0)
                gauge_active_livreurs.set(active)
                gauge_messages_processed.set(total)
            except Exception:
                pass
            await asyncio.sleep(5)

    task = asyncio.create_task(_update_fleet_metrics())
    log.info("FleetStream API v2 démarrée — Redis: %s | Data: %s", REDIS_URL, DATA_PATH)
    yield
    task.cancel()
    await app.state.redis.aclose()
    log.info("API arrêtée proprement.")


# ── App ─────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="FleetStream API",
    description=(
        "## Architecture Lambda — Tracking Flotte Temps Réel\n\n"
        "**Hot Path** (Redis Stack) : GEOSEARCH en <10ms\n\n"
        "**Cold Path** (DuckDB/Parquet) : Analytics & historique\n\n"
        "**Monitoring** : `/metrics` (Prometheus)"
    ),
    version="2.0.0",
    lifespan=lifespan,
)

instrumentator.instrument(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Sert le dashboard HTML statique depuis le dossier /dashboard
_DASHBOARD_DIR = Path(__file__).parent / "static"
if _DASHBOARD_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_DASHBOARD_DIR)), name="static")

@app.get("/map", include_in_schema=False)
async def live_map():
    """Dashboard HTML live — carte Leaflet sans clignotement."""
    p = Path(__file__).parent / "static" / "index.html"
    if p.exists():
        return FileResponse(str(p), media_type="text/html")
    return {"detail": "Dashboard non trouvé — monter le volume /static"}


# ── Modèles ─────────────────────────────────────────────────────────────────────
class LivreurPosition(BaseModel):
    livreur_id: str
    lat: float
    lon: float
    speed_kmh: float = 0.0
    heading_deg: float = 0.0
    status: str = "unknown"
    accuracy_m: float = 0.0
    ts: str = ""
    distance_km: Optional[float] = None


class NearbyResponse(BaseModel):
    count: int
    rayon_km: float
    livreurs: list[LivreurPosition]


class HistoryResume(BaseModel):
    nb_points: int
    distance_totale_km: float = Field(description="Distance parcourue (Haversine)")
    vitesse_moy_kmh: float
    vitesse_max_kmh: float
    statut_dominant: str
    premiere_position: Optional[dict] = None
    derniere_position: Optional[dict] = None


class HistoryResponse(BaseModel):
    livreur_id: str
    heures: int
    resume: HistoryResume
    trajectory: list[dict]


# ── Utilitaires ─────────────────────────────────────────────────────────────────
def _parquet_glob() -> str:
    return str(DATA_PATH / "**" / "*.parquet")


def _percentile(data: list[float], pct: int) -> float:
    if not data:
        return 0.0
    data_sorted = sorted(data)
    idx = max(0, int(len(data_sorted) * pct / 100) - 1)
    return round(data_sorted[idx], 3)


# ════════════════════════════════════════════════════════════════════════════════
#  HOT PATH
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/livreurs-proches",
    response_model=NearbyResponse,
    summary="Livreurs dans un rayon — Redis GEOSEARCH (<10ms)",
    tags=["Hot Path"],
)
async def livreurs_proches(
    lat:    float = Query(..., description="Latitude",  example=48.8566),
    lon:    float = Query(..., description="Longitude", example=2.3522),
    rayon:  float = Query(1.5, description="Rayon en km", ge=0.1, le=50.0),
    statut: Optional[Literal["available", "delivering", "idle"]] = Query(None),
    limit:  int   = Query(50, ge=1, le=200),
):
    r: aioredis.Redis = app.state.redis

    raw = await r.geosearch(
        GEO_KEY,
        longitude=lon, latitude=lat,
        radius=rayon, unit="km",
        withcoord=True, withdist=True,
        sort="ASC", count=limit,
    )
    if not raw:
        return NearbyResponse(count=0, rayon_km=rayon, livreurs=[])

    pipe = r.pipeline(transaction=False)
    for item in raw:
        pipe.hgetall(f"{HASH_PREFIX}{item[0]}")
    hashes = await pipe.execute()

    livreurs: list[LivreurPosition] = []
    for item, h in zip(raw, hashes):
        name, dist, (glon, glat) = item
        if not h:
            continue
        if statut and h.get("status") != statut:
            continue
        livreurs.append(LivreurPosition(
            livreur_id=name,
            lat=float(h.get("lat", glat)),
            lon=float(h.get("lon", glon)),
            speed_kmh=float(h.get("speed_kmh", 0)),
            heading_deg=float(h.get("heading_deg", 0)),
            status=h.get("status", "unknown"),
            accuracy_m=float(h.get("accuracy_m", 0)),
            ts=h.get("ts", ""),
            distance_km=round(dist, 3),
        ))

    return NearbyResponse(count=len(livreurs), rayon_km=rayon, livreurs=livreurs)


@app.get(
    "/livreurs/{livreur_id}",
    response_model=LivreurPosition,
    summary="Position temps réel d'un livreur",
    tags=["Hot Path"],
)
async def get_livreur(livreur_id: str):
    r: aioredis.Redis = app.state.redis
    h = await r.hgetall(f"{HASH_PREFIX}{livreur_id}")
    if not h:
        raise HTTPException(
            status_code=404,
            detail=f"'{livreur_id}' introuvable — TTL expiré ou livreur inexistant.",
        )
    return LivreurPosition(
        livreur_id=livreur_id,
        lat=float(h.get("lat", 0)),
        lon=float(h.get("lon", 0)),
        speed_kmh=float(h.get("speed_kmh", 0)),
        heading_deg=float(h.get("heading_deg", 0)),
        status=h.get("status", "unknown"),
        accuracy_m=float(h.get("accuracy_m", 0)),
        ts=h.get("ts", ""),
    )


@app.get(
    "/stats",
    summary="Métriques temps réel (Hot + Cold Path)",
    tags=["Hot Path"],
)
async def stats():
    r: aioredis.Redis = app.state.redis

    total_geo  = await r.zcard(GEO_KEY)
    total_msgs = int(await r.get(STATS_MSGS_KEY) or 0)

    keys = await r.keys(f"{HASH_PREFIX}*")
    status_counts: dict[str, int] = {"available": 0, "delivering": 0, "idle": 0}
    if keys:
        pipe = r.pipeline(transaction=False)
        for k in keys:
            pipe.hget(k, "status")
        for s in await pipe.execute():
            if s in status_counts:
                status_counts[s] += 1

    parquet_files = list(DATA_PATH.glob("**/*.parquet")) if DATA_PATH.exists() else []

    return {
        "hot_path": {
            "livreurs_actifs":    total_geo,
            "messages_traites":   total_msgs,
            "statuts":            status_counts,
            "backend":            "Redis Stack (GEOSEARCH)",
            "ttl_secondes":       30,
        },
        "cold_path": {
            "fichiers_parquet":   len(parquet_files),
            "taille_totale_mb":   round(
                sum(f.stat().st_size for f in parquet_files) / 1_048_576, 2
            ),
            "backend":            "Apache Parquet (Snappy) + DuckDB",
        },
    }


# ════════════════════════════════════════════════════════════════════════════════
#  COLD PATH — DuckDB / Parquet
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/analytics/history/{livreur_id}",
    response_model=HistoryResponse,
    summary="Historique complet + stats de trajectoire (Cold Path — DuckDB)",
    tags=["Cold Path"],
)
async def history(
    livreur_id: str,
    heures: int = Query(1, description="Fenêtre temporelle en heures", ge=1, le=24),
):
    """
    Boucle Lambda : lit le Data Lake Parquet via DuckDB pour reconstruire
    la trajectoire complète d'un livreur avec statistiques de mouvement.

    Inclut : distance parcourue (Haversine), vitesse moyenne/max, statut dominant.
    """
    glob = _parquet_glob()
    try:
        conn = duckdb.connect()

        # glob vient de DATA_PATH (config serveur, pas entrée user) → interpolation safe
        # livreur_id et heures restent paramétrés (protection injection)
        rows = conn.execute(
            f"""
            SELECT lat, lon, speed_kmh, heading_deg, status, ts
            FROM   read_parquet('{glob}', hive_partitioning = true)
            WHERE  livreur_id = ?
              AND  ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            ORDER BY ts
            """,
            [livreur_id, heures],
        ).fetchall()

        if not rows:
            conn.close()
            raise HTTPException(
                status_code=404,
                detail=f"Aucune donnée historique pour '{livreur_id}' sur {heures}h.",
            )

        # Distance totale (Haversine vectorisé côté DuckDB)
        dist_row = conn.execute(
            f"""
            WITH ordered AS (
                SELECT lat, lon,
                       LAG(lat) OVER (ORDER BY ts) AS prev_lat,
                       LAG(lon) OVER (ORDER BY ts) AS prev_lon
                FROM   read_parquet('{glob}', hive_partitioning = true)
                WHERE  livreur_id = ?
                  AND  ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            )
            SELECT COALESCE(SUM(
                6371.0 * 2 * ASIN(SQRT(
                    POWER(SIN(RADIANS(lat - prev_lat) / 2), 2) +
                    COS(RADIANS(prev_lat)) * COS(RADIANS(lat)) *
                    POWER(SIN(RADIANS(lon - prev_lon) / 2), 2)
                ))
            ), 0) AS total_km
            FROM ordered WHERE prev_lat IS NOT NULL
            """,
            [livreur_id, heures],
        ).fetchone()

        conn.close()

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    speeds    = [r[2] for r in rows if r[2] is not None]
    statuses  = [r[4] for r in rows if r[4]]
    dominant  = max(set(statuses), key=statuses.count) if statuses else "unknown"

    resume = HistoryResume(
        nb_points=len(rows),
        distance_totale_km=round(dist_row[0] if dist_row else 0.0, 2),
        vitesse_moy_kmh=round(statistics.mean(speeds), 1) if speeds else 0.0,
        vitesse_max_kmh=round(max(speeds), 1) if speeds else 0.0,
        statut_dominant=dominant,
        premiere_position={"lat": rows[0][0],  "lon": rows[0][1],  "ts": str(rows[0][5])},
        derniere_position={"lat": rows[-1][0], "lon": rows[-1][1], "ts": str(rows[-1][5])},
    )

    trajectory = [
        {
            "lat": r[0], "lon": r[1],
            "speed_kmh": r[2], "heading_deg": r[3],
            "status": r[4], "ts": str(r[5]),
        }
        for r in rows
    ]

    return HistoryResponse(
        livreur_id=livreur_id,
        heures=heures,
        resume=resume,
        trajectory=trajectory,
    )


@app.get(
    "/analytics/heatmap",
    summary="Densité de flotte pour heatmap (Cold Path — DuckDB)",
    tags=["Cold Path"],
)
async def heatmap(
    heures:     int   = Query(1, ge=1, le=24),
    resolution: float = Query(0.01, ge=0.001, le=0.1,
                              description="Taille de cellule en degrés (~1km)"),
):
    glob = _parquet_glob()
    try:
        conn = duckdb.connect()
        rows = conn.execute(
            f"""
            SELECT
                ROUND(lat / ?, 0) * ?  AS lat_cell,
                ROUND(lon / ?, 0) * ?  AS lon_cell,
                COUNT(*)               AS nb_passages,
                AVG(speed_kmh)         AS avg_speed
            FROM   read_parquet('{glob}', hive_partitioning = true)
            WHERE  ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            GROUP  BY lat_cell, lon_cell
            ORDER  BY nb_passages DESC
            LIMIT  500
            """,
            [resolution, resolution, resolution, resolution, heures],
        ).fetchall()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    return {
        "heures":        heures,
        "resolution_deg": resolution,
        "nb_cellules":   len(rows),
        "heatmap": [
            {"lat": r[0], "lon": r[1], "nb_passages": r[2], "avg_speed_kmh": round(r[3] or 0, 1)}
            for r in rows
        ],
    }


# ════════════════════════════════════════════════════════════════════════════════
#  PERFORMANCE / MONITORING
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/health/performance",
    summary="Benchmark Redis live — Preuve SLA <10ms",
    tags=["Monitoring"],
)
async def performance_check(
    samples: int = Query(200, description="Nombre de GEOSEARCH à mesurer", ge=10, le=1000),
):
    """
    Lance N requêtes GEOSEARCH contre Redis et retourne les percentiles.
    Utilisé pour prouver que le Hot Path respecte le SLA <10ms (p99).
    """
    r: aioredis.Redis = app.state.redis

    # Warm-up (5 requêtes ignorées)
    for _ in range(5):
        await r.geosearch(GEO_KEY, longitude=2.3522, latitude=48.8566,
                          radius=15, unit="km", count=10)

    latencies_ms: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await r.geosearch(
            GEO_KEY,
            longitude=2.3522, latitude=48.8566,
            radius=15, unit="km",
            withcoord=False, withdist=False,
            count=100,
        )
        latencies_ms.append((time.perf_counter() - t0) * 1000)

    # Redis INFO
    info = await r.info("all")

    sla_ok = _percentile(latencies_ms, 99) < 10.0

    return {
        "sla_hot_path_ok": sla_ok,
        "sla_target":      "<10ms (p99)",
        "geosearch_benchmark": {
            "samples":  samples,
            "p50_ms":   _percentile(latencies_ms, 50),
            "p95_ms":   _percentile(latencies_ms, 95),
            "p99_ms":   _percentile(latencies_ms, 99),
            "mean_ms":  round(statistics.mean(latencies_ms), 3),
            "max_ms":   round(max(latencies_ms), 3),
        },
        "redis_info": {
            "version":               info.get("redis_version"),
            "connected_clients":     info.get("connected_clients"),
            "used_memory_human":     info.get("used_memory_human"),
            "ops_per_sec":           info.get("instantaneous_ops_per_sec"),
            "total_commands":        info.get("total_commands_processed"),
            "keyspace_hits":         info.get("keyspace_hits"),
            "keyspace_misses":       info.get("keyspace_misses"),
        },
        "architecture": "Lambda Architecture (Speed Layer + Batch Layer)",
    }


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "version": "2.0.0"}
