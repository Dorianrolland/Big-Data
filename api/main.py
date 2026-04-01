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


# ════════════════════════════════════════════════════════════════════════════════
#  BUSINESS INTELLIGENCE — Valeur opérationnelle & analytique
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/analytics/fleet-insights",
    summary="Tableau de bord opérationnel — KPIs temps réel + alertes flotte",
    tags=["Business Intelligence"],
)
async def fleet_insights(
    heures: int = Query(1, description="Fenêtre d'analyse historique", ge=1, le=24),
):
    """
    Vue métier complète de la flotte : taux d'utilisation, alertes opérationnelles,
    productivité globale. Combine Hot Path (Redis) et Cold Path (DuckDB).

    Cas d'usage : dashboard opérateur, alertes dispatch, reporting journalier.
    """
    r: aioredis.Redis = app.state.redis
    glob = _parquet_glob()

    # ── Hot Path : état instantané ───────────────────────────────────────────
    total_actifs = await r.zcard(GEO_KEY)
    keys = await r.keys(f"{HASH_PREFIX}*")
    status_counts: dict[str, int] = {"available": 0, "delivering": 0, "idle": 0}
    idle_suspects: list[dict] = []

    if keys:
        pipe = r.pipeline(transaction=False)
        for k in keys:
            pipe.hmget(k, "status", "speed_kmh", "ts", "lat", "lon")
        results = await pipe.execute()
        for k, vals in zip(keys, results):
            status, speed, ts, lat, lon = vals
            if status in status_counts:
                status_counts[status] += 1
            # Livreur "delivering" mais vitesse quasi nulle → suspect
            try:
                if status == "delivering" and float(speed or 0) < 1.5:
                    idle_suspects.append({
                        "livreur_id": k.replace(HASH_PREFIX, ""),
                        "status": status,
                        "speed_kmh": round(float(speed or 0), 1),
                        "ts": ts,
                    })
            except (ValueError, TypeError):
                pass

    utilisation_pct = round(
        status_counts["delivering"] / total_actifs * 100 if total_actifs else 0, 1
    )
    disponibilite_pct = round(
        status_counts["available"] / total_actifs * 100 if total_actifs else 0, 1
    )
    inactivite_pct = round(
        status_counts["idle"] / total_actifs * 100 if total_actifs else 0, 1
    )

    # ── Cold Path : productivité historique ──────────────────────────────────
    cold_stats = {}
    try:
        conn = duckdb.connect()
        row = conn.execute(
            f"""
            SELECT
                COUNT(DISTINCT livreur_id)                              AS nb_livreurs,
                ROUND(AVG(speed_kmh), 1)                                AS vitesse_moy,
                ROUND(AVG(CASE WHEN status = 'delivering' THEN 1.0 ELSE 0.0 END) * 100, 1)
                                                                        AS taux_livraison_pct,
                COUNT(*) FILTER (WHERE speed_kmh > 50)                  AS alertes_vitesse,
                COUNT(*) FILTER (WHERE speed_kmh < 1 AND status = 'delivering')
                                                                        AS stops_suspects
            FROM read_parquet('{glob}', hive_partitioning = true)
            WHERE ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            """,
            [heures],
        ).fetchone()
        conn.close()
        if row:
            cold_stats = {
                "livreurs_actifs_periode": row[0],
                "vitesse_moyenne_kmh":     row[1],
                "taux_livraison_pct":      row[2],
                "alertes_vitesse_exces":   row[3],
                "stops_suspects_total":    row[4],
            }
    except Exception as exc:
        log.warning("fleet-insights cold path error: %s", exc)

    # ── Score de santé opérationnelle (0-100) ────────────────────────────────
    score = 100
    if utilisation_pct < 50:
        score -= 20   # Trop de livreurs inactifs
    if inactivite_pct > 20:
        score -= 15   # Trop de livreurs en idle
    if len(idle_suspects) > 5:
        score -= 15   # Beaucoup de livreurs bloqués
    if cold_stats.get("alertes_vitesse_exces", 0) > 10:
        score -= 10   # Excès de vitesse fréquents
    score = max(0, score)

    return {
        "periode_heures": heures,
        "sante_operationnelle": {
            "score": score,
            "niveau": "excellent" if score >= 85 else "bon" if score >= 65 else "attention" if score >= 45 else "critique",
        },
        "flotte_temps_reel": {
            "total_actifs":        total_actifs,
            "statuts":             status_counts,
            "taux_utilisation_pct": utilisation_pct,
            "taux_disponibilite_pct": disponibilite_pct,
            "taux_inactivite_pct": inactivite_pct,
        },
        "alertes": {
            "livreurs_immobiles_en_livraison": len(idle_suspects),
            "detail_suspects": idle_suspects[:5],  # top 5
        },
        "productivite_historique": cold_stats,
        "recommandations": _build_recommendations(
            utilisation_pct, inactivite_pct, len(idle_suspects),
            cold_stats.get("alertes_vitesse_exces", 0),
        ),
    }


def _build_recommendations(
    util_pct: float, idle_pct: float, suspects: int, speed_alerts: int
) -> list[str]:
    recs = []
    if util_pct < 50:
        recs.append(
            f"Seulement {util_pct}% de la flotte en livraison active — "
            "envisager de réduire le nombre de livreurs connectés ou de stimuler la demande."
        )
    if idle_pct > 20:
        recs.append(
            f"{idle_pct}% de livreurs en idle — vérifier si certains se sont déconnectés "
            "sans mettre à jour leur statut."
        )
    if suspects > 3:
        recs.append(
            f"{suspects} livreur(s) en statut 'delivering' mais quasi à l'arrêt — "
            "possible problème de véhicule, accident ou application gelée."
        )
    if speed_alerts > 10:
        recs.append(
            f"{speed_alerts} enregistrements à vitesse excessive (>50 km/h) — "
            "risque assurance et conformité réglementaire."
        )
    if not recs:
        recs.append("Flotte opérationnelle nominale — aucune action requise.")
    return recs


# ────────────────────────────────────────────────────────────────────────────────

@app.get(
    "/analytics/driver-score/{livreur_id}",
    summary="Score de performance livreur — KPIs RH & assurance",
    tags=["Business Intelligence"],
)
async def driver_score(
    livreur_id: str,
    heures: int = Query(1, description="Fenêtre d'analyse en heures", ge=1, le=24),
):
    """
    Score de performance individuel calculé sur le Cold Path (DuckDB/Parquet).

    Métriques : productivité (% temps en livraison), efficacité (vitesse moyenne),
    sécurité (excès de vitesse), fiabilité (continuité de service).

    Cas d'usage : primes de performance, tarification assurance, onboarding RH.
    """
    glob = _parquet_glob()
    try:
        conn = duckdb.connect()
        row = conn.execute(
            f"""
            WITH ordered AS (
                SELECT
                    speed_kmh, status, ts, lat, lon,
                    LAG(lat) OVER (ORDER BY ts) AS prev_lat,
                    LAG(lon) OVER (ORDER BY ts) AS prev_lon
                FROM read_parquet('{glob}', hive_partitioning = true)
                WHERE livreur_id = ?
                  AND ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            ),
            stats AS (
                SELECT
                    COUNT(*)                                                         AS nb_points,
                    ROUND(AVG(speed_kmh), 1)                                        AS vitesse_moy,
                    ROUND(MAX(speed_kmh), 1)                                        AS vitesse_max,
                    ROUND(STDDEV(speed_kmh), 2)                                     AS vitesse_ecart_type,
                    COUNT(*) FILTER (WHERE status = 'delivering')                   AS ticks_delivering,
                    COUNT(*) FILTER (WHERE status = 'available')                    AS ticks_available,
                    COUNT(*) FILTER (WHERE status = 'idle')                         AS ticks_idle,
                    COUNT(*) FILTER (WHERE speed_kmh > 50)                          AS exces_vitesse,
                    COUNT(*) FILTER (WHERE speed_kmh < 1 AND status = 'delivering') AS arrets_suspects,
                    COALESCE(SUM(
                        CASE WHEN prev_lat IS NOT NULL THEN
                            6371.0 * 2 * ASIN(SQRT(
                                POWER(SIN(RADIANS(lat - prev_lat) / 2), 2) +
                                COS(RADIANS(prev_lat)) * COS(RADIANS(lat)) *
                                POWER(SIN(RADIANS(lon - prev_lon) / 2), 2)
                            ))
                        ELSE 0 END
                    ), 0)                                                            AS distance_km
                FROM ordered
            )
            SELECT * FROM stats
            """,
            [livreur_id, heures],
        ).fetchone()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    if not row or row[0] == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune donnée pour '{livreur_id}' sur {heures}h.",
        )

    nb, v_moy, v_max, v_std, t_del, t_av, t_idle, exces, arrets, dist = row
    total = nb or 1

    taux_livraison  = round(t_del / total * 100, 1)
    taux_disponible = round(t_av  / total * 100, 1)
    taux_idle       = round(t_idle / total * 100, 1)

    # ── Scoring composé (0-100) ──────────────────────────────────────────────
    # Productivité (40 pts) : % temps en livraison
    score_productivite = min(40, round(taux_livraison * 0.4, 1))

    # Sécurité (30 pts) : pénalité par excès de vitesse
    penalite_vitesse = min(30, round(exces / total * 100 * 1.5, 1))
    score_securite = round(30 - penalite_vitesse, 1)

    # Fiabilité (20 pts) : peu d'arrêts suspects en livraison
    penalite_arrets = min(20, round(arrets / total * 100 * 2, 1))
    score_fiabilite = round(20 - penalite_arrets, 1)

    # Activité (10 pts) : faible taux idle
    score_activite = round(max(0, 10 - taux_idle * 0.15), 1)

    score_total = round(
        max(0, score_productivite + score_securite + score_fiabilite + score_activite), 1
    )
    grade = (
        "A" if score_total >= 85 else
        "B" if score_total >= 70 else
        "C" if score_total >= 55 else
        "D" if score_total >= 40 else "E"
    )

    return {
        "livreur_id":    livreur_id,
        "periode_heures": heures,
        "score_global":  score_total,
        "grade":         grade,
        "details_score": {
            "productivite":  {"points": score_productivite, "max": 40,
                              "detail": f"{taux_livraison}% du temps en livraison"},
            "securite":      {"points": score_securite,     "max": 30,
                              "detail": f"{exces} enregistrement(s) >50 km/h"},
            "fiabilite":     {"points": score_fiabilite,    "max": 20,
                              "detail": f"{arrets} arrêt(s) suspects en livraison"},
            "activite":      {"points": score_activite,     "max": 10,
                              "detail": f"{taux_idle}% du temps en idle"},
        },
        "metriques": {
            "distance_parcourue_km": round(float(dist), 2),
            "vitesse_moyenne_kmh":   v_moy,
            "vitesse_max_kmh":       v_max,
            "regularite_vitesse":    round(float(v_std or 0), 2),
            "taux_livraison_pct":    taux_livraison,
            "taux_disponible_pct":   taux_disponible,
            "taux_idle_pct":         taux_idle,
            "exces_vitesse":         exces,
            "arrets_suspects":       arrets,
        },
        "interpretation": (
            "Performance excellente — livreur exemplaire."         if grade == "A" else
            "Bonne performance — quelques axes d'amélioration."    if grade == "B" else
            "Performance correcte — suivi recommandé."             if grade == "C" else
            "Performance insuffisante — entretien RH conseillé."   if grade == "D" else
            "Performance critique — intervention requise."
        ),
    }


# ────────────────────────────────────────────────────────────────────────────────

@app.get(
    "/analytics/zone-coverage",
    summary="Couverture territoriale — zones sur/sous-couvertes vs demande historique",
    tags=["Business Intelligence"],
)
async def zone_coverage(
    resolution: float = Query(0.02, ge=0.005, le=0.1,
                              description="Taille de cellule en degrés (~2km)"),
    heures:     int   = Query(1, ge=1, le=24),
):
    """
    Compare la distribution actuelle des livreurs (Hot Path Redis) avec la densité
    historique de passages (Cold Path DuckDB) pour identifier les déséquilibres.

    Cas d'usage : dispatch intelligent, rééquilibrage de flotte, alertes zones découvertes.
    """
    r: aioredis.Redis = app.state.redis
    glob = _parquet_glob()

    # ── Hot Path : positions actuelles → agrégation par cellule ─────────────
    raw = await r.geosearch(
        GEO_KEY,
        longitude=2.3522, latitude=48.8566,
        radius=15, unit="km",
        withcoord=True, count=500,
    )
    supply: dict[tuple, int] = {}
    for _, (lon, lat) in raw:
        cell = (
            round(round(lat / resolution) * resolution, 4),
            round(round(lon / resolution) * resolution, 4),
        )
        supply[cell] = supply.get(cell, 0) + 1

    # ── Cold Path : densité historique par cellule ───────────────────────────
    demand: dict[tuple, int] = {}
    try:
        conn = duckdb.connect()
        rows = conn.execute(
            f"""
            SELECT
                ROUND(ROUND(lat / ?, 0) * ?, 4)  AS lat_cell,
                ROUND(ROUND(lon / ?, 0) * ?, 4)  AS lon_cell,
                COUNT(*)                          AS passages
            FROM read_parquet('{glob}', hive_partitioning = true)
            WHERE ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            GROUP BY lat_cell, lon_cell
            """,
            [resolution, resolution, resolution, resolution, heures],
        ).fetchall()
        conn.close()
        for lat_c, lon_c, passages in rows:
            demand[(lat_c, lon_c)] = passages
    except Exception as exc:
        log.warning("zone-coverage cold path error: %s", exc)

    if not demand:
        raise HTTPException(status_code=503, detail="Données historiques insuffisantes.")

    # ── Calcul du ratio offre/demande par cellule ────────────────────────────
    max_demand = max(demand.values()) or 1
    max_supply  = max(supply.values()) if supply else 1

    zones = []
    for cell, passages in demand.items():
        drivers = supply.get(cell, 0)
        demand_norm  = passages / max_demand
        supply_norm  = drivers  / max(max_supply, 1)
        coverage_gap = round(demand_norm - supply_norm, 3)

        if demand_norm < 0.1:
            continue  # zone marginale, on l'ignore

        statut = (
            "sur-couverte"   if coverage_gap < -0.15 else
            "sous-couverte"  if coverage_gap >  0.20 else
            "équilibrée"
        )
        zones.append({
            "lat":          cell[0],
            "lon":          cell[1],
            "livreurs_actifs": drivers,
            "passages_historiques": passages,
            "ecart_couverture": coverage_gap,
            "statut":       statut,
        })

    zones.sort(key=lambda z: z["ecart_couverture"], reverse=True)

    sous_couvertes = [z for z in zones if z["statut"] == "sous-couverte"]
    sur_couvertes  = [z for z in zones if z["statut"] == "sur-couverte"]

    return {
        "periode_heures":   heures,
        "resolution_deg":   resolution,
        "nb_zones_analysees": len(zones),
        "resume": {
            "zones_sous_couvertes": len(sous_couvertes),
            "zones_sur_couvertes":  len(sur_couvertes),
            "zones_equilibrees":    len(zones) - len(sous_couvertes) - len(sur_couvertes),
        },
        "alertes_dispatch": {
            "zones_prioritaires": sous_couvertes[:5],
            "zones_saturees":     sur_couvertes[:5],
        },
        "toutes_zones": zones,
    }


# ────────────────────────────────────────────────────────────────────────────────

@app.get(
    "/analytics/anomalies",
    summary="Détection d'anomalies comportementales — sécurité & opérationnel",
    tags=["Business Intelligence"],
)
async def detect_anomalies(
    fenetre_minutes: int = Query(10, description="Fenêtre d'analyse en minutes", ge=5, le=60),
    seuil_vitesse:   float = Query(50.0, description="Seuil excès de vitesse (km/h)", ge=20.0, le=120.0),
    seuil_immobile:  float = Query(2.0, description="Vitesse max pour considérer un livreur immobile (km/h)", ge=0.5, le=10.0),
):
    """
    Analyse statistique du comportement de chaque livreur sur les N dernières minutes.
    Détecte 3 types d'anomalies :

    - VITESSE_EXCESSIVE : dépassement du seuil configurable (défaut 50 km/h)
    - IMMOBILISATION_SUSPECTE : livreur en 'delivering' quasi à l'arrêt > 2 min
    - DEVIATION_COMPORTEMENTALE : vitesse actuelle dévie fortement de sa propre baseline (z-score)

    Cas d'usage : sécurité routière, détection pannes/accidents, conformité assurance.
    """
    glob = _parquet_glob()
    try:
        conn = duckdb.connect()

        # Récupère les stats par livreur sur la fenêtre demandée
        rows = conn.execute(
            f"""
            WITH base AS (
                SELECT
                    livreur_id,
                    speed_kmh,
                    status,
                    ts,
                    -- Moyenne et écart-type personnel sur une fenêtre plus large (baseline)
                    AVG(speed_kmh) OVER (PARTITION BY livreur_id)    AS baseline_moy,
                    STDDEV(speed_kmh) OVER (PARTITION BY livreur_id) AS baseline_std
                FROM read_parquet('{glob}', hive_partitioning = true)
                WHERE ts >= NOW() - INTERVAL (CAST(? AS INTEGER) * 2) MINUTE
            ),
            recent AS (
                SELECT
                    livreur_id,
                    -- Stats sur la fenêtre courte (anomalie immédiate)
                    MAX(speed_kmh)                                          AS speed_max_recent,
                    AVG(speed_kmh)                                          AS speed_moy_recent,
                    COUNT(*) FILTER (WHERE speed_kmh > ?)                   AS nb_exces_vitesse,
                    COUNT(*) FILTER (WHERE speed_kmh < ? AND status = 'delivering') AS nb_immobile_delivering,
                    -- Z-score : déviation de la vitesse récente vs baseline personnelle
                    CASE
                        WHEN MAX(baseline_std) > 0
                        THEN ABS(AVG(speed_kmh) - MAX(baseline_moy)) / MAX(baseline_std)
                        ELSE 0
                    END AS z_score,
                    MAX(ts) AS derniere_position
                FROM base
                WHERE ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) MINUTE
                GROUP BY livreur_id
                HAVING COUNT(*) >= 3
            )
            SELECT * FROM recent
            WHERE nb_exces_vitesse > 0
               OR nb_immobile_delivering >= 2
               OR z_score > 2.5
            ORDER BY z_score DESC, nb_exces_vitesse DESC
            """,
            [fenetre_minutes, seuil_vitesse, seuil_immobile, fenetre_minutes],
        ).fetchall()
        total_scanned = conn.execute(
            f"""
            SELECT COUNT(DISTINCT livreur_id)
            FROM read_parquet('{glob}', hive_partitioning = true)
            WHERE ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) MINUTE
            """,
            [fenetre_minutes],
        ).fetchone()[0]
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    anomalies = []
    for row in rows:
        lid, spd_max, spd_moy, nb_exces, nb_immob, z_score, last_ts = row

        types_anomalie = []
        niveau = "info"

        if nb_exces > 0:
            types_anomalie.append({
                "type": "VITESSE_EXCESSIVE",
                "detail": f"Vitesse max {round(spd_max, 1)} km/h — {nb_exces} dépassement(s) du seuil {seuil_vitesse} km/h",
                "risque": "Sécurité routière, responsabilité assurance",
            })
            niveau = "critique" if spd_max > 70 else "warning"

        if nb_immob >= 2:
            types_anomalie.append({
                "type": "IMMOBILISATION_SUSPECTE",
                "detail": f"Livreur en statut 'delivering' quasi à l'arrêt ({nb_immob} fois en {fenetre_minutes}min)",
                "risque": "Accident possible, véhicule en panne, application gelée",
            })
            niveau = "critique"

        if z_score > 2.5:
            types_anomalie.append({
                "type": "DEVIATION_COMPORTEMENTALE",
                "detail": f"Comportement anormal — z-score {round(z_score, 2)} (>2.5 = déviation significative)",
                "risque": "Changement de comportement inhabituel, vérification recommandée",
            })
            if niveau == "info":
                niveau = "warning"

        if types_anomalie:
            anomalies.append({
                "livreur_id":        lid,
                "niveau":            niveau,
                "anomalies":         types_anomalie,
                "vitesse_max_kmh":   round(spd_max, 1),
                "vitesse_moy_kmh":   round(spd_moy, 1),
                "z_score":           round(z_score, 2),
                "derniere_position": str(last_ts),
            })

    nb_critique = sum(1 for a in anomalies if a["niveau"] == "critique")
    nb_warning  = sum(1 for a in anomalies if a["niveau"] == "warning")

    return {
        "fenetre_minutes":    fenetre_minutes,
        "seuils": {
            "vitesse_excessive_kmh":  seuil_vitesse,
            "immobilisation_kmh_max": seuil_immobile,
            "z_score_deviation":      2.5,
        },
        "resume": {
            "livreurs_scannes":    total_scanned,
            "anomalies_detectees": len(anomalies),
            "critiques":           nb_critique,
            "warnings":            nb_warning,
        },
        "anomalies": anomalies,
        "methodologie": (
            "z-score sur baseline individuelle (vitesse moyenne personnelle) + "
            "seuils absolus vitesse/immobilisation"
        ),
    }


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "version": "2.0.0"}
