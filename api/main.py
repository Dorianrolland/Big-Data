"""
FleetStream — Serving Layer API
================================
Bridge entre le Hot Path (Redis) et le Cold Path (DuckDB/Parquet).

Endpoints Hot Path (< 10ms) :
  GET /livreurs-proches   — GEOSEARCH Redis dans un rayon
  GET /livreurs/{id}      — Position et métadonnées d'un livreur
  GET /stats              — Métriques temps réel

Endpoints Cold Path (analytics) :
  GET /analytics/trajectoire/{id}  — Historique via DuckDB sur Parquet
  GET /analytics/heatmap           — Densité agrégée par cellule géo

Docs interactives : http://localhost:8000/docs
"""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, Optional

import duckdb
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
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

GEO_KEY = "fleet:geo"
HASH_PREFIX = "fleet:livreur:"
STATS_MSGS_KEY = "fleet:stats:total_messages"


# ── Lifespan (startup / shutdown) ───────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    log.info("Pool Redis initialisé → %s", REDIS_URL)
    yield
    await app.state.redis.aclose()
    log.info("Pool Redis fermé.")


# ── App ─────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="FleetStream API",
    description=(
        "Tracking temps réel d'une flotte de livreurs — Architecture Lambda.\n\n"
        "**Hot Path** : Redis Stack GEO (<10ms)\n"
        "**Cold Path** : DuckDB sur Parquet Snappy (analytics & ML)"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Modèles de réponse ──────────────────────────────────────────────────────────
class LivreurPosition(BaseModel):
    livreur_id: str
    lat: float
    lon: float
    speed_kmh: float = Field(0.0, description="Vitesse en km/h")
    heading_deg: float = Field(0.0, description="Cap en degrés (0=Nord)")
    status: str = Field("unknown", description="available | delivering | idle")
    accuracy_m: float = Field(0.0, description="Précision GPS en mètres")
    ts: str = Field("", description="Timestamp ISO 8601 UTC")
    distance_km: Optional[float] = Field(None, description="Distance au point de référence")


class NearbyResponse(BaseModel):
    count: int
    rayon_km: float
    livreurs: list[LivreurPosition]


class TrajectPoint(BaseModel):
    lat: float
    lon: float
    speed_kmh: Optional[float]
    status: Optional[str]
    ts: str


class TrajectoireResponse(BaseModel):
    livreur_id: str
    heures: int
    nb_points: int
    trajectory: list[TrajectPoint]


class HeatmapCell(BaseModel):
    lat: float
    lon: float
    nb_passages: int
    avg_speed_kmh: float


# ── HOT PATH ────────────────────────────────────────────────────────────────────

@app.get(
    "/livreurs-proches",
    response_model=NearbyResponse,
    summary="Livreurs dans un rayon (Hot Path — Redis GEOSEARCH)",
    tags=["Hot Path"],
)
async def livreurs_proches(
    lat: float = Query(..., description="Latitude du point de recherche", example=48.8566),
    lon: float = Query(..., description="Longitude du point de recherche", example=2.3522),
    rayon: float = Query(1.5, description="Rayon de recherche en km", ge=0.1, le=50.0),
    statut: Optional[Literal["available", "delivering", "idle"]] = Query(
        None, description="Filtrer par statut"
    ),
    limit: int = Query(50, description="Nombre maximum de résultats", ge=1, le=200),
):
    r: aioredis.Redis = app.state.redis

    # GEOSEARCH remplace GEORADIUS (Redis 6.2+) — retourne [(name, dist, (lon, lat)), ...]
    raw = await r.geosearch(
        GEO_KEY,
        longitude=lon,
        latitude=lat,
        radius=rayon,
        unit="km",
        withcoord=True,
        withdist=True,
        sort="ASC",
        count=limit,
    )

    if not raw:
        return NearbyResponse(count=0, rayon_km=rayon, livreurs=[])

    # Récupère tous les hashes en un seul round-trip pipeline
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
        livreurs.append(
            LivreurPosition(
                livreur_id=name,
                lat=float(h.get("lat", glat)),
                lon=float(h.get("lon", glon)),
                speed_kmh=float(h.get("speed_kmh", 0)),
                heading_deg=float(h.get("heading_deg", 0)),
                status=h.get("status", "unknown"),
                accuracy_m=float(h.get("accuracy_m", 0)),
                ts=h.get("ts", ""),
                distance_km=round(dist, 3),
            )
        )

    return NearbyResponse(count=len(livreurs), rayon_km=rayon, livreurs=livreurs)


@app.get(
    "/livreurs/{livreur_id}",
    response_model=LivreurPosition,
    summary="Position d'un livreur (Hot Path)",
    tags=["Hot Path"],
)
async def get_livreur(livreur_id: str):
    r: aioredis.Redis = app.state.redis
    h = await r.hgetall(f"{HASH_PREFIX}{livreur_id}")
    if not h:
        raise HTTPException(
            status_code=404,
            detail=f"Livreur '{livreur_id}' introuvable (TTL expiré ou inexistant).",
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
    summary="Métriques temps réel (Hot Path)",
    tags=["Hot Path"],
)
async def stats():
    r: aioredis.Redis = app.state.redis

    # zcard = nombre de livreurs avec une position GEO active
    total_geo = await r.zcard(GEO_KEY)
    total_msgs = int(await r.get(STATS_MSGS_KEY) or 0)

    # Comptage des statuts via pipeline (1 seul round-trip pour tous les hashes)
    keys = await r.keys(f"{HASH_PREFIX}*")
    status_counts = {"available": 0, "delivering": 0, "idle": 0}
    if keys:
        pipe = r.pipeline(transaction=False)
        for k in keys:
            pipe.hget(k, "status")
        statuses = await pipe.execute()
        for s in statuses:
            if s in status_counts:
                status_counts[s] += 1

    parquet_files = list(DATA_PATH.glob("**/*.parquet")) if DATA_PATH.exists() else []

    return {
        "hot_path": {
            "livreurs_actifs": total_geo,
            "messages_traites": total_msgs,
            "statuts": status_counts,
            "backend": "Redis Stack (GEOSEARCH)",
            "ttl_secondes": 30,
        },
        "cold_path": {
            "fichiers_parquet": len(parquet_files),
            "taille_totale_mb": round(
                sum(f.stat().st_size for f in parquet_files) / 1_048_576, 2
            ),
            "backend": "Apache Parquet (Snappy) + DuckDB",
        },
    }


# ── COLD PATH ───────────────────────────────────────────────────────────────────

def _parquet_glob() -> str:
    """Retourne le glob DuckDB pour tous les fichiers Parquet."""
    return str(DATA_PATH / "**" / "*.parquet")


@app.get(
    "/analytics/trajectoire/{livreur_id}",
    response_model=TrajectoireResponse,
    summary="Trajectoire historique d'un livreur (Cold Path — DuckDB)",
    tags=["Cold Path"],
)
async def trajectoire(
    livreur_id: str,
    heures: int = Query(1, description="Fenêtre temporelle en heures", ge=1, le=24),
):
    glob = _parquet_glob()
    try:
        conn = duckdb.connect()
        rows = conn.execute(
            """
            SELECT lat, lon, speed_kmh, status, ts
            FROM read_parquet(?, hive_partitioning := true)
            WHERE livreur_id = ?
              AND ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            ORDER BY ts
            """,
            [glob, livreur_id, heures],
        ).fetchall()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Aucune donnée historique pour '{livreur_id}' sur les {heures} dernières heures.",
        )

    return TrajectoireResponse(
        livreur_id=livreur_id,
        heures=heures,
        nb_points=len(rows),
        trajectory=[
            TrajectPoint(
                lat=r[0], lon=r[1],
                speed_kmh=r[2], status=r[3],
                ts=str(r[4]),
            )
            for r in rows
        ],
    )


@app.get(
    "/analytics/heatmap",
    summary="Carte de densité (Cold Path — agrégation DuckDB)",
    tags=["Cold Path"],
)
async def heatmap(
    heures: int = Query(1, description="Fenêtre temporelle", ge=1, le=24),
    resolution: float = Query(
        0.01, description="Résolution de la grille en degrés (~1km)", ge=0.001, le=0.1
    ),
):
    """
    Retourne une grille de densité de passages pour visualisation heatmap.
    Utile pour détecter les zones de surge pricing (forte demande).
    """
    glob = _parquet_glob()
    try:
        conn = duckdb.connect()
        rows = conn.execute(
            """
            SELECT
                ROUND(lat / ?, 0) * ?  AS lat_cell,
                ROUND(lon / ?, 0) * ?  AS lon_cell,
                COUNT(*)               AS nb_passages,
                AVG(speed_kmh)         AS avg_speed_kmh
            FROM read_parquet(?, hive_partitioning := true)
            WHERE ts >= NOW() - INTERVAL (CAST(? AS INTEGER)) HOUR
            GROUP BY lat_cell, lon_cell
            ORDER BY nb_passages DESC
            LIMIT 500
            """,
            [resolution, resolution, resolution, resolution, glob, heures],
        ).fetchall()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    return {
        "heures": heures,
        "resolution_deg": resolution,
        "nb_cellules": len(rows),
        "heatmap": [
            HeatmapCell(
                lat=r[0], lon=r[1],
                nb_passages=r[2],
                avg_speed_kmh=round(r[3] or 0.0, 1),
            )
            for r in rows
        ],
    }


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "service": "fleetstream-api"}
