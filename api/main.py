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
import json
import logging
import math
import os
import statistics
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Literal, Optional

import duckdb
import redis.asyncio as aioredis
from redis.exceptions import RedisError
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field

from copilot_router import copilot_router, start_copilot_background_tasks, stop_copilot_background_tasks

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("api")

# ── Config ──────────────────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
DATA_PATH = Path(os.getenv("DATA_PATH", "/data/parquet"))
REDIS_CONNECT_TIMEOUT_SECONDS = float(os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "1.5"))
REDIS_OP_TIMEOUT_SECONDS = float(os.getenv("REDIS_OP_TIMEOUT_SECONDS", "6.0"))
DUCKDB_QUERY_TIMEOUT_SECONDS = float(os.getenv("DUCKDB_QUERY_TIMEOUT_SECONDS", "12.0"))

GEO_KEY          = "fleet:geo"
HASH_PREFIX      = "fleet:livreur:"
STATS_MSGS_KEY   = "fleet:stats:total_messages"
STATS_DLQ_KEY    = "fleet:stats:dlq_count"
DLQ_KEY          = "fleet:dlq"
DLQ_PATH         = Path(os.getenv("DLQ_PATH", "/data/dlq"))
TLC_REPLAY_STATUS_KEY = "copilot:replay:tlc:status"
DELIVERING_STATUS_ALIASES = {
    "delivering",
    "pickup_arrived",
    "pickup_assigned",
    "pickup_en_route",
    "on_trip",
    "busy",
}
AVAILABLE_STATUS_ALIASES = {"available", "ready", "waiting_order"}
IDLE_STATUS_ALIASES = {"idle", "offline", "paused"}

# ── Prometheus custom metrics ───────────────────────────────────────────────────
gauge_active_livreurs = Gauge(
    "fleet_active_livreurs",
    "Nombre de livreurs actifs dans Redis (TTL 30s)",
)
gauge_messages_processed = Gauge(
    "fleet_messages_processed",
    "Total de messages GPS traités par le hot consumer",
)
gauge_feedback_zones = Gauge(
    "fleet_feedback_zones",
    "Zones avec baseline vitesse injectées dans Redis par le Feedback Loop",
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
    app.state.redis = aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_OP_TIMEOUT_SECONDS,
        health_check_interval=15,
        retry_on_timeout=True,
    )
    try:
        await app.state.redis.ping()
    except Exception as exc:
        log.warning("Redis indisponible au démarrage (%s): %s", REDIS_URL, exc)

    # Connexion DuckDB persistante — source de vérité pour tous les endpoints
    # Cold Path. Chaque requête obtient son propre cursor (thread-safe), ce qui
    # évite (1) la latence de setup d'une connexion DuckDB par requête
    # (~20-50ms), (2) le re-chargement des extensions (parquet, httpfs) et
    # (3) le re-scan du cache de métadonnées Parquet. L'extension httpfs est
    # pré-chargée pour préparer la future migration vers MinIO/S3.
    app.state.duckdb = duckdb.connect(":memory:")
    try:
        app.state.duckdb.execute("SET enable_object_cache = true;")
        app.state.duckdb.execute("INSTALL parquet; LOAD parquet;")
        app.state.duckdb.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception as exc:
        log.warning("DuckDB extension preload partiel : %s", exc)
    log.info("Connexion DuckDB persistante initialisée (:memory:)")

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

    async def _feedback_loop() -> None:
        """
        Boucle de rétroaction : Batch Layer (DuckDB/Parquet) → Speed Layer (Redis)
        ─────────────────────────────────────────────────────────────────────────────
        Toutes les 5 minutes, calcule la baseline de vitesse par zone géographique
        (cellules 0.02°) depuis l'historique Parquet et l'injecte dans Redis.

        Clé Redis : fleet:context:zone:{lat}_{lon}
        Champs    : avg_speed, std_speed, nb_points, updated_at

        Usage : les endpoints de scoring et d'anomalies lisent ces baselines pour
        contextualiser les seuils — un livreur lent dans une zone historiquement
        lente n'est pas pénalisé.
        """
        await asyncio.sleep(30)  # Attendre que les premières données Parquet arrivent
        while True:
            try:
                ref_ts = await _latest_parquet_ts()
                if ref_ts is None:
                    log.info("Feedback loop: en attente de ts de reference Parquet")
                    await asyncio.sleep(300)
                    continue
                window_start = ref_ts - timedelta(hours=2)
                scan_sql = _parquet_scan_sql(window_start, ref_ts, max_hours=24)
                rows = await _dq(
                    f"""
                    SELECT
                        ROUND(ROUND(lat / 0.02, 0) * 0.02, 3) AS lat_cell,
                        ROUND(ROUND(lon / 0.02, 0) * 0.02, 3) AS lon_cell,
                        ROUND(AVG(speed_kmh), 1)               AS avg_speed,
                        ROUND(STDDEV(speed_kmh), 1)            AS std_speed,
                        COUNT(*)                               AS nb_points
                    FROM {scan_sql}
                    WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
                    GROUP BY lat_cell, lon_cell
                    HAVING COUNT(*) >= 20
                    """,
                    [_dt_to_iso(window_start), _dt_to_iso(ref_ts)],
                )

                r: aioredis.Redis = app.state.redis
                pipe = r.pipeline(transaction=False)
                for lat_c, lon_c, avg_s, std_s, nb in rows:
                    key = f"fleet:context:zone:{lat_c}_{lon_c}"
                    pipe.hset(key, mapping={
                        "avg_speed": avg_s,
                        "std_speed": std_s or 0,
                        "nb_points": nb,
                        "updated_at": time.time(),
                    })
                    pipe.expire(key, 7200)  # TTL 2h
                await pipe.execute()
                gauge_feedback_zones.set(len(rows))
                log.info(
                    "Feedback loop: %d speed zones updated in Redis (ref=%s)",
                    len(rows),
                    _dt_to_iso(ref_ts),
                )
            except Exception as exc:
                log.warning("Feedback loop error: %s", exc)
            await asyncio.sleep(300)  # 5 minutes

    task = asyncio.create_task(_update_fleet_metrics())
    task2 = asyncio.create_task(_feedback_loop())
    copilot_tasks = start_copilot_background_tasks(app)
    log.info("FleetStream API v2 démarrée — Redis: %s | Data: %s", REDIS_URL, DATA_PATH)
    yield
    task.cancel()
    task2.cancel()
    stop_copilot_background_tasks(copilot_tasks)
    try:
        app.state.duckdb.close()
    except Exception:
        pass
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
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.exception_handler(RedisError)
async def handle_redis_error(request: Request, exc: RedisError):
    log.warning("RedisError non gérée sur %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=503,
        content={
            "detail": _redis_unavailable_message(),
            "path": request.url.path,
            "backend": "redis",
        },
    )

# Sert le dashboard HTML statique depuis le dossier /dashboard
_DASHBOARD_DIR = Path(__file__).parent / "static"
if _DASHBOARD_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_DASHBOARD_DIR)), name="static")

app.include_router(copilot_router)

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
    battery_pct: float = 0.0
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
    time_reference: dict[str, str] | None = None


# ── Utilitaires ─────────────────────────────────────────────────────────────────
def _parquet_glob() -> str:
    return str(DATA_PATH / "**" / "*.parquet")


async def _dq(
    sql: str,
    params: list | None = None,
    *,
    timeout_sec: float | None = None,
) -> list[tuple]:
    """
    Exécute une requête SQL sur la connexion DuckDB partagée dans un thread.

    Chaque appel obtient son propre cursor via app.state.duckdb.cursor(), ce qui
    isole l'état d'exécution (prepared statements, résultats) de la connexion
    parente tout en partageant le catalogue et le cache. C'est le pattern
    recommandé par DuckDB pour les serveurs HTTP multi-requêtes.

    L'exécution est déportée via asyncio.to_thread pour ne pas bloquer la boucle
    d'événements uvicorn pendant les scans Parquet (qui peuvent durer plusieurs
    dizaines de millisecondes et saturaient auparavant toute l'API).
    """
    def _run() -> list[tuple]:
        cur = app.state.duckdb.cursor()
        return cur.execute(sql, params or []).fetchall()
    effective_timeout = (
        DUCKDB_QUERY_TIMEOUT_SECONDS if timeout_sec is None else timeout_sec
    )
    if effective_timeout <= 0:
        return await asyncio.to_thread(_run)
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_run),
            timeout=effective_timeout,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                f"DuckDB timeout ({effective_timeout:.1f}s). "
                "Réduis la fenêtre d'analyse ou relance quand les données sont moins chargées."
            ),
        ) from exc


def _percentile(data: list[float], pct: int) -> float:
    if not data:
        return 0.0
    data_sorted = sorted(data)
    idx = max(0, int(len(data_sorted) * pct / 100) - 1)
    return round(data_sorted[idx], 3)


def _coerce_datetime_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return _coerce_datetime_utc(parsed)


def _dt_to_iso(value: datetime) -> str:
    return _coerce_datetime_utc(value).isoformat()


def _sql_quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _hour_partition_globs(
    window_start: datetime,
    window_end: datetime,
    *,
    max_hours: int = 72,
) -> list[str]:
    """
    Retourne les globs Parquet des partitions horaires couvertes par la fenêtre.

    Exemple:
      /data/parquet/year=2024/month=01/day=01/hour=06/*.parquet
    """
    start_utc = _coerce_datetime_utc(window_start)
    end_utc = _coerce_datetime_utc(window_end)
    if end_utc < start_utc:
        start_utc, end_utc = end_utc, start_utc

    start_hour = start_utc.replace(minute=0, second=0, microsecond=0)
    end_hour = end_utc.replace(minute=0, second=0, microsecond=0)
    span_hours = int((end_hour - start_hour).total_seconds() // 3600) + 1
    if span_hours > max_hours:
        return []

    globs: list[str] = []
    seen: set[str] = set()
    cur = start_hour
    while cur <= end_hour:
        partition_dir = (
            DATA_PATH
            / f"year={cur.year:04d}"
            / f"month={cur:%m}"
            / f"day={cur:%d}"
            / f"hour={cur:%H}"
        )
        if partition_dir.exists():
            glob = str(partition_dir / "*.parquet")
            if glob not in seen:
                globs.append(glob)
                seen.add(glob)
        cur += timedelta(hours=1)
    return globs


def _recent_hour_partition_globs(limit: int = 8) -> list[str]:
    if not DATA_PATH.exists():
        return []

    hour_dirs = sorted(
        (
            p for p in DATA_PATH.glob("year=*/month=*/day=*/hour=*")
            if p.is_dir()
        ),
        key=lambda p: p.as_posix(),
        reverse=True,
    )
    out: list[str] = []
    for part in hour_dirs:
        if next(part.glob("*.parquet"), None) is None:
            continue
        out.append(str(part / "*.parquet"))
        if len(out) >= limit:
            break
    return out


def _parquet_scan_sql_from_globs(globs: list[str] | None = None) -> str:
    if globs:
        quoted = ", ".join(_sql_quote_literal(g) for g in globs)
        return f"read_parquet([{quoted}], hive_partitioning = true)"
    return f"read_parquet('{_parquet_glob()}', hive_partitioning = true)"


def _parquet_scan_sql(
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    *,
    max_hours: int = 72,
) -> str:
    if window_start is not None and window_end is not None:
        globs = _hour_partition_globs(window_start, window_end, max_hours=max_hours)
        if globs:
            return _parquet_scan_sql_from_globs(globs)
    return _parquet_scan_sql_from_globs()


def _canonical_status(raw_status: str | None) -> str:
    status = (raw_status or "").strip().lower()
    if status in DELIVERING_STATUS_ALIASES:
        return "delivering"
    if status in AVAILABLE_STATUS_ALIASES:
        return "available"
    if status in IDLE_STATUS_ALIASES:
        return "idle"
    return status or "unknown"


def _redis_unavailable_message() -> str:
    return (
        "Redis est temporairement indisponible. Vérifie `redis`/`redpanda` avec "
        "`docker compose ps`, puis relance la requête."
    )


def _redis_http_exception(exc: Exception) -> HTTPException:
    log.warning("Redis indisponible: %s", exc)
    return HTTPException(status_code=503, detail=_redis_unavailable_message())


async def _run_redis(
    awaitable: Awaitable[Any],
    *,
    op_name: str,
    default: Any = None,
    strict: bool = True,
) -> Any:
    try:
        return await awaitable
    except (RedisError, OSError) as exc:
        log.warning("Redis operation failed (%s): %s", op_name, exc)
        if strict:
            raise _redis_http_exception(exc) from exc
        return default


async def _latest_parquet_ts() -> datetime | None:
    recent_globs = _recent_hour_partition_globs(limit=8)
    fast_scan_sql = _parquet_scan_sql_from_globs(recent_globs)
    try:
        rows = await _dq(
            f"""
            SELECT MAX(ts) AS max_ts
            FROM {fast_scan_sql}
            """
        )
    except Exception:
        rows = []

    raw = rows[0][0] if rows else None

    # Fast scan only covers the ~8 most recent hour partitions. If it finds
    # nothing AND we actually narrowed the scan (i.e. recent_globs wasn't
    # empty), fall back to a full scan across every partition.
    if raw is None and recent_globs:
        try:
            rows = await _dq(
                f"""
                SELECT MAX(ts) AS max_ts
                FROM {_parquet_scan_sql_from_globs()}
                """
            )
        except Exception:
            return None
        raw = rows[0][0] if rows else None

    if isinstance(raw, datetime):
        return _coerce_datetime_utc(raw)
    if isinstance(raw, str):
        return _parse_iso_timestamp(raw)
    return None


async def _resolve_reference_ts(reference_ts: str | None) -> tuple[datetime, dict[str, str]]:
    if reference_ts:
        explicit = _parse_iso_timestamp(reference_ts)
        if explicit is None:
            raise HTTPException(
                status_code=400,
                detail=f"reference_ts invalide: '{reference_ts}' (format ISO-8601 attendu)",
            )
        return explicit, {
            "mode": "explicit",
            "requested_reference_ts": reference_ts,
            "source": "query_param",
        }

    redis_client: aioredis.Redis = app.state.redis
    replay_status = await _run_redis(
        redis_client.hgetall(TLC_REPLAY_STATUS_KEY),
        op_name="hgetall tlc replay status",
        default={},
        strict=False,
    ) or {}

    replay_virtual_ts = _parse_iso_timestamp(replay_status.get("virtual_time"))
    if replay_virtual_ts is not None:
        payload = {"mode": "tlc_virtual_time", "source": "redis"}
        state = replay_status.get("state")
        if state:
            payload["replay_state"] = state
        return replay_virtual_ts, payload

    latest = await _latest_parquet_ts()
    if latest is not None:
        return latest, {"mode": "parquet_max_ts", "source": "duckdb"}

    now_utc = datetime.now(timezone.utc)
    return now_utc, {"mode": "system_now", "source": "system_clock"}


def _build_time_reference(
    base: dict[str, str],
    reference_ts: datetime,
    window_start: datetime | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    payload = dict(base)
    payload["reference_ts"] = _dt_to_iso(reference_ts)
    payload["window_end"] = _dt_to_iso(reference_ts)
    if window_start is not None:
        payload["window_start"] = _dt_to_iso(window_start)
    if extra:
        payload.update(extra)
    return payload


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
    lat:    float = Query(..., description="Latitude",  example=40.7580),
    lon:    float = Query(..., description="Longitude", example=-73.9855),
    rayon:  float = Query(1.5, description="Rayon en km", ge=0.1, le=50.0),
    statut: Optional[Literal["available", "delivering", "idle"]] = Query(None),
    limit:  int   = Query(50, ge=1, le=1000),
):
    r: aioredis.Redis = app.state.redis

    raw = await _run_redis(
        r.geosearch(
        GEO_KEY,
        longitude=lon, latitude=lat,
        radius=rayon, unit="km",
        withcoord=True, withdist=True,
        sort="ASC", count=limit,
        ),
        op_name="geosearch livreurs-proches",
    )
    if not raw:
        return NearbyResponse(count=0, rayon_km=rayon, livreurs=[])

    pipe = r.pipeline(transaction=False)
    for item in raw:
        pipe.hgetall(f"{HASH_PREFIX}{item[0]}")
    hashes = await _run_redis(
        pipe.execute(),
        op_name="pipeline hgetall livreurs-proches",
    )

    livreurs: list[LivreurPosition] = []
    for item, h in zip(raw, hashes):
        name, dist, (glon, glat) = item
        if not h:
            continue
        normalized_status = _canonical_status(h.get("status"))
        if statut and normalized_status != statut:
            continue
        livreurs.append(LivreurPosition(
            livreur_id=name,
            lat=float(h.get("lat", glat)),
            lon=float(h.get("lon", glon)),
            speed_kmh=float(h.get("speed_kmh", 0)),
            heading_deg=float(h.get("heading_deg", 0)),
            status=normalized_status,
            accuracy_m=float(h.get("accuracy_m", 0)),
            battery_pct=float(h.get("battery_pct", 0)),
            ts=h.get("ts", ""),
            distance_km=round(dist, 3),
        ))

    return NearbyResponse(count=len(livreurs), rayon_km=rayon, livreurs=livreurs)


@app.get(
    "/livreurs/focus/status",
    summary="Métadonnées du scénario single-driver (léger, pour header UI)",
    tags=["Hot Path"],
)
async def livreur_focus_status():
    """Léger status du runner single-driver (polled rarement par l'UI).

    Retourne le contenu brut de la hash Redis
    `copilot:replay:tlc:single:status` plus quelques dérivés utiles pour
    un header (age, is_healthy, providers). Ne touche pas au Cold Path.
    """
    r: aioredis.Redis = app.state.redis
    raw = await _run_redis(
        r.hgetall("copilot:replay:tlc:single:status"),
        op_name="hgetall focus status",
        default={},
        strict=False,
    ) or {}
    now_utc = datetime.now(timezone.utc)
    updated_at_iso = raw.get("updated_at") or ""
    age_seconds: float | None = None
    parsed = _parse_iso_timestamp(updated_at_iso)
    if parsed is not None:
        age_seconds = max(0.0, (now_utc - parsed).total_seconds())

    def _to_int(key: str) -> int:
        try:
            return int(raw.get(key, 0) or 0)
        except (TypeError, ValueError):
            return 0

    providers = [p for p in (raw.get("routing_providers") or "").split(",") if p]
    is_healthy = (age_seconds is not None and age_seconds < 60.0) and bool(providers)

    return {
        "driver_id": raw.get("driver_id") or "",
        "state": raw.get("state") or "unknown",
        "virtual_time": raw.get("virtual_time") or "",
        "updated_at": updated_at_iso,
        "age_seconds": age_seconds,
        "is_healthy": is_healthy,
        "stale_reason": raw.get("stale_reason") or "",
        "routing_providers": providers,
        "positions": _to_int("positions"),
        "trips": _to_int("trips"),
        "repositions": _to_int("repositions"),
        "lunch_breaks": _to_int("lunch_breaks"),
        "routing_errors": _to_int("routing_errors"),
        "generated_at": _dt_to_iso(now_utc),
    }


@app.get(
    "/livreurs/focus",
    summary="Vue focus sur un chauffeur unique (position + stale + trail court)",
    tags=["Hot Path"],
)
async def livreur_focus(
    driver_id: str = Query(..., description="Identifiant du chauffeur à suivre"),
    trail_points: int = Query(60, ge=0, le=500, description="Longueur du trail court (Cold Path)"),
    stale_after_s: float = Query(15.0, ge=1.0, le=600.0),
):
    """Endpoint dédié au mode focus de la carte live.

    Retourne la position Hot Path du chauffeur, un indicateur `stale` si
    son dernier point est plus vieux que `stale_after_s`, et un trail
    court reconstruit depuis le Cold Path Parquet pour animer le sillage
    derrière le marqueur sans le faire disparaître au moindre retard.
    """
    r: aioredis.Redis = app.state.redis

    hot = await _run_redis(
        r.hgetall(f"{HASH_PREFIX}{driver_id}"),
        op_name=f"hgetall focus {driver_id}",
        default={},
        strict=False,
    )

    now_utc = datetime.now(timezone.utc)
    position: dict | None = None
    stale = True
    age_seconds: float | None = None
    if hot:
        try:
            lat = float(hot.get("lat", 0.0))
            lon = float(hot.get("lon", 0.0))
        except (TypeError, ValueError):
            lat = 0.0
            lon = 0.0
        ts_iso = hot.get("ts") or ""
        ts_parsed = _parse_iso_timestamp(ts_iso)
        if ts_parsed is not None:
            age_seconds = max(0.0, (now_utc - ts_parsed).total_seconds())
            stale = age_seconds > stale_after_s
        else:
            stale = True
        position = {
            "driver_id": driver_id,
            "lat": lat,
            "lon": lon,
            "speed_kmh": float(hot.get("speed_kmh", 0.0) or 0.0),
            "heading_deg": float(hot.get("heading_deg", 0.0) or 0.0),
            "status": _canonical_status(hot.get("status")),
            "accuracy_m": float(hot.get("accuracy_m", 0.0) or 0.0),
            "battery_pct": float(hot.get("battery_pct", 0.0) or 0.0),
            "ts": ts_iso,
        }

    trail: list[dict] = []
    if trail_points > 0:
        window_end = now_utc
        window_start = window_end - timedelta(hours=2)
        scan_sql = _parquet_scan_sql(window_start, window_end, max_hours=4)
        try:
            rows = await _dq(
                f"""
                SELECT ts, lat, lon, speed_kmh, status
                FROM {scan_sql}
                WHERE livreur_id = ?
                  AND ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
                ORDER BY ts DESC
                LIMIT ?
                """,
                [driver_id, _dt_to_iso(window_start), _dt_to_iso(window_end), int(trail_points)],
            )
        except Exception as exc:
            log.warning("focus trail failed for %s: %s", driver_id, exc)
            rows = []
        for ts_val, lat_v, lon_v, speed_v, status_v in reversed(rows):
            if lat_v is None or lon_v is None:
                continue
            trail.append(
                {
                    "ts": ts_val.isoformat() if isinstance(ts_val, datetime) else str(ts_val),
                    "lat": float(lat_v),
                    "lon": float(lon_v),
                    "speed_kmh": float(speed_v or 0.0),
                    "status": _canonical_status(status_v),
                }
            )

    replay_status = await _run_redis(
        r.hgetall("copilot:replay:tlc:single:status"),
        op_name="hgetall single status",
        default={},
        strict=False,
    ) or {}

    return {
        "driver_id": driver_id,
        "position": position,
        "stale": stale,
        "age_seconds": age_seconds,
        "stale_after_s": stale_after_s,
        "trail": trail,
        "replay_status": replay_status,
        "generated_at": _dt_to_iso(now_utc),
    }


@app.get(
    "/livreurs/{livreur_id}",
    response_model=LivreurPosition,
    summary="Position temps réel d'un livreur",
    tags=["Hot Path"],
)
async def get_livreur(livreur_id: str):
    r: aioredis.Redis = app.state.redis
    h = await _run_redis(
        r.hgetall(f"{HASH_PREFIX}{livreur_id}"),
        op_name=f"hgetall livreur {livreur_id}",
    )
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
        status=_canonical_status(h.get("status")),
        accuracy_m=float(h.get("accuracy_m", 0)),
        battery_pct=float(h.get("battery_pct", 0)),
        ts=h.get("ts", ""),
    )


@app.get(
    "/stats",
    summary="Métriques temps réel (Hot + Cold Path)",
    tags=["Hot Path"],
)
async def stats():
    r: aioredis.Redis = app.state.redis

    redis_available = bool(
        await _run_redis(
            r.ping(),
            op_name="ping stats",
            default=False,
            strict=False,
        )
    )
    total_geo = 0
    total_msgs = 0
    if redis_available:
        total_geo = int(
            await _run_redis(
                r.zcard(GEO_KEY),
                op_name="zcard stats",
                default=0,
                strict=False,
            ) or 0
        )
        total_msgs = int(
            await _run_redis(
                r.get(STATS_MSGS_KEY),
                op_name="get stats messages",
                default=0,
                strict=False,
            ) or 0
        )

    # Source de vérité : sorted set GEO (purgé par _purge_ghosts_loop côté hot path).
    # On évite KEYS HASH_PREFIX:* qui est O(N) bloquant sur Redis et inclut les hashes
    # éphémères pas encore expirés alors que le livreur a déjà disparu de la flotte.
    livreur_ids = (
        await _run_redis(
            r.zrange(GEO_KEY, 0, -1),
            op_name="zrange stats",
            default=[],
            strict=False,
        ) or []
    )
    status_counts: dict[str, int] = {"available": 0, "delivering": 0, "idle": 0}
    if livreur_ids:
        pipe = r.pipeline(transaction=False)
        for lid in livreur_ids:
            pipe.hget(f"{HASH_PREFIX}{lid}", "status")
        statuses = await _run_redis(
            pipe.execute(),
            op_name="pipeline statuses stats",
            default=[],
            strict=False,
        ) or []
        for s in statuses:
            canonical = _canonical_status(s)
            if canonical in status_counts:
                status_counts[canonical] += 1

    parquet_files = list(DATA_PATH.glob("**/*.parquet")) if DATA_PATH.exists() else []

    return {
        "hot_path": {
            "livreurs_actifs":    total_geo,
            "messages_traites":   total_msgs,
            "statuts":            status_counts,
            "backend":            "Redis Stack (GEOSEARCH)",
            "ttl_secondes":       30,
            "redis_available":    redis_available,
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
#  DEAD LETTER QUEUE — observabilité des messages rejetés à la validation
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/dlq/stats",
    summary="Statistiques de la Dead Letter Queue (Hot + Cold Path)",
    tags=["Hot Path"],
)
async def dlq_stats():
    """
    Synthèse des messages GPS rejetés par la validation Pydantic dans les
    deux consumers. Chaque couche route ses rejets vers un sink différent
    pour préserver l'indépendance Speed/Batch :

    - Hot Path : Redis LIST `fleet:dlq` (LPUSH + LTRIM 1000) — fenêtre
      glissante des derniers rejets, latency-friendly.
    - Cold Path : fichiers JSONL append-only `/data/dlq/cold-dlq-<jour>.jsonl`
      — audit hors-ligne, requêtable par DuckDB read_json_auto().

    Cas d'usage : surveiller la qualité du flux producer, détecter une
    régression de schéma, alimenter une alerte si le taux de rejet
    dépasse un seuil métier.
    """
    r: aioredis.Redis = app.state.redis

    # ── Hot Path : compteur Redis + taille de la liste DLQ ───────────────────
    hot_count = int(
        await _run_redis(
            r.get(STATS_DLQ_KEY),
            op_name="get dlq stats counter",
            default=0,
            strict=False,
        ) or 0
    )
    hot_window_size = int(
        await _run_redis(
            r.llen(DLQ_KEY),
            op_name="llen dlq",
            default=0,
            strict=False,
        ) or 0
    )

    # ── Cold Path : agrégation des fichiers DLQ JSONL ────────────────────────
    cold_count = 0
    cold_files: list[dict] = []
    if DLQ_PATH.exists():
        for f in sorted(DLQ_PATH.glob("cold-dlq-*.jsonl")):
            try:
                # Compte les lignes sans charger le fichier en mémoire
                with f.open("rb") as fh:
                    nb = sum(1 for _ in fh)
                cold_count += nb
                cold_files.append({
                    "fichier": f.name,
                    "rejets":  nb,
                    "taille_kb": f.stat().st_size // 1024,
                })
            except OSError:
                pass

    return {
        "hot_path": {
            "source":            "Redis LIST fleet:dlq",
            "rejets_cumules":    hot_count,
            "fenetre_glissante": hot_window_size,
            "cap_max":           1000,
        },
        "cold_path": {
            "source":           "JSONL /data/dlq/cold-dlq-*.jsonl",
            "rejets_cumules":   cold_count,
            "fichiers":         cold_files,
        },
        "total_rejets": hot_count + cold_count,
    }


@app.get(
    "/dlq/peek",
    summary="Inspecter les N derniers messages rejetés (Hot Path)",
    tags=["Hot Path"],
)
async def dlq_peek(
    limit: int = Query(20, description="Nombre de rejets à retourner", ge=1, le=200),
):
    """
    Retourne les `limit` derniers messages rejetés depuis la DLQ Redis.
    Permet de diagnostiquer rapidement un producer bugué : on voit le
    payload brut + la raison Pydantic (champ manquant, valeur hors borne...).
    """
    r: aioredis.Redis = app.state.redis
    raw_items = await _run_redis(
        r.lrange(DLQ_KEY, 0, limit - 1),
        op_name="lrange dlq peek",
        default=[],
        strict=False,
    ) or []
    items: list[dict] = []
    for s in raw_items:
        try:
            items.append(json.loads(s))
        except (ValueError, TypeError):
            items.append({"raw": s, "reason": "DLQ entry not JSON-decodable"})
    return {
        "source":   "Redis LIST fleet:dlq (LPUSH + LTRIM 1000)",
        "limit":    limit,
        "returned": len(items),
        "items":    items,
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
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Boucle Lambda : lit le Data Lake Parquet via DuckDB pour reconstruire
    la trajectoire complète d'un livreur avec statistiques de mouvement.

    Inclut : distance parcourue (Haversine), vitesse moyenne/max, statut dominant.
    """
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    window_start = ref_ts - timedelta(hours=heures)
    window_start_iso = _dt_to_iso(window_start)
    window_end_iso = _dt_to_iso(ref_ts)
    scan_sql = _parquet_scan_sql(window_start, ref_ts)
    try:
        rows = await _dq(
            f"""
            SELECT lat, lon, speed_kmh, heading_deg, status, ts
            FROM   {scan_sql}
            WHERE  livreur_id = ?
              AND  ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            ORDER BY ts
            """,
            [livreur_id, window_start_iso, window_end_iso],
        )

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Aucune donnée historique pour '{livreur_id}' sur {heures}h.",
            )

        # Distance totale (Haversine vectorisé côté DuckDB)
        dist_rows = await _dq(
            f"""
            WITH ordered AS (
                SELECT lat, lon,
                       LAG(lat) OVER (ORDER BY ts) AS prev_lat,
                       LAG(lon) OVER (ORDER BY ts) AS prev_lon
                FROM   {scan_sql}
                WHERE  livreur_id = ?
                  AND  ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
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
            [livreur_id, window_start_iso, window_end_iso],
        )
        dist_row = dist_rows[0] if dist_rows else (0.0,)

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
        time_reference=_build_time_reference(ref_meta, ref_ts, window_start),
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
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    window_start = ref_ts - timedelta(hours=heures)
    scan_sql = _parquet_scan_sql(window_start, ref_ts)
    try:
        rows = await _dq(
            f"""
            SELECT
                ROUND(lat / ?, 0) * ?  AS lat_cell,
                ROUND(lon / ?, 0) * ?  AS lon_cell,
                COUNT(*)               AS nb_passages,
                AVG(speed_kmh)         AS avg_speed
            FROM   {scan_sql}
            WHERE  ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            GROUP  BY lat_cell, lon_cell
            ORDER  BY nb_passages DESC
            LIMIT  500
            """,
            [resolution, resolution, resolution, resolution, _dt_to_iso(window_start), _dt_to_iso(ref_ts)],
        )
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
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
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
        await _run_redis(
            r.geosearch(
                GEO_KEY,
                longitude=-73.9855,
                latitude=40.7580,
                radius=20,
                unit="km",
                count=10,
            ),
            op_name="geosearch performance warmup",
        )

    latencies_ms: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await _run_redis(
            r.geosearch(
                GEO_KEY,
                longitude=-73.9855, latitude=40.7580,
                radius=20, unit="km",
                withcoord=False, withdist=False,
                count=100,
            ),
            op_name="geosearch performance sample",
        )
        latencies_ms.append((time.perf_counter() - t0) * 1000)

    # Redis INFO
    info = await _run_redis(
        r.info("all"),
        op_name="info performance",
    )

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
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Vue métier complète de la flotte : taux d'utilisation, alertes opérationnelles,
    productivité globale. Combine Hot Path (Redis) et Cold Path (DuckDB).

    Cas d'usage : dashboard opérateur, alertes dispatch, reporting journalier.
    """
    r: aioredis.Redis = app.state.redis
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    window_start = ref_ts - timedelta(hours=heures)
    scan_sql = _parquet_scan_sql(window_start, ref_ts)

    # ── Hot Path : état instantané ───────────────────────────────────────────
    redis_available = bool(
        await _run_redis(
            r.ping(),
            op_name="ping fleet-insights",
            default=False,
            strict=False,
        )
    )
    total_actifs = 0
    # Liste source : sorted set GEO purgé en continu par le hot consumer.
    # ZRANGE est O(log N + M) et garanti non-bloquant — KEYS HASH_PREFIX:*
    # serait O(N) sur tout le keyspace Redis, à proscrire en prod.
    livreur_ids: list[str] = []
    if redis_available:
        total_actifs = int(
            await _run_redis(
                r.zcard(GEO_KEY),
                op_name="zcard fleet-insights",
                default=0,
                strict=False,
            ) or 0
        )
        livreur_ids = (
            await _run_redis(
                r.zrange(GEO_KEY, 0, -1),
                op_name="zrange fleet-insights",
                default=[],
                strict=False,
            ) or []
        )
    status_counts: dict[str, int] = {"available": 0, "delivering": 0, "idle": 0}
    idle_suspects: list[dict] = []
    low_battery: list[dict] = []

    if livreur_ids:
        pipe = r.pipeline(transaction=False)
        for lid in livreur_ids:
            pipe.hmget(f"{HASH_PREFIX}{lid}", "status", "speed_kmh", "ts", "lat", "lon", "battery_pct")
        results = await _run_redis(
            pipe.execute(),
            op_name="pipeline hmget fleet-insights",
            default=[],
            strict=False,
        ) or []
        for lid, vals in zip(livreur_ids, results):
            status, speed, ts, lat, lon, battery = vals
            canonical_status = _canonical_status(status)
            if canonical_status in status_counts:
                status_counts[canonical_status] += 1
            try:
                # Livreur "delivering" mais vitesse quasi nulle → suspect
                if canonical_status == "delivering" and float(speed or 0) < 1.5:
                    idle_suspects.append({
                        "livreur_id": lid,
                        "status": canonical_status,
                        "speed_kmh": round(float(speed or 0), 1),
                        "ts": ts,
                    })
                # Batterie faible (<20%) → alerte opérationnelle
                batt = float(battery or 0)
                if batt > 0 and batt < 20:
                    low_battery.append({
                        "livreur_id": lid,
                        "battery_pct": round(batt, 1),
                        "status": canonical_status,
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
    cold_stats = {
        "livreurs_actifs_periode": 0,
        "vitesse_moyenne_kmh": 0.0,
        "taux_livraison_pct": 0.0,
        "alertes_vitesse_exces": 0,
        "stops_suspects_total": 0,
    }
    cold_path_available = False
    try:
        rows = await _dq(
            f"""
            SELECT
                COUNT(DISTINCT livreur_id)                              AS nb_livreurs,
                ROUND(AVG(speed_kmh), 1)                                AS vitesse_moy,
                ROUND(AVG(CASE WHEN status = 'delivering' THEN 1.0 ELSE 0.0 END) * 100, 1)
                                                                        AS taux_livraison_pct,
                COUNT(*) FILTER (WHERE speed_kmh > 50)                  AS alertes_vitesse,
                COUNT(*) FILTER (WHERE speed_kmh < 1 AND status = 'delivering')
                                                                        AS stops_suspects
            FROM {scan_sql}
            WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            """,
            [_dt_to_iso(window_start), _dt_to_iso(ref_ts)],
        )
        row = rows[0] if rows else None
        if row:
            cold_path_available = True
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
    if len(low_battery) > 10:
        score -= 10   # Trop de livreurs en batterie faible
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
            "redis_available":     redis_available,
        },
        "alertes": {
            "livreurs_immobiles_en_livraison": len(idle_suspects),
            "detail_suspects": idle_suspects[:5],
            "livreurs_batterie_faible": len(low_battery),
            "detail_batterie": sorted(low_battery, key=lambda x: x["battery_pct"])[:5],
        },
        "productivite_historique": cold_stats,
        "cold_path_available": cold_path_available,
        "recommandations": _build_recommendations(
            utilisation_pct, inactivite_pct, len(idle_suspects),
            cold_stats.get("alertes_vitesse_exces", 0), len(low_battery),
        ),
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
    }


def _build_recommendations(
    util_pct: float, idle_pct: float, suspects: int, speed_alerts: int,
    low_batt: int = 0,
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
    if low_batt > 5:
        recs.append(
            f"{low_batt} livreur(s) en batterie faible (<20%) — "
            "prévoir des relais de charge ou rappeler les livreurs concernés."
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
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Score de performance individuel calculé sur le Cold Path (DuckDB/Parquet).

    Métriques : productivité (% temps en livraison), efficacité (vitesse moyenne),
    sécurité (excès de vitesse), fiabilité (continuité de service).

    Cas d'usage : primes de performance, tarification assurance, onboarding RH.
    """
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    window_start = ref_ts - timedelta(hours=heures)
    scan_sql = _parquet_scan_sql(window_start, ref_ts)
    try:
        rows = await _dq(
            f"""
            WITH ordered AS (
                SELECT
                    speed_kmh, status, ts, lat, lon,
                    LAG(lat) OVER (ORDER BY ts) AS prev_lat,
                    LAG(lon) OVER (ORDER BY ts) AS prev_lon
                FROM {scan_sql}
                WHERE livreur_id = ?
                  AND ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
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
            [livreur_id, _dt_to_iso(window_start), _dt_to_iso(ref_ts)],
        )
        row = rows[0] if rows else None
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
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
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
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Compare la distribution actuelle des livreurs (Hot Path Redis) avec la densité
    historique de passages (Cold Path DuckDB) pour identifier les déséquilibres.

    Cas d'usage : dispatch intelligent, rééquilibrage de flotte, alertes zones découvertes.
    """
    r: aioredis.Redis = app.state.redis
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    window_start = ref_ts - timedelta(hours=heures)
    scan_sql = _parquet_scan_sql(window_start, ref_ts)

    # ── Hot Path : positions actuelles → agrégation par cellule ─────────────
    raw = await _run_redis(
        r.geosearch(
            GEO_KEY,
            longitude=-73.9855, latitude=40.7580,
            radius=20, unit="km",
            withcoord=True, count=500,
        ),
        op_name="geosearch zone-coverage",
        default=[],
        strict=False,
    ) or []
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
        rows = await _dq(
            f"""
            SELECT
                ROUND(ROUND(lat / ?, 0) * ?, 4)  AS lat_cell,
                ROUND(ROUND(lon / ?, 0) * ?, 4)  AS lon_cell,
                COUNT(*)                          AS passages
            FROM {scan_sql}
            WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            GROUP BY lat_cell, lon_cell
            """,
            [resolution, resolution, resolution, resolution, _dt_to_iso(window_start), _dt_to_iso(ref_ts)],
        )
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
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
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
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Analyse statistique du comportement de chaque livreur sur les N dernières minutes.
    Détecte 3 types d'anomalies :

    - VITESSE_EXCESSIVE : dépassement du seuil configurable (défaut 50 km/h)
    - IMMOBILISATION_SUSPECTE : livreur en 'delivering' quasi à l'arrêt > 2 min
    - DEVIATION_COMPORTEMENTALE : vitesse actuelle dévie fortement de sa propre baseline (z-score)

    Cas d'usage : sécurité routière, détection pannes/accidents, conformité assurance.
    """
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    baseline_start = ref_ts - timedelta(minutes=fenetre_minutes * 2)
    analysis_start = ref_ts - timedelta(minutes=fenetre_minutes)
    scan_sql = _parquet_scan_sql(baseline_start, ref_ts, max_hours=24)
    try:
        # Récupère les stats par livreur sur la fenêtre demandée
        rows = await _dq(
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
                FROM {scan_sql}
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
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
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
                GROUP BY livreur_id
                HAVING COUNT(*) >= 3
            )
            SELECT * FROM recent
            WHERE nb_exces_vitesse > 0
               OR nb_immobile_delivering >= 2
               OR z_score > 2.5
            ORDER BY z_score DESC, nb_exces_vitesse DESC
            """,
            [
                _dt_to_iso(baseline_start),
                _dt_to_iso(ref_ts),
                seuil_vitesse,
                seuil_immobile,
                _dt_to_iso(analysis_start),
                _dt_to_iso(ref_ts),
            ],
        )
        scanned_rows = await _dq(
            f"""
            SELECT COUNT(DISTINCT livreur_id)
            FROM {scan_sql}
            WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            """,
            [_dt_to_iso(analysis_start), _dt_to_iso(ref_ts)],
        )
        total_scanned = scanned_rows[0][0] if scanned_rows else 0
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
        "time_reference": _build_time_reference(
            ref_meta,
            ref_ts,
            analysis_start,
            {"baseline_window_start": _dt_to_iso(baseline_start)},
        ),
    }


# ────────────────────────────────────────────────────────────────────────────────

@app.get(
    "/analytics/gps-fraud",
    summary="Détection de fraude GPS — téléportations et sauts impossibles",
    tags=["Business Intelligence"],
)
async def detect_gps_fraud(
    fenetre_minutes: int = Query(15, description="Fenêtre d'analyse en minutes", ge=5, le=60),
    seuil_teleport_km: float = Query(2.0, description="Distance min pour considérer une téléportation (km)", ge=0.5, le=20.0),
    vitesse_max_physique_kmh: float = Query(90.0, description="Vitesse max physiquement possible (km/h)", ge=30.0, le=200.0),
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Détecte les anomalies GPS indicatives de fraude ou de dysfonctionnement matériel :

    - TELEPORTATION : saut de position physiquement impossible (distance / temps → vitesse irréaliste)
    - POSITION_FIGEE : coordonnées identiques sur >5 mesures consécutives (GPS gelé / spoofing)
    - SAUT_ZONE : changement brusque de zone géographique (>2km en <10s)

    Calcul : Haversine entre positions consécutives par livreur, ordonnées par timestamp.
    Si la vitesse implicite dépasse le seuil physique → fraude potentielle.

    Cas d'usage : détection de triche livreur (fausse position), GPS spoofing,
    dysfonctionnement capteur, compliance assurance.
    """
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    window_start = ref_ts - timedelta(minutes=fenetre_minutes)
    scan_sql = _parquet_scan_sql(window_start, ref_ts, max_hours=24)
    try:
        rows = await _dq(
            f"""
            WITH ordered AS (
                SELECT
                    livreur_id,
                    lat, lon, speed_kmh, ts,
                    LAG(lat) OVER w  AS prev_lat,
                    LAG(lon) OVER w  AS prev_lon,
                    LAG(ts)  OVER w  AS prev_ts
                FROM {scan_sql}
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
                WINDOW w AS (PARTITION BY livreur_id ORDER BY ts)
            ),
            jumps AS (
                SELECT
                    livreur_id,
                    lat, lon, prev_lat, prev_lon,
                    ts, prev_ts,
                    speed_kmh,
                    -- Distance Haversine entre positions consécutives
                    6371.0 * 2 * ASIN(SQRT(
                        POWER(SIN(RADIANS(lat - prev_lat) / 2), 2) +
                        COS(RADIANS(prev_lat)) * COS(RADIANS(lat)) *
                        POWER(SIN(RADIANS(lon - prev_lon) / 2), 2)
                    )) AS jump_km,
                    -- Temps entre les deux mesures (en heures)
                    EXTRACT(EPOCH FROM (ts - prev_ts)) / 3600.0 AS dt_hours
                FROM ordered
                WHERE prev_lat IS NOT NULL
            ),
            fraud AS (
                SELECT
                    livreur_id,
                    lat, lon, prev_lat, prev_lon,
                    ts, prev_ts,
                    speed_kmh,
                    ROUND(jump_km, 3) AS jump_km,
                    ROUND(
                        CASE WHEN dt_hours > 0 THEN jump_km / dt_hours ELSE 0 END
                    , 1) AS vitesse_implicite_kmh
                FROM jumps
                WHERE jump_km > ?
                  AND dt_hours > 0
                  AND (jump_km / dt_hours) > ?
            )
            SELECT * FROM fraud
            ORDER BY vitesse_implicite_kmh DESC
            LIMIT 50
            """,
            [
                _dt_to_iso(window_start),
                _dt_to_iso(ref_ts),
                seuil_teleport_km,
                vitesse_max_physique_kmh,
            ],
        )

        # Positions figées : même lat/lon sur >5 mesures consécutives
        frozen = await _dq(
            f"""
            WITH numbered AS (
                SELECT
                    livreur_id,
                    lat, lon, ts,
                    ROUND(lat, 5) AS rlat,
                    ROUND(lon, 5) AS rlon,
                    ROW_NUMBER() OVER (PARTITION BY livreur_id ORDER BY ts) AS rn
                FROM {scan_sql}
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            ),
            groups AS (
                SELECT *,
                    rn - ROW_NUMBER() OVER (PARTITION BY livreur_id, rlat, rlon ORDER BY ts) AS grp
                FROM numbered
            )
            SELECT
                livreur_id,
                rlat AS lat,
                rlon AS lon,
                COUNT(*) AS nb_repetitions,
                MIN(ts) AS debut,
                MAX(ts) AS fin
            FROM groups
            GROUP BY livreur_id, rlat, rlon, grp
            HAVING COUNT(*) >= 5
            ORDER BY nb_repetitions DESC
            LIMIT 20
            """,
            [_dt_to_iso(window_start), _dt_to_iso(ref_ts)],
        )

        scanned_rows = await _dq(
            f"""
            SELECT COUNT(DISTINCT livreur_id)
            FROM {scan_sql}
            WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            """,
            [_dt_to_iso(window_start), _dt_to_iso(ref_ts)],
        )
        total_scanned = scanned_rows[0][0] if scanned_rows else 0
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    teleportations = []
    for row in rows:
        lid, lat, lon, prev_lat, prev_lon, ts, prev_ts, speed, jump_km, v_impl = row
        teleportations.append({
            "livreur_id":             lid,
            "type":                   "TELEPORTATION",
            "niveau":                 "critique" if v_impl > 500 else "warning",
            "saut_km":                float(jump_km),
            "vitesse_implicite_kmh":  float(v_impl),
            "seuil_kmh":              vitesse_max_physique_kmh,
            "position_avant":         {"lat": float(prev_lat), "lon": float(prev_lon), "ts": str(prev_ts)},
            "position_apres":         {"lat": float(lat),      "lon": float(lon),      "ts": str(ts)},
            "vitesse_declaree_kmh":   float(speed),
            "risque":                 "Fraude GPS probable — position falsifiée ou capteur défaillant",
        })

    positions_figees = []
    for row in frozen:
        lid, lat, lon, nb, debut, fin = row
        positions_figees.append({
            "livreur_id":     lid,
            "type":           "POSITION_FIGEE",
            "niveau":         "warning",
            "lat":            float(lat),
            "lon":            float(lon),
            "nb_repetitions": nb,
            "debut":          str(debut),
            "fin":            str(fin),
            "risque":         "GPS gelé / spoofing — coordonnées identiques répétées",
        })

    nb_critique = sum(1 for t in teleportations if t["niveau"] == "critique")

    return {
        "fenetre_minutes":    fenetre_minutes,
        "seuils": {
            "teleportation_km":         seuil_teleport_km,
            "vitesse_max_physique_kmh": vitesse_max_physique_kmh,
            "position_figee_min_rep":   5,
        },
        "resume": {
            "livreurs_scannes":     total_scanned,
            "teleportations":       len(teleportations),
            "positions_figees":     len(positions_figees),
            "critiques":            nb_critique,
            "total_fraudes":        len(teleportations) + len(positions_figees),
        },
        "teleportations": teleportations,
        "positions_figees": positions_figees,
        "methodologie": (
            "Haversine entre positions GPS consécutives par livreur. "
            "Si vitesse implicite (distance/temps) > seuil physique → téléportation. "
            "Positions figées : mêmes coordonnées (arrondi 5 décimales) sur ≥5 mesures."
        ),
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
    }


# ════════════════════════════════════════════════════════════════════════════════
#  DISPATCH PRÉDICTIF — Feedback Loop + Demand Prediction
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/analytics/predict-demand",
    summary="Dispatch prédictif — zones en hausse de fréquentation à T+N min",
    tags=["Business Intelligence"],
)
async def predict_demand(
    horizon_minutes: int = Query(30, description="Horizon de prédiction en minutes", ge=15, le=120),
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Identifie les zones où la demande est en hausse en comparant l'activité
    des 15 dernières minutes à la baseline de l'heure écoulée.

    Méthode : extrapolation tendancielle (activité T-15min vs baseline T-1h).
    Une zone en +15% d'activité recente → prédiction de forte demande dans {horizon}min.

    Cas d'usage : envoyer des livreurs disponibles dans une zone AVANT que les
    commandes n'arrivent — c'est le principe du dispatch prédictif d'Uber/Deliveroo.
    """
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    recent_start = ref_ts - timedelta(minutes=15)
    baseline_start = ref_ts - timedelta(hours=1)
    scan_sql = _parquet_scan_sql(baseline_start, ref_ts, max_hours=24)
    try:
        rows = await _dq(
            f"""
            WITH recent AS (
                SELECT
                    ROUND(ROUND(lat / 0.02, 0) * 0.02, 4) AS lat_cell,
                    ROUND(ROUND(lon / 0.02, 0) * 0.02, 4) AS lon_cell,
                    COUNT(*)                               AS recent_count,
                    ROUND(AVG(speed_kmh), 1)               AS recent_speed
                FROM {scan_sql}
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
                GROUP BY lat_cell, lon_cell
            ),
            baseline AS (
                SELECT
                    ROUND(ROUND(lat / 0.02, 0) * 0.02, 4) AS lat_cell,
                    ROUND(ROUND(lon / 0.02, 0) * 0.02, 4) AS lon_cell,
                    COUNT(*) / 3.0                         AS baseline_count
                FROM {scan_sql}
                WHERE ts >= CAST(? AS TIMESTAMPTZ)
                  AND ts <  CAST(? AS TIMESTAMPTZ)
                GROUP BY lat_cell, lon_cell
            )
            SELECT
                r.lat_cell,
                r.lon_cell,
                r.recent_count,
                r.recent_speed,
                COALESCE(b.baseline_count, r.recent_count)    AS baseline_count,
                ROUND(
                    (r.recent_count - COALESCE(b.baseline_count, r.recent_count))
                    / NULLIF(COALESCE(b.baseline_count, r.recent_count), 0) * 100
                , 1)                                           AS tendance_pct
            FROM recent r
            LEFT JOIN baseline b USING (lat_cell, lon_cell)
            WHERE r.recent_count >= 5
            ORDER BY tendance_pct DESC
            LIMIT 30
            """,
            [
                _dt_to_iso(recent_start),
                _dt_to_iso(ref_ts),
                _dt_to_iso(baseline_start),
                _dt_to_iso(recent_start),
            ],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur DuckDB : {exc}")

    zones = []
    for lat_c, lon_c, recent, speed, baseline, trend in rows:
        trend = trend or 0.0
        signal = "hausse" if trend > 15 else "baisse" if trend < -15 else "stable"
        action = (
            "Envoyer un livreur disponible" if signal == "hausse" else
            "Redéployer vers une zone plus active" if signal == "baisse" else
            "Maintenir la couverture actuelle"
        )
        zones.append({
            "lat":              lat_c,
            "lon":              lon_c,
            "activite_recente": recent,
            "baseline_15min":   round(float(baseline), 1),
            "vitesse_recente":  speed,
            "tendance_pct":     trend,
            "signal":           signal,
            "action":           action,
        })

    hot   = [z for z in zones if z["signal"] == "hausse"]
    cold  = [z for z in zones if z["signal"] == "baisse"]
    stable = [z for z in zones if z["signal"] == "stable"]

    return {
        "horizon_minutes":  horizon_minutes,
        "methode":          "Extrapolation tendancielle T-15min vs baseline T-1h",
        "nb_zones_analysees": len(zones),
        "resume": {
            "zones_en_hausse": len(hot),
            "zones_stables":   len(stable),
            "zones_en_baisse": len(cold),
        },
        "dispatch_prioritaire": hot[:5],
        "zones_a_decouvrir":    cold[:3],
        "toutes_zones":         zones,
        "time_reference": _build_time_reference(
            ref_meta,
            ref_ts,
            recent_start,
            {"baseline_window_start": _dt_to_iso(baseline_start)},
        ),
    }


# ════════════════════════════════════════════════════════════════════════════════
#  SURGE PRICING — Pièce maîtresse business
#  Combine Hot Path (offre instantanée) + Cold Path (demande historique)
#  pour calculer un multiplicateur de prix dynamique explicable.
# ════════════════════════════════════════════════════════════════════════════════

@app.get(
    "/analytics/surge-price",
    summary="Tarification dynamique — multiplicateur surge explicable (Hot ⨯ Cold)",
    tags=["Business Intelligence"],
)
async def surge_price(
    lat:    float = Query(..., description="Latitude du point de demande", ge=-90, le=90),
    lon:    float = Query(..., description="Longitude du point de demande", ge=-180, le=180),
    rayon:  float = Query(2.0, description="Rayon de la zone (km)", ge=0.5, le=10.0),
    base_price_eur: float = Query(5.0, description="Prix de base de la course en euros", ge=1.0, le=100.0),
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Calcule un multiplicateur de prix dynamique (surge) à la Uber/Deliveroo.

    Le surge n'est pas une boîte noire : il est décomposé en deux pressions
    indépendantes, chacune calculée à partir d'une couche distincte de
    l'architecture Lambda — c'est précisément ce qui rend l'endpoint
    explicable et défendable face au régulateur (DGCCRF en France impose
    la transparence des prix dynamiques sur les plateformes de livraison) :

      • PRESSION DEMANDE (Cold Path)
        Compare l'activité GPS dans la zone sur les 15 dernières minutes
        avec la baseline horaire historique (1h glissante normalisée).
        Une activité actuelle 2× supérieure à la baseline = forte demande.
        Source : DuckDB sur Parquet partitionné par event time.

      • PRESSION OFFRE (Hot Path)
        Ratio livreurs disponibles / livreurs totaux dans le rayon.
        Source : Redis GEOSEARCH (<10ms).
        Plus la part de livreurs "delivering" est élevée, plus l'offre
        disponible est rare → surge plus élevé.

    Formule (additive, plafonnée) :
        multiplier = 1.0 + α·demand_pressure + β·supply_scarcity
        avec α=1.0, β=0.8, plafond 3.0× (au-delà = manipulation déloyale).

    Cas d'usage : pricing dynamique des courses, allocation d'incentives
    aux livreurs en zone tendue, alerte congestion pour le dispatch.
    """
    r: aioredis.Redis = app.state.redis
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)

    # ── 1. HOT PATH : photographie instantanée de l'offre dans le rayon ──────
    raw_drivers = await _run_redis(
        r.geosearch(
            GEO_KEY,
            longitude=lon, latitude=lat,
            radius=rayon, unit="km",
            withcoord=False, count=500,
        ),
        op_name="geosearch surge-price",
        default=[],
        strict=False,
    ) or []
    nb_total_zone = len(raw_drivers)

    nb_available = 0
    nb_delivering = 0
    nb_idle = 0
    if raw_drivers:
        pipe = r.pipeline(transaction=False)
        for lid in raw_drivers:
            pipe.hget(f"{HASH_PREFIX}{lid}", "status")
        statuses = await _run_redis(
            pipe.execute(),
            op_name="pipeline status surge-price",
            default=[],
            strict=False,
        ) or []
        for status in statuses:
            canonical_status = _canonical_status(status)
            if canonical_status == "available":
                nb_available += 1
            elif canonical_status == "delivering":
                nb_delivering += 1
            elif canonical_status == "idle":
                nb_idle += 1

    # Pression offre : 0.0 (beaucoup de livreurs dispos) → 1.0 (aucun dispo)
    # On ne peut pas diviser par 0 — si la zone est désertée, scarcity = 1.0 max.
    if nb_total_zone == 0:
        supply_scarcity = 1.0
        supply_label = "désert"
    else:
        ratio_available = nb_available / nb_total_zone
        supply_scarcity = round(1.0 - ratio_available, 3)
        if ratio_available >= 0.5:
            supply_label = "abondante"
        elif ratio_available >= 0.25:
            supply_label = "modérée"
        elif ratio_available > 0:
            supply_label = "tendue"
        else:
            supply_label = "saturée"

    # ── 2. COLD PATH : pression demande sur la zone (~rayon en degrés) ───────
    # Conversion approximative km → degrés (1° lat ≈ 111.32 km, lon dépend du cos)
    dlat = rayon / 111.32
    dlon = rayon / (111.32 * max(math.cos(math.radians(lat)), 0.01))
    recent_start = ref_ts - timedelta(minutes=15)
    baseline_start = ref_ts - timedelta(hours=1)
    scan_sql = _parquet_scan_sql(baseline_start, ref_ts, max_hours=24)

    demand_pressure = 0.0
    recent_count = 0
    baseline_count = 0.0
    try:
        rows = await _dq(
            f"""
            WITH zone_filter AS (
                SELECT speed_kmh, status, ts
                FROM {scan_sql}
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
                  AND lat BETWEEN ? AND ?
                  AND lon BETWEEN ? AND ?
            ),
            recent AS (
                SELECT COUNT(*) AS n
                FROM zone_filter
                WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            ),
            baseline AS (
                -- Baseline : moyenne 15-min sur l'heure écoulée (normalisée)
                SELECT COUNT(*) / 4.0 AS n
                FROM zone_filter
                WHERE ts >= CAST(? AS TIMESTAMPTZ)
                  AND ts <  CAST(? AS TIMESTAMPTZ)
            )
            SELECT recent.n, baseline.n
            FROM recent, baseline
            """,
            [
                _dt_to_iso(baseline_start),
                _dt_to_iso(ref_ts),
                lat - dlat,
                lat + dlat,
                lon - dlon,
                lon + dlon,
                _dt_to_iso(recent_start),
                _dt_to_iso(ref_ts),
                _dt_to_iso(baseline_start),
                _dt_to_iso(recent_start),
            ],
        )
        if rows and rows[0]:
            recent_count = int(rows[0][0] or 0)
            baseline_count = float(rows[0][1] or 0.0)
            if baseline_count >= 1.0:
                # Ratio recent / baseline, recentré : 1.0 = stable, 2.0 = doublé
                # On clamp à [0, 2] puis on divise par 2 pour rester dans [0, 1]
                ratio = recent_count / baseline_count
                demand_pressure = round(min(max(ratio - 1.0, 0.0), 2.0) / 2.0, 3)
            elif recent_count > 5:
                # Pas de baseline mais activité réelle → demande émergente
                demand_pressure = 0.3
    except Exception as exc:
        log.warning("surge-price cold path error: %s", exc)
        demand_pressure = 0.0

    # ── 3. CALCUL DU MULTIPLICATEUR (formule additive plafonnée) ─────────────
    ALPHA_DEMAND  = 1.0   # Poids de la pression demande
    BETA_SUPPLY   = 0.8   # Poids de la rareté offre
    SURGE_FLOOR   = 1.0   # Pas de réduction sous le prix de base
    SURGE_CAP     = 3.0   # Plafond légal/éthique (DGCCRF)

    raw_multiplier = 1.0 + ALPHA_DEMAND * demand_pressure + BETA_SUPPLY * supply_scarcity
    multiplier = round(max(SURGE_FLOOR, min(raw_multiplier, SURGE_CAP)), 2)
    surge_price_eur = round(base_price_eur * multiplier, 2)

    # ── 4. NIVEAU & EXPLICATION HUMAINE ──────────────────────────────────────
    if multiplier >= 2.0:
        niveau = "extrême"
    elif multiplier >= 1.5:
        niveau = "élevé"
    elif multiplier >= 1.2:
        niveau = "modéré"
    else:
        niveau = "normal"

    raisons = []
    if demand_pressure >= 0.5:
        raisons.append(
            f"Demande {round(recent_count / max(baseline_count, 1), 1)}× supérieure "
            f"à la baseline horaire ({recent_count} positions vs {round(baseline_count,1)} attendues)"
        )
    elif demand_pressure > 0:
        raisons.append(
            f"Demande légèrement au-dessus de la baseline ({recent_count} vs {round(baseline_count,1)})"
        )
    if supply_scarcity >= 0.6:
        raisons.append(
            f"Offre {supply_label} : {nb_available}/{nb_total_zone} livreurs disponibles dans le rayon"
        )
    elif supply_scarcity > 0:
        raisons.append(
            f"Offre {supply_label} : {nb_available} disponibles sur {nb_total_zone} présents"
        )
    if not raisons:
        raisons.append("Marché équilibré — aucune pression détectée")

    return {
        "zone": {
            "lat": lat,
            "lon": lon,
            "rayon_km": rayon,
        },
        "pricing": {
            "base_price_eur":   base_price_eur,
            "multiplier":       multiplier,
            "surge_price_eur":  surge_price_eur,
            "niveau":           niveau,
            "cap_atteint":      multiplier >= SURGE_CAP,
        },
        "supply": {
            "source":            "Redis Hot Path (GEOSEARCH)",
            "livreurs_total":    nb_total_zone,
            "available":         nb_available,
            "delivering":        nb_delivering,
            "idle":              nb_idle,
            "scarcity_score":    supply_scarcity,
            "label":             supply_label,
        },
        "demand": {
            "source":            "DuckDB Cold Path (Parquet 15min vs baseline 1h)",
            "recent_15min":      recent_count,
            "baseline_15min":    round(baseline_count, 1),
            "pressure_score":    demand_pressure,
        },
        "decomposition": {
            "alpha_demand":      ALPHA_DEMAND,
            "beta_supply":       BETA_SUPPLY,
            "raw_multiplier":    round(raw_multiplier, 3),
            "floor":             SURGE_FLOOR,
            "cap":               SURGE_CAP,
            "formule":           "1.0 + α·demand_pressure + β·supply_scarcity",
        },
        "explication":           " ; ".join(raisons),
        "compliance": (
            "Multiplicateur plafonné à 3.0× — décomposition complète exposée "
            "pour transparence DGCCRF (loi française sur les prix dynamiques)."
        ),
        "time_reference": _build_time_reference(
            ref_meta,
            ref_ts,
            recent_start,
            {"baseline_window_start": _dt_to_iso(baseline_start)},
        ),
    }


@app.get(
    "/analytics/feedback-loop-status",
    summary="Statut du Feedback Loop — Baselines de vitesse injectées dans Redis",
    tags=["Business Intelligence"],
)
async def feedback_loop_status():
    """
    Retourne le statut du Feedback Loop : nombre de zones avec baseline Redis,
    quelques exemples de clés, et la dernière mise à jour.
    """
    r: aioredis.Redis = app.state.redis
    redis_available = bool(
        await _run_redis(
            r.ping(),
            op_name="ping feedback-loop-status",
            default=False,
            strict=False,
        )
    )
    if not redis_available:
        return {
            "actif": False,
            "message": "Redis indisponible: feedback loop temporairement inaccessible.",
            "nb_zones": 0,
            "exemples": [],
        }

    # SCAN cursor-based, non-bloquant — KEYS bloquerait Redis sur tout le keyspace.
    # COUNT=200 = compromis entre nombre de round-trips et taille des batches.
    keys: list[str] = []
    async for k in r.scan_iter(match="fleet:context:zone:*", count=200):
        keys.append(k)

    if not keys:
        return {
            "actif": False,
            "message": "Feedback loop en attente — données Parquet insuffisantes (< 30s après démarrage).",
            "nb_zones": 0,
            "exemples": [],
        }

    pipe = r.pipeline(transaction=False)
    for k in keys[:5]:
        pipe.hgetall(k)
    samples = await _run_redis(
        pipe.execute(),
        op_name="pipeline feedback-loop-status samples",
        default=[],
        strict=False,
    ) or []

    exemples = []
    for k, h in zip(keys[:5], samples):
        zone = k.replace("fleet:context:zone:", "")
        exemples.append({
            "zone":      zone,
            "avg_speed": h.get("avg_speed"),
            "std_speed": h.get("std_speed"),
            "nb_points": h.get("nb_points"),
        })

    return {
        "actif":    True,
        "nb_zones": len(keys),
        "description": (
            "Les baselines de vitesse par zone (calculées sur 2h d'historique Parquet) "
            "sont injectées dans Redis toutes les 5 minutes. "
            "Le scoring et la détection d'anomalies utilisent ces données "
            "pour contextualiser les seuils."
        ),
        "exemples": exemples,
    }


@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok", "version": "2.0.0"}
