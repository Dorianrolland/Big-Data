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
import copy
import json
import logging
import math
import os
import statistics
import time
from collections import OrderedDict
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
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
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
TLC_SCENARIO = (os.getenv("TLC_SCENARIO", "fleet") or "fleet").strip().lower()
TLC_SINGLE_DRIVER_ID = (os.getenv("TLC_SINGLE_DRIVER_ID", "drv_demo_001") or "drv_demo_001").strip()
FOCUS_TRAIL_LOOKBACK_MIN = max(10, min(180, int(os.getenv("FOCUS_TRAIL_LOOKBACK_MIN", "45") or "45")))
FOCUS_TRAIL_MAX_HOURS = max(1, min(12, int(os.getenv("FOCUS_TRAIL_MAX_HOURS", "2") or "2")))
FOCUS_TRAIL_QUERY_TIMEOUT_SECONDS = float(os.getenv("FOCUS_TRAIL_QUERY_TIMEOUT_SECONDS", "3.0"))
FOCUS_TRAIL_CACHE_TTL_SECONDS = max(1.0, float(os.getenv("FOCUS_TRAIL_CACHE_TTL_SECONDS", "8.0") or "8.0"))
FOCUS_TRAIL_CACHE_MAX = max(32, int(os.getenv("FOCUS_TRAIL_CACHE_MAX", "512") or "512"))
PERF_BENCH_CACHE_TTL_SECONDS = max(10.0, float(os.getenv("PERF_BENCH_CACHE_TTL_SECONDS", "60.0") or "60.0"))
PERF_BENCH_MAX_SAMPLES = max(20, int(os.getenv("PERF_BENCH_MAX_SAMPLES", "120") or "120"))
STATS_STATUS_SCAN_LIMIT = max(0, int(os.getenv("STATS_STATUS_SCAN_LIMIT", "1500") or "1500"))
HOT_PATH_TTL_SECONDS = max(5, int(float(os.getenv("GPS_TTL_SECONDS", "30") or "30")))
STATS_COLD_CACHE_TTL_SECONDS = max(
    5.0,
    float(os.getenv("STATS_COLD_CACHE_TTL_SECONDS", "45.0") or "45.0"),
)
ANALYTICS_RESULT_CACHE_TTL_SECONDS = max(
    10.0,
    float(os.getenv("ANALYTICS_RESULT_CACHE_TTL_SECONDS", "30.0") or "30.0"),
)
ANALYTICS_RESULT_CACHE_MAX = max(
    32,
    int(os.getenv("ANALYTICS_RESULT_CACHE_MAX", "256") or "256"),
)
ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS = max(
    1.0,
    float(os.getenv("ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS", "4.0") or "4.0"),
)

GEO_KEY          = "fleet:geo"
HASH_PREFIX      = "fleet:livreur:"
TRACK_KEY_PREFIX = "fleet:track:"
STATS_MSGS_KEY   = "fleet:stats:total_messages"
STATS_DLQ_KEY    = "fleet:stats:dlq_count"
DLQ_KEY          = "fleet:dlq"
DLQ_PATH         = Path(os.getenv("DLQ_PATH", "/data/dlq"))
TLC_REPLAY_STATUS_KEY = "copilot:replay:tlc:status"
ZONE_CONTEXT_PREFIX = "copilot:context:zone:"
DELIVERING_STATUS_ALIASES = {"delivering", "on_trip", "busy"}
PICKUP_STATUS_ALIASES = {"pickup_arrived", "pickup_assigned", "pickup_en_route"}
REPOSITIONING_STATUS_ALIASES = {"repositioning"}
AVAILABLE_STATUS_ALIASES = {"available", "ready", "waiting_order"}
IDLE_STATUS_ALIASES = {"idle", "offline", "paused"}
STATUS_DETAIL_KEYS = (
    "available",
    "pickup_assigned",
    "pickup_en_route",
    "pickup_arrived",
    "repositioning",
    "delivering",
    "idle",
    "unknown",
)
STATUS_BUCKET_KEYS = (
    "available",
    "pickup",
    "repositioning",
    "delivering",
    "idle",
    "unknown",
)
FLEET_INSIGHTS_STATIONARY_SPEED_KMH = max(
    0.1,
    float(os.getenv("FLEET_INSIGHTS_STATIONARY_SPEED_KMH", "1.5") or "1.5"),
)
FLEET_INSIGHTS_STATIONARY_MIN_SECONDS = max(
    60,
    int(float(os.getenv("FLEET_INSIGHTS_STATIONARY_MIN_SECONDS", "120") or "120")),
)
FLEET_INSIGHTS_TRACK_POINTS = max(
    4,
    int(float(os.getenv("FLEET_INSIGHTS_TRACK_POINTS", "12") or "12")),
)
HISTORY_HOT_FALLBACK_TRACK_POINTS = max(
    FLEET_INSIGHTS_TRACK_POINTS,
    min(64, int(float(os.getenv("HISTORY_HOT_FALLBACK_TRACK_POINTS", "64") or "64"))),
)
ANOMALY_STATIONARY_MIN_SECONDS = max(
    60,
    int(
        float(
            os.getenv(
                "ANOMALY_STATIONARY_MIN_SECONDS",
                str(FLEET_INSIGHTS_STATIONARY_MIN_SECONDS),
            )
            or str(FLEET_INSIGHTS_STATIONARY_MIN_SECONDS)
        )
    ),
)
ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH = max(
    30.0,
    float(os.getenv("ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH", "65.0") or "65.0"),
)
ANOMALY_SPEED_MIN_EXCEEDANCES = max(
    1,
    int(float(os.getenv("ANOMALY_SPEED_MIN_EXCEEDANCES", "3") or "3")),
)
ANOMALY_ZSCORE_THRESHOLD = max(
    2.5,
    float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "3.0") or "3.0"),
)
ANOMALY_BASELINE_SPEED_CAP_KMH = max(
    ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH,
    float(os.getenv("ANOMALY_BASELINE_SPEED_CAP_KMH", "90.0") or "90.0"),
)
GPS_FRAUD_MIN_INTERVAL_SECONDS = max(
    1,
    int(float(os.getenv("GPS_FRAUD_MIN_INTERVAL_SECONDS", "8") or "8")),
)
GPS_FRAUD_FROZEN_MIN_REPETITIONS = max(
    5,
    int(float(os.getenv("GPS_FRAUD_FROZEN_MIN_REPETITIONS", "12") or "12")),
)
GPS_FRAUD_FROZEN_MIN_DURATION_SECONDS = max(
    60,
    int(float(os.getenv("GPS_FRAUD_FROZEN_MIN_DURATION_SECONDS", "180") or "180")),
)

_focus_trail_cache: OrderedDict[str, tuple[float, list[dict[str, Any]]]] = OrderedDict()
_focus_trail_cache_lock = asyncio.Lock()
_perf_bench_cache: dict[int, tuple[float, dict[str, Any]]] = {}
_perf_bench_cache_lock = asyncio.Lock()
_stats_cold_cache: dict[str, Any] = {"expires_at": 0.0, "payload": {}}
_stats_cold_cache_lock = asyncio.Lock()
_stats_cold_refresh_in_flight = False
_analytics_result_cache: OrderedDict[str, tuple[float, float, dict[str, Any]]] = OrderedDict()
_analytics_result_cache_lock = asyncio.Lock()

# ── Prometheus custom metrics ───────────────────────────────────────────────────
gauge_active_livreurs = Gauge(
    "fleet_active_livreurs",
    f"Nombre de livreurs actifs dans Redis (TTL {HOT_PATH_TTL_SECONDS}s)",
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
async def live_map(request: Request):
    """Dashboard HTML live — carte Leaflet sans clignotement."""
    if TLC_SCENARIO == "single_driver" and not request.query_params.get("focus"):
        return RedirectResponse(url=f"/map?focus={TLC_SINGLE_DRIVER_ID}", status_code=307)
    p = Path(__file__).parent / "static" / "index.html"
    if p.exists():
        return FileResponse(
            str(p),
            media_type="text/html",
            headers={"Cache-Control": "no-store, must-revalidate"},
        )
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
    route_source: str = ""
    anomaly_state: str = "ok"
    stale_reason: str = ""
    recent_track: list[dict[str, Any]] = Field(default_factory=list)


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
    resume: HistoryResume | None = None
    trajectory: list[dict] = Field(default_factory=list)
    time_reference: dict[str, str] | None = None
    cold_path_available: bool | None = None
    analytics_status: dict[str, Any] | None = None
    detail: str | None = None


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


def _path_exists_safe(path: Path) -> bool:
    try:
        return path.exists()
    except OSError as exc:
        log.warning("path exists check failed for %s: %s", path, exc)
        return False


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
        if _path_exists_safe(partition_dir):
            glob = str(partition_dir / "*.parquet")
            if glob not in seen:
                globs.append(glob)
                seen.add(glob)
        cur += timedelta(hours=1)
    return globs


def _recent_hour_partition_globs(limit: int = 8) -> list[str]:
    if not _path_exists_safe(DATA_PATH):
        return []

    try:
        raw_dirs = list(DATA_PATH.glob("year=*/month=*/day=*/hour=*"))
    except OSError as exc:
        log.warning("recent partition scan failed: %s", exc)
        return []

    hour_dirs: list[Path] = []
    for p in raw_dirs:
        try:
            if p.is_dir():
                hour_dirs.append(p)
        except OSError:
            continue
    hour_dirs = sorted(hour_dirs, key=lambda p: p.as_posix(), reverse=True)

    out: list[str] = []
    for part in hour_dirs:
        try:
            if next(part.glob("*.parquet"), None) is None:
                continue
        except OSError:
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
    if status in PICKUP_STATUS_ALIASES:
        return status
    if status in REPOSITIONING_STATUS_ALIASES:
        return "repositioning"
    if status in AVAILABLE_STATUS_ALIASES:
        return "available"
    if status in IDLE_STATUS_ALIASES:
        return "idle"
    return status or "unknown"


def _status_bucket(raw_status: str | None) -> str:
    status = _canonical_status(raw_status)
    if status in {"available", "delivering", "repositioning", "idle"}:
        return status
    if status in PICKUP_STATUS_ALIASES:
        return "pickup"
    return "unknown"


def _status_detail_counts_template() -> dict[str, int]:
    return {key: 0 for key in STATUS_DETAIL_KEYS}


def _status_bucket_counts_template() -> dict[str, int]:
    return {key: 0 for key in STATUS_BUCKET_KEYS}


def _scale_count_map(counts: dict[str, int], total: int, sampled: int) -> dict[str, int]:
    if sampled <= 0 or total <= sampled:
        return dict(counts)
    ratio = total / max(sampled, 1)
    scaled = {key: int(round(value * ratio)) for key, value in counts.items()}
    delta = total - sum(scaled.values())
    scaled["unknown"] = max(0, scaled.get("unknown", 0) + delta)
    return scaled


def _trailing_stationary_duration_seconds(
    points: list[dict[str, Any]],
    *,
    speed_threshold_kmh: float,
    stationary_statuses: set[str] | None = None,
) -> tuple[float, int]:
    stationary_statuses = stationary_statuses or {"delivering"}
    normalized: list[tuple[datetime, float, str]] = []
    for point in points:
        ts = _parse_iso_timestamp(str(point.get("ts") or ""))
        if ts is None:
            continue
        try:
            speed = float(point.get("speed_kmh", 0.0) or 0.0)
        except (TypeError, ValueError):
            speed = 0.0
        normalized.append((ts, speed, _canonical_status(point.get("status"))))
    if not normalized:
        return 0.0, 0
    latest_ts, latest_speed, latest_status = normalized[-1]
    if latest_status not in stationary_statuses or latest_speed > speed_threshold_kmh:
        return 0.0, 0
    earliest_ts = latest_ts
    samples = 0
    for ts, speed, status in reversed(normalized):
        if status not in stationary_statuses or speed > speed_threshold_kmh:
            break
        earliest_ts = ts
        samples += 1
    return max(0.0, (latest_ts - earliest_ts).total_seconds()), samples


def _parse_recent_track(raw_items: list[str] | None) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for raw in reversed(raw_items or []):
        try:
            item = json.loads(raw)
        except Exception:
            continue
        if not isinstance(item, dict):
            continue
        try:
            lat = float(item.get("lat"))
            lon = float(item.get("lon"))
        except (TypeError, ValueError):
            continue
        point = {
            "lat": lat,
            "lon": lon,
            "ts": str(item.get("ts") or ""),
            "speed_kmh": float(item.get("speed_kmh", 0.0) or 0.0),
            "status": _canonical_status(item.get("status")),
            "route_source": str(item.get("route_source") or ""),
            "anomaly_state": str(item.get("anomaly_state") or "ok"),
        }
        points.append(point)
    return points


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2.0) ** 2
    )
    return r_km * 2.0 * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _track_points_in_window(
    points: list[dict[str, Any]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for point in points:
        ts = _parse_iso_timestamp(str(point.get("ts") or ""))
        if ts is None:
            continue
        if ts < window_start or ts > window_end:
            continue
        item = dict(point)
        item["_parsed_ts"] = ts
        out.append(item)
    return out


def _best_live_frozen_streak(
    points: list[dict[str, Any]],
    *,
    suspicious_statuses: set[str] | None = None,
) -> dict[str, Any] | None:
    suspicious_statuses = suspicious_statuses or {"delivering", "repositioning"}
    best: dict[str, Any] | None = None
    streak: dict[str, Any] | None = None

    for point in points:
        ts = point.get("_parsed_ts")
        if not isinstance(ts, datetime):
            ts = _parse_iso_timestamp(str(point.get("ts") or ""))
        if ts is None:
            continue
        status = _canonical_status(point.get("status"))
        if status not in suspicious_statuses:
            streak = None
            continue
        lat = round(float(point.get("lat", 0.0) or 0.0), 5)
        lon = round(float(point.get("lon", 0.0) or 0.0), 5)
        if (
            streak
            and streak["lat"] == lat
            and streak["lon"] == lon
        ):
            streak["count"] += 1
            streak["end"] = ts
        else:
            streak = {
                "lat": lat,
                "lon": lon,
                "count": 1,
                "start": ts,
                "end": ts,
            }
        duration_s = max(0.0, (streak["end"] - streak["start"]).total_seconds())
        if (
            streak["count"] >= GPS_FRAUD_FROZEN_MIN_REPETITIONS
            and duration_s >= GPS_FRAUD_FROZEN_MIN_DURATION_SECONDS
        ):
            candidate = {
                "lat": lat,
                "lon": lon,
                "nb_repetitions": int(streak["count"]),
                "duration_s": int(round(duration_s)),
                "debut": _dt_to_iso(streak["start"]),
                "fin": _dt_to_iso(streak["end"]),
            }
            if (
                best is None
                or candidate["nb_repetitions"] > best["nb_repetitions"]
                or (
                    candidate["nb_repetitions"] == best["nb_repetitions"]
                    and candidate["duration_s"] > best["duration_s"]
                )
            ):
                best = candidate
    return best


def _float_or_default(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def _zone_id_coordinates(zone_id: str | None) -> tuple[float, float] | None:
    token = str(zone_id or "").strip()
    if not token or "_" not in token:
        return None
    lat_raw, lon_raw = token.split("_", 1)
    try:
        return round(float(lat_raw), 6), round(float(lon_raw), 6)
    except (TypeError, ValueError):
        return None


def _is_live_track_degraded(point: dict[str, Any]) -> bool:
    route_source = str(point.get("route_source") or "").strip().lower()
    anomaly_state = str(point.get("anomaly_state") or "ok").strip().lower()
    return route_source == "hold" or anomaly_state not in {"", "ok"}


def _trusted_live_motion_points(
    points: list[dict[str, Any]],
    *,
    speed_cap_kmh: float,
) -> list[dict[str, Any]]:
    trusted: list[dict[str, Any]] = []
    moving_statuses = {
        "delivering",
        "pickup_assigned",
        "pickup_en_route",
        "pickup_arrived",
        "repositioning",
    }
    for point in points:
        if _canonical_status(point.get("status")) not in moving_statuses:
            continue
        if _is_live_track_degraded(point):
            continue
        speed_kmh = _float_or_default(point.get("speed_kmh"), 0.0)
        if speed_kmh < 0.0 or speed_kmh > speed_cap_kmh:
            continue
        trusted.append(point)
    return trusted


def _live_stationary_issue(
    points: list[dict[str, Any]],
    *,
    speed_threshold_kmh: float,
    min_duration_s: float,
) -> tuple[float, int, dict[str, Any] | None]:
    stationary_seconds, stationary_samples = _trailing_stationary_duration_seconds(
        points,
        speed_threshold_kmh=speed_threshold_kmh,
    )
    if stationary_seconds < min_duration_s or not points:
        return 0.0, stationary_samples, None
    frozen = _best_live_frozen_streak(points)
    if frozen is None and not _is_live_track_degraded(points[-1]):
        return 0.0, stationary_samples, None
    return stationary_seconds, stationary_samples, frozen


async def _load_live_zone_context_payloads(redis_client: aioredis.Redis) -> list[dict[str, Any]]:
    zone_keys: list[str] = []
    zone_hashes: list[dict[str, Any]] = []

    scan_iter = getattr(redis_client, "scan_iter", None)
    if callable(scan_iter):
        async for key in scan_iter(match=f"{ZONE_CONTEXT_PREFIX}*"):
            zone_keys.append(str(key))
        if zone_keys:
            pipe = redis_client.pipeline(transaction=False)
            for key in zone_keys:
                pipe.hgetall(key)
            zone_hashes = await _run_redis(
                pipe.execute(),
                op_name="pipeline zone-context",
                default=[],
                strict=False,
            ) or []
    else:
        raw_hashes = getattr(redis_client, "hashes", {})
        if isinstance(raw_hashes, dict):
            for key, value in raw_hashes.items():
                key_token = str(key)
                if not key_token.startswith(ZONE_CONTEXT_PREFIX):
                    continue
                zone_keys.append(key_token)
                zone_hashes.append(value if isinstance(value, dict) else {})

    zones: list[dict[str, Any]] = []
    for key, zone in zip(zone_keys, zone_hashes):
        if not isinstance(zone, dict) or not zone:
            continue
        zone_id = str(zone.get("zone_id") or key.removeprefix(ZONE_CONTEXT_PREFIX))
        coords = _zone_id_coordinates(zone_id)
        if coords is None:
            continue
        demand_index = max(0.0, _float_or_default(zone.get("demand_index"), 0.0))
        supply_index = max(0.0, _float_or_default(zone.get("supply_index"), 0.0))
        traffic_factor = max(0.2, _float_or_default(zone.get("traffic_factor"), 1.0))
        demand_trend = _float_or_default(zone.get("demand_trend"), 0.0)
        forecast_15m = max(
            0.0,
            _float_or_default(
                zone.get("forecast_demand_index_15m"),
                _float_or_default(zone.get("demand_trend_ema"), demand_index),
            ),
        )
        zones.append(
            {
                "zone_id": zone_id,
                "lat": coords[0],
                "lon": coords[1],
                "demand_index": round(demand_index, 4),
                "supply_index": round(supply_index, 4),
                "traffic_factor": round(traffic_factor, 4),
                "demand_trend": round(demand_trend, 4),
                "forecast_demand_index_15m": round(forecast_15m, 4),
            }
        )
    return zones


def _build_zone_coverage_live_fallback(
    *,
    zone_contexts: list[dict[str, Any]],
    supply: dict[tuple[float, float], int],
    resolution: float,
    heures: int,
    ref_meta: dict[str, Any],
    ref_ts: datetime,
    window_start: datetime,
) -> dict[str, Any] | None:
    if not zone_contexts:
        return None
    demand_by_cell: dict[tuple[float, float], float] = {}
    for zone in zone_contexts:
        cell = (
            round(round(float(zone["lat"]) / resolution) * resolution, 4),
            round(round(float(zone["lon"]) / resolution) * resolution, 4),
        )
        demand_signal = max(
            _float_or_default(zone.get("forecast_demand_index_15m"), 0.0),
            _float_or_default(zone.get("demand_index"), 0.0),
        )
        demand_by_cell[cell] = max(demand_by_cell.get(cell, 0.0), demand_signal)
    if not demand_by_cell:
        return None

    max_demand = max(demand_by_cell.values()) or 1.0
    max_supply = max(supply.values()) if supply else 1
    zones: list[dict[str, Any]] = []
    for cell, demand_signal in demand_by_cell.items():
        drivers = int(supply.get(cell, 0) or 0)
        demand_norm = demand_signal / max_demand
        supply_norm = drivers / max(max_supply, 1)
        coverage_gap = round(demand_norm - supply_norm, 3)
        if demand_norm < 0.1:
            continue
        statut = (
            "sur-couverte" if coverage_gap < -0.15 else
            "sous-couverte" if coverage_gap > 0.20 else
            "équilibrée"
        )
        zones.append(
            {
                "lat": cell[0],
                "lon": cell[1],
                "livreurs_actifs": drivers,
                "passages_historiques": max(1, int(round(demand_signal * 100))),
                "ecart_couverture": coverage_gap,
                "statut": statut,
            }
        )
    zones.sort(key=lambda item: item["ecart_couverture"], reverse=True)
    sous_couvertes = [zone for zone in zones if zone["statut"] == "sous-couverte"]
    sur_couvertes = [zone for zone in zones if zone["statut"] == "sur-couverte"]
    return {
        "periode_heures": heures,
        "resolution_deg": resolution,
        "nb_zones_analysees": len(zones),
        "resume": {
            "zones_sous_couvertes": len(sous_couvertes),
            "zones_sur_couvertes": len(sur_couvertes),
            "zones_equilibrees": len(zones) - len(sous_couvertes) - len(sur_couvertes),
        },
        "alertes_dispatch": {
            "zones_prioritaires": sous_couvertes[:5],
            "zones_saturees": sur_couvertes[:5],
        },
        "toutes_zones": zones,
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
    }


def _build_predict_demand_live_fallback(
    *,
    zone_contexts: list[dict[str, Any]],
    horizon_minutes: int,
    ref_meta: dict[str, Any],
    ref_ts: datetime,
    recent_start: datetime,
    baseline_start: datetime,
) -> dict[str, Any] | None:
    if not zone_contexts:
        return None
    zones: list[dict[str, Any]] = []
    for zone in zone_contexts:
        demand_index = max(_float_or_default(zone.get("demand_index"), 0.0), 0.1)
        forecast = max(_float_or_default(zone.get("forecast_demand_index_15m"), demand_index), 0.0)
        baseline = max(demand_index, 0.1)
        trend_pct = round(((forecast - baseline) / baseline) * 100.0, 1)
        traffic_factor = max(_float_or_default(zone.get("traffic_factor"), 1.0), 0.2)
        signal = "hausse" if trend_pct > 15 else "baisse" if trend_pct < -15 else "stable"
        action = (
            "Envoyer un livreur disponible" if signal == "hausse" else
            "Redéployer vers une zone plus active" if signal == "baisse" else
            "Maintenir la couverture actuelle"
        )
        zones.append(
            {
                "lat": float(zone["lat"]),
                "lon": float(zone["lon"]),
                "activite_recente": max(1, int(round(demand_index * 12))),
                "baseline_15min": round(baseline, 1),
                "vitesse_recente": round(max(8.0, 18.0 / traffic_factor), 1),
                "tendance_pct": trend_pct,
                "signal": signal,
                "action": action,
            }
        )
    zones.sort(key=lambda item: item["tendance_pct"], reverse=True)
    hot = [zone for zone in zones if zone["signal"] == "hausse"]
    cold = [zone for zone in zones if zone["signal"] == "baisse"]
    stable = [zone for zone in zones if zone["signal"] == "stable"]
    return {
        "horizon_minutes": horizon_minutes,
        "methode": "Projection live issue des signaux de zone Redis (demande, offre, trafic)",
        "nb_zones_analysees": len(zones),
        "resume": {
            "zones_en_hausse": len(hot),
            "zones_stables": len(stable),
            "zones_en_baisse": len(cold),
        },
        "dispatch_prioritaire": hot[:5],
        "zones_a_decouvrir": cold[:3],
        "toutes_zones": zones,
        "time_reference": _build_time_reference(
            ref_meta,
            ref_ts,
            recent_start,
            {"baseline_window_start": _dt_to_iso(baseline_start)},
        ),
    }


def _normalize_focus_trail_point(point: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(point, dict):
        return None
    try:
        lat = float(point.get("lat"))
        lon = float(point.get("lon"))
    except (TypeError, ValueError):
        return None
    speed_raw = point.get("speed_kmh", 0.0)
    try:
        speed = float(speed_raw or 0.0)
    except (TypeError, ValueError):
        speed = 0.0
    return {
        "ts": str(point.get("ts") or ""),
        "lat": lat,
        "lon": lon,
        "speed_kmh": speed,
        "status": _canonical_status(point.get("status")),
    }


def _merge_focus_trail_points(
    cold_points: list[dict[str, Any]] | None,
    hot_points: list[dict[str, Any]] | None,
    *,
    position: dict[str, Any] | None = None,
    max_points: int = 60,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []

    def _append(point: dict[str, Any] | None) -> None:
        normalized = _normalize_focus_trail_point(point)
        if normalized is None:
            return
        if merged:
            prev = merged[-1]
            prev_ts = _parse_iso_timestamp(prev.get("ts"))
            point_ts = _parse_iso_timestamp(normalized.get("ts"))
            if prev_ts is not None and point_ts is not None and point_ts < prev_ts:
                return
            same_coords = (
                abs(prev["lat"] - normalized["lat"]) < 1e-6
                and abs(prev["lon"] - normalized["lon"]) < 1e-6
            )
            same_ts = bool(normalized["ts"]) and normalized["ts"] == prev.get("ts")
            if same_coords or same_ts:
                prev.update({key: value for key, value in normalized.items() if value not in ("", None)})
                return
        merged.append(normalized)

    for point in cold_points or []:
        _append(point)
    for point in hot_points or []:
        _append(point)
    _append(position)

    if max_points > 0 and len(merged) > max_points:
        merged = merged[-max_points:]
    return merged


def _build_livreur_position(
    livreur_id: str,
    h: dict[str, Any],
    *,
    fallback_lat: float,
    fallback_lon: float,
    distance_km: float | None = None,
    recent_track: list[dict[str, Any]] | None = None,
) -> LivreurPosition:
    return LivreurPosition(
        livreur_id=livreur_id,
        lat=float(h.get("lat", fallback_lat)),
        lon=float(h.get("lon", fallback_lon)),
        speed_kmh=float(h.get("speed_kmh", 0)),
        heading_deg=float(h.get("heading_deg", 0)),
        status=_canonical_status(h.get("status")),
        accuracy_m=float(h.get("accuracy_m", 0)),
        battery_pct=float(h.get("battery_pct", 0)),
        ts=h.get("ts", ""),
        distance_km=distance_km,
        route_source=str(h.get("route_source") or ""),
        anomaly_state=str(h.get("anomaly_state") or "ok"),
        stale_reason=str(h.get("stale_reason") or ""),
        recent_track=recent_track or [],
    )


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


def _focus_trail_cache_key(driver_id: str, trail_points: int, window_end: datetime) -> str:
    # 5-second buckets are enough for a smooth focus animation while avoiding
    # repeated DuckDB scans from multiple concurrent UI polls.
    bucket = int(_coerce_datetime_utc(window_end).timestamp() // 5)
    return f"{driver_id}:{int(trail_points)}:{bucket}"


async def _focus_trail_cache_get(key: str) -> list[dict[str, Any]] | None:
    now = time.time()
    async with _focus_trail_cache_lock:
        entry = _focus_trail_cache.get(key)
        if not entry:
            return None
        expires_at, payload = entry
        if expires_at <= now:
            _focus_trail_cache.pop(key, None)
            return None
        _focus_trail_cache.move_to_end(key)
        return payload


async def _focus_trail_cache_set(key: str, payload: list[dict[str, Any]]) -> None:
    expires_at = time.time() + FOCUS_TRAIL_CACHE_TTL_SECONDS
    async with _focus_trail_cache_lock:
        _focus_trail_cache[key] = (expires_at, payload)
        _focus_trail_cache.move_to_end(key)
        while len(_focus_trail_cache) > FOCUS_TRAIL_CACHE_MAX:
            _focus_trail_cache.popitem(last=False)


def _scan_cold_path_snapshot_sync() -> dict[str, Any]:
    if not _path_exists_safe(DATA_PATH):
        return {
            "cold_parquet_files": 0,
            "cold_parquet_total_mb": 0.0,
        }
    total_size = 0
    counted = 0
    try:
        iterator = DATA_PATH.rglob("*.parquet")
        for f in iterator:
            try:
                total_size += int(f.stat().st_size)
                counted += 1
            except OSError:
                continue
    except OSError as exc:
        log.warning("cold path glob failed: %s", exc)
        counted = 0
        total_size = 0
    parquet_total_mb = round(float(total_size) / 1_048_576, 2)
    return {
        "cold_parquet_files": counted,
        "cold_parquet_total_mb": parquet_total_mb,
    }


async def _cold_path_snapshot_cached() -> dict[str, Any]:
    global _stats_cold_refresh_in_flight
    now = time.time()
    cached_payload: dict[str, Any] = {}
    async with _stats_cold_cache_lock:
        cached_payload = _stats_cold_cache.get("payload", {}) or {}
        if float(_stats_cold_cache.get("expires_at", 0.0)) > now and cached_payload:
            return cached_payload
        # Avoid thundering herd when many /stats calls hit the same refresh.
        if _stats_cold_refresh_in_flight:
            return cached_payload or {
                "cold_parquet_files": 0,
                "cold_parquet_total_mb": 0.0,
            }
        _stats_cold_refresh_in_flight = True

    try:
        payload = await asyncio.to_thread(_scan_cold_path_snapshot_sync)
        async with _stats_cold_cache_lock:
            _stats_cold_cache["payload"] = payload
            _stats_cold_cache["expires_at"] = time.time() + STATS_COLD_CACHE_TTL_SECONDS
            _stats_cold_refresh_in_flight = False
        return payload
    except Exception as exc:  # noqa: BLE001
        log.warning("cold path snapshot refresh failed: %s", exc)
        async with _stats_cold_cache_lock:
            _stats_cold_refresh_in_flight = False
            cached_payload = _stats_cold_cache.get("payload", {}) or {}
            if cached_payload:
                return cached_payload
        return {
            "cold_parquet_files": 0,
            "cold_parquet_total_mb": 0.0,
        }
    finally:
        # Defensive reset on cancellation/early exits.
        if _stats_cold_refresh_in_flight:
            async with _stats_cold_cache_lock:
                _stats_cold_refresh_in_flight = False


def _analytics_cache_key(name: str, **params: Any) -> str:
    normalized: dict[str, Any] = {}
    for key, value in sorted(params.items()):
        if isinstance(value, datetime):
            normalized[key] = _dt_to_iso(value)
        else:
            normalized[key] = value
    return f"{name}:{json.dumps(normalized, sort_keys=True, separators=(',', ':'), ensure_ascii=True)}"


async def _analytics_result_cache_get(
    key: str,
    *,
    allow_stale: bool = False,
) -> tuple[dict[str, Any], float] | None:
    now = time.time()
    async with _analytics_result_cache_lock:
        entry = _analytics_result_cache.get(key)
        if not entry:
            return None
        expires_at, cached_at, payload = entry
        if expires_at <= now and not allow_stale:
            return None
        _analytics_result_cache.move_to_end(key)
        return copy.deepcopy(payload), cached_at


async def _analytics_result_cache_set(key: str, payload: dict[str, Any]) -> None:
    now = time.time()
    expires_at = now + ANALYTICS_RESULT_CACHE_TTL_SECONDS
    async with _analytics_result_cache_lock:
        _analytics_result_cache[key] = (expires_at, now, copy.deepcopy(payload))
        _analytics_result_cache.move_to_end(key)
        while len(_analytics_result_cache) > ANALYTICS_RESULT_CACHE_MAX:
            _analytics_result_cache.popitem(last=False)


def _analytics_exception_detail(exc: Exception, *, fallback_label: str) -> str:
    if isinstance(exc, HTTPException):
        raw = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    else:
        raw = str(exc)
    message = " ".join((raw or "").split())
    if not message:
        return fallback_label
    if message.lower().startswith(fallback_label.lower()):
        return message
    return f"{fallback_label} {message}"


def _analytics_status_payload(
    payload: dict[str, Any],
    *,
    cold_path_available: bool,
    degraded: bool,
    source: str,
    detail: str = "",
    cached_at: float | None = None,
) -> dict[str, Any]:
    result = copy.deepcopy(payload)
    if not detail:
        result.pop("detail", None)
    result["cold_path_available"] = cold_path_available
    status: dict[str, Any] = {
        "source": source,
        "degraded": degraded,
        "cold_path_available": cold_path_available,
    }
    if detail:
        status["detail"] = detail
        result["detail"] = detail
    if cached_at is not None:
        cached_dt = datetime.fromtimestamp(cached_at, tz=timezone.utc)
        status["cached_at"] = _dt_to_iso(cached_dt)
        status["snapshot_age_s"] = int(max(0.0, time.time() - cached_at))
    result["analytics_status"] = status
    return result


def _analytics_should_degrade(exc: Exception) -> bool:
    return not isinstance(exc, HTTPException) or exc.status_code >= 500


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


async def _latest_hot_track_ts(redis_client: aioredis.Redis) -> datetime | None:
    courier_ids = (
        await _run_redis(
            redis_client.zrange(GEO_KEY, 0, 255),
            op_name="zrange latest-hot-track-ts",
            default=[],
            strict=False,
        ) or []
    )
    if not courier_ids:
        return None
    pipe = redis_client.pipeline(transaction=False)
    for courier_id in courier_ids:
        pipe.lrange(f"{TRACK_KEY_PREFIX}{courier_id}", 0, 0)
    payloads = await _run_redis(
        pipe.execute(),
        op_name="pipeline latest-hot-track-ts",
        default=[],
        strict=False,
    ) or []
    latest_ts: datetime | None = None
    for raw_items in payloads:
        points = _parse_recent_track(raw_items if isinstance(raw_items, list) else None)
        if not points:
            continue
        ts = _parse_iso_timestamp(str(points[-1].get("ts") or ""))
        if ts is None:
            continue
        if latest_ts is None or ts > latest_ts:
            latest_ts = ts
    return latest_ts


async def _align_live_reference_ts(
    ref_ts: datetime,
    ref_meta: dict[str, str],
    *,
    redis_client: aioredis.Redis,
) -> tuple[datetime, dict[str, str]]:
    if ref_meta.get("mode") not in {"parquet_max_ts", "system_now"}:
        return ref_ts, ref_meta
    hot_ts = await _latest_hot_track_ts(redis_client)
    if hot_ts is None:
        return ref_ts, ref_meta
    aligned = dict(ref_meta)
    aligned["mode"] = "hot_track_max_ts"
    aligned["source"] = "redis"
    aligned["fallback_from"] = ref_meta.get("mode", "")
    return hot_ts, aligned


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


def _history_resume_from_points(points: list[dict[str, Any]]) -> dict[str, Any]:
    if not points:
        return HistoryResume(
            nb_points=0,
            distance_totale_km=0.0,
            vitesse_moy_kmh=0.0,
            vitesse_max_kmh=0.0,
            statut_dominant="unknown",
            premiere_position=None,
            derniere_position=None,
        ).model_dump()
    speeds = [
        _float_or_default(point.get("speed_kmh"), 0.0)
        for point in points
        if point.get("speed_kmh") is not None
    ]
    statuses = [str(point.get("status") or "") for point in points if point.get("status")]
    dominant = max(set(statuses), key=statuses.count) if statuses else "unknown"
    total_km = 0.0
    for prev_point, point in zip(points, points[1:]):
        total_km += _haversine_km(
            float(prev_point.get("lat", 0.0) or 0.0),
            float(prev_point.get("lon", 0.0) or 0.0),
            float(point.get("lat", 0.0) or 0.0),
            float(point.get("lon", 0.0) or 0.0),
        )
    return HistoryResume(
        nb_points=len(points),
        distance_totale_km=round(total_km, 2),
        vitesse_moy_kmh=round(statistics.mean(speeds), 1) if speeds else 0.0,
        vitesse_max_kmh=round(max(speeds), 1) if speeds else 0.0,
        statut_dominant=dominant,
        premiere_position={
            "lat": points[0].get("lat"),
            "lon": points[0].get("lon"),
            "ts": str(points[0].get("ts") or ""),
        },
        derniere_position={
            "lat": points[-1].get("lat"),
            "lon": points[-1].get("lon"),
            "ts": str(points[-1].get("ts") or ""),
        },
    ).model_dump()


async def _history_live_fallback_payload(
    *,
    redis_client: aioredis.Redis,
    livreur_id: str,
    heures: int,
    ref_ts: datetime,
    ref_meta: dict[str, str],
) -> dict[str, Any] | None:
    aligned_ref_ts, aligned_ref_meta = await _align_live_reference_ts(
        ref_ts,
        ref_meta,
        redis_client=redis_client,
    )
    window_start = aligned_ref_ts - timedelta(hours=heures)
    raw_track = await _run_redis(
        redis_client.lrange(
            f"{TRACK_KEY_PREFIX}{livreur_id}",
            0,
            min(HISTORY_HOT_FALLBACK_TRACK_POINTS, 64) - 1,
        ),
        op_name="lrange history fallback",
        default=[],
        strict=False,
    ) or []
    recent_track = _parse_recent_track(raw_track if isinstance(raw_track, list) else None)
    track_window = _track_points_in_window(
        recent_track,
        window_start=window_start,
        window_end=aligned_ref_ts,
    )
    points = track_window or recent_track
    if not points:
        return None
    trajectory = [
        {
            "lat": point.get("lat"),
            "lon": point.get("lon"),
            "speed_kmh": point.get("speed_kmh"),
            "heading_deg": point.get("heading_deg", 0.0) if isinstance(point, dict) else 0.0,
            "status": point.get("status", "unknown"),
            "ts": str(point.get("ts") or ""),
            "route_source": point.get("route_source", ""),
            "anomaly_state": point.get("anomaly_state", "ok"),
        }
        for point in points
    ]
    return {
        "livreur_id": livreur_id,
        "heures": heures,
        "resume": _history_resume_from_points(points),
        "trajectory": trajectory,
        "time_reference": _build_time_reference(aligned_ref_meta, aligned_ref_ts, window_start),
    }


async def _driver_score_live_fallback_payload(
    *,
    redis_client: aioredis.Redis,
    livreur_id: str,
    heures: int,
    ref_ts: datetime,
    ref_meta: dict[str, str],
) -> dict[str, Any] | None:
    aligned_ref_ts, aligned_ref_meta = await _align_live_reference_ts(
        ref_ts,
        ref_meta,
        redis_client=redis_client,
    )
    window_start = aligned_ref_ts - timedelta(hours=heures)
    raw_track = await _run_redis(
        redis_client.lrange(
            f"{TRACK_KEY_PREFIX}{livreur_id}",
            0,
            min(HISTORY_HOT_FALLBACK_TRACK_POINTS, 64) - 1,
        ),
        op_name="lrange driver-score fallback",
        default=[],
        strict=False,
    ) or []
    recent_track = _parse_recent_track(raw_track if isinstance(raw_track, list) else None)
    track_window = _track_points_in_window(
        recent_track,
        window_start=window_start,
        window_end=aligned_ref_ts,
    )
    points = track_window or recent_track
    if not points:
        return None

    speeds = [
        _float_or_default(point.get("speed_kmh"), 0.0)
        for point in points
        if point.get("speed_kmh") is not None
    ]
    statuses = [str(point.get("status") or "idle") for point in points]
    nb_points = len(points)
    total = max(nb_points, 1)
    ticks_delivering = sum(1 for status in statuses if status == "delivering")
    ticks_available = sum(1 for status in statuses if status == "available")
    ticks_idle = sum(1 for status in statuses if status == "idle")
    exces_vitesse = sum(1 for speed in speeds if speed > 50.0)
    arrets_suspects = sum(
        1
        for point in points
        if _float_or_default(point.get("speed_kmh"), 0.0) < 1.0
        and str(point.get("status") or "") == "delivering"
    )
    distance_km = 0.0
    for prev_point, point in zip(points, points[1:]):
        distance_km += _haversine_km(
            float(prev_point.get("lat", 0.0) or 0.0),
            float(prev_point.get("lon", 0.0) or 0.0),
            float(point.get("lat", 0.0) or 0.0),
            float(point.get("lon", 0.0) or 0.0),
        )

    vitesse_moy = round(statistics.mean(speeds), 1) if speeds else 0.0
    vitesse_max = round(max(speeds), 1) if speeds else 0.0
    vitesse_std = round(statistics.pstdev(speeds), 2) if len(speeds) > 1 else 0.0
    taux_livraison = round(ticks_delivering / total * 100, 1)
    taux_disponible = round(ticks_available / total * 100, 1)
    taux_idle = round(ticks_idle / total * 100, 1)

    score_productivite = min(40, round(taux_livraison * 0.4, 1))
    penalite_vitesse = min(30, round(exces_vitesse / total * 100 * 1.5, 1))
    score_securite = round(30 - penalite_vitesse, 1)
    penalite_arrets = min(20, round(arrets_suspects / total * 100 * 2, 1))
    score_fiabilite = round(20 - penalite_arrets, 1)
    score_activite = round(max(0, 10 - taux_idle * 0.15), 1)
    score_total = round(
        max(0, score_productivite + score_securite + score_fiabilite + score_activite),
        1,
    )
    grade = (
        "A" if score_total >= 85 else
        "B" if score_total >= 70 else
        "C" if score_total >= 55 else
        "D" if score_total >= 40 else "E"
    )
    return {
        "livreur_id": livreur_id,
        "periode_heures": heures,
        "score_global": score_total,
        "grade": grade,
        "details_score": {
            "productivite": {
                "points": score_productivite,
                "max": 40,
                "detail": f"{taux_livraison}% du temps en livraison",
            },
            "securite": {
                "points": score_securite,
                "max": 30,
                "detail": f"{exces_vitesse} enregistrement(s) >50 km/h",
            },
            "fiabilite": {
                "points": score_fiabilite,
                "max": 20,
                "detail": f"{arrets_suspects} arrêt(s) suspects en livraison",
            },
            "activite": {
                "points": score_activite,
                "max": 10,
                "detail": f"{taux_idle}% du temps en idle",
            },
        },
        "metriques": {
            "distance_parcourue_km": round(distance_km, 2),
            "vitesse_moyenne_kmh": vitesse_moy,
            "vitesse_max_kmh": vitesse_max,
            "regularite_vitesse": vitesse_std,
            "taux_livraison_pct": taux_livraison,
            "taux_disponible_pct": taux_disponible,
            "taux_idle_pct": taux_idle,
            "exces_vitesse": exces_vitesse,
            "arrets_suspects": arrets_suspects,
        },
        "interpretation": (
            "Performance excellente — livreur exemplaire." if grade == "A" else
            "Bonne performance — quelques axes d'amélioration." if grade == "B" else
            "Performance correcte — suivi recommandé." if grade == "C" else
            "Performance insuffisante — entretien RH conseillé." if grade == "D" else
            "Performance critique — intervention requise."
        ),
        "time_reference": _build_time_reference(aligned_ref_meta, aligned_ref_ts, window_start),
    }


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
    lat:    float = Query(..., description="Latitude", examples=[40.7580]),
    lon:    float = Query(..., description="Longitude", examples=[-73.9855]),
    rayon:  float = Query(1.5, description="Rayon en km", ge=0.1, le=50.0),
    statut: Optional[Literal["available", "delivering", "pickup", "repositioning", "idle"]] = Query(None),
    limit:  int   = Query(50, ge=1, le=1000),
    track_points: int = Query(0, ge=0, le=64, description="Nombre de points recents a joindre"),
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

    track_payloads: list[list[str] | None]
    if track_points > 0:
        pipe = r.pipeline(transaction=False)
        for item in raw:
            pipe.lrange(f"{TRACK_KEY_PREFIX}{item[0]}", 0, track_points - 1)
        track_payloads = await _run_redis(
            pipe.execute(),
            op_name="pipeline recent-track livreurs-proches",
            default=[],
        ) or []
        if len(track_payloads) != len(raw):
            track_payloads = [None] * len(raw)
    else:
        track_payloads = [None] * len(raw)

    livreurs: list[LivreurPosition] = []
    for item, h, track_raw in zip(raw, hashes, track_payloads):
        name, dist, (glon, glat) = item
        if not h:
            continue
        normalized_status = _canonical_status(h.get("status"))
        if statut and _status_bucket(normalized_status) != statut:
            continue
        livreurs.append(
            _build_livreur_position(
                name,
                h,
                fallback_lat=float(glat),
                fallback_lon=float(glon),
                distance_km=round(dist, 3),
                recent_track=_parse_recent_track(track_raw if isinstance(track_raw, list) else None),
            )
        )

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
    routing_health_raw = raw.get("routing_health_json") or "{}"
    try:
        routing_health = json.loads(routing_health_raw)
        if not isinstance(routing_health, dict):
            routing_health = {}
    except Exception:
        routing_health = {}

    routing_degraded_raw = str(raw.get("routing_degraded", "0") or "0").strip().lower()
    routing_degraded = routing_degraded_raw in {"1", "true", "yes", "on"}
    routing_last_error = raw.get("routing_last_error") or ""
    is_healthy = (age_seconds is not None and age_seconds < 60.0) and bool(providers) and (not routing_degraded)

    return {
        "driver_id": raw.get("driver_id") or "",
        "state": raw.get("state") or "unknown",
        "scenario": TLC_SCENARIO,
        "focus_supported": TLC_SCENARIO == "single_driver",
        "virtual_time": raw.get("virtual_time") or "",
        "updated_at": updated_at_iso,
        "age_seconds": age_seconds,
        "is_healthy": is_healthy,
        "stale_reason": raw.get("stale_reason") or "",
        "routing_providers": providers,
        "routing_health": routing_health,
        "routing_degraded": routing_degraded,
        "routing_last_error": routing_last_error,
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
            "route_source": str(hot.get("route_source") or ""),
            "anomaly_state": str(hot.get("anomaly_state") or "ok"),
            "stale_reason": str(hot.get("stale_reason") or ""),
        }

    trail: list[dict] = []
    if trail_points > 0 and position is not None:
        hot_track_raw = await _run_redis(
            r.lrange(f"{TRACK_KEY_PREFIX}{driver_id}", 0, min(int(trail_points), 64) - 1),
            op_name=f"lrange focus-track {driver_id}",
            default=[],
            strict=False,
        ) or []
        hot_trail = _parse_recent_track(hot_track_raw if isinstance(hot_track_raw, list) else None)
        window_end = now_utc
        cache_key = _focus_trail_cache_key(driver_id, int(trail_points), window_end)
        cached_trail = await _focus_trail_cache_get(cache_key)
        if cached_trail is not None:
            trail = _merge_focus_trail_points(
                cached_trail,
                hot_trail,
                position=position,
                max_points=int(trail_points),
            )
        else:
            cold_trail: list[dict[str, Any]] = []
            dynamic_lookback_min = max(
                10,
                min(FOCUS_TRAIL_LOOKBACK_MIN, int(math.ceil(float(trail_points) / 3.0))),
            )
            window_start = window_end - timedelta(minutes=dynamic_lookback_min)
            scan_sql = _parquet_scan_sql(window_start, window_end, max_hours=FOCUS_TRAIL_MAX_HOURS)
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
                    timeout_sec=FOCUS_TRAIL_QUERY_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                log.warning("focus trail failed for %s: %s", driver_id, exc)
                rows = []
            for ts_val, lat_v, lon_v, speed_v, status_v in reversed(rows):
                if lat_v is None or lon_v is None:
                    continue
                cold_trail.append(
                    {
                        "ts": ts_val.isoformat() if isinstance(ts_val, datetime) else str(ts_val),
                        "lat": float(lat_v),
                        "lon": float(lon_v),
                        "speed_kmh": float(speed_v or 0.0),
                        "status": _canonical_status(status_v),
                    }
                )
            trail = _merge_focus_trail_points(
                cold_trail,
                hot_trail,
                position=position,
                max_points=int(trail_points),
            )
            await _focus_trail_cache_set(cache_key, cold_trail)

    replay_status = await _run_redis(
        r.hgetall("copilot:replay:tlc:single:status"),
        op_name="hgetall single status",
        default={},
        strict=False,
    ) or {}

    return {
        "driver_id": driver_id,
        "scenario": TLC_SCENARIO,
        "focus_supported": TLC_SCENARIO == "single_driver",
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
    raw_track = await _run_redis(
        r.lrange(f"{TRACK_KEY_PREFIX}{livreur_id}", 0, 5),
        op_name=f"lrange recent-track {livreur_id}",
        default=[],
        strict=False,
    )
    return _build_livreur_position(
        livreur_id,
        h,
        fallback_lat=0.0,
        fallback_lon=0.0,
        recent_track=_parse_recent_track(raw_track if isinstance(raw_track, list) else None),
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
    status_counts = _status_bucket_counts_template()
    status_counts_detail = _status_detail_counts_template()
    status_sampled = 0
    livreur_ids: list[str] = []
    if redis_available and total_geo > 0:
        zrange_end = total_geo - 1
        if STATS_STATUS_SCAN_LIMIT > 0:
            zrange_end = min(zrange_end, STATS_STATUS_SCAN_LIMIT - 1)
        if zrange_end >= 0:
            livreur_ids = (
                await _run_redis(
                    r.zrange(GEO_KEY, 0, zrange_end),
                    op_name="zrange stats",
                    default=[],
                    strict=False,
                ) or []
            )
            status_sampled = len(livreur_ids)

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
            detail_key = canonical if canonical in status_counts_detail else "unknown"
            status_counts_detail[detail_key] += 1
            bucket = _status_bucket(canonical)
            if bucket in status_counts:
                status_counts[bucket] += 1
    if status_sampled > 0 and total_geo > status_sampled:
        status_counts = _scale_count_map(status_counts, total_geo, status_sampled)
        status_counts_detail = _scale_count_map(status_counts_detail, total_geo, status_sampled)

    cold_snapshot = await _cold_path_snapshot_cached()
    parquet_files_count = int(cold_snapshot.get("cold_parquet_files", 0) or 0)
    parquet_total_mb = float(cold_snapshot.get("cold_parquet_total_mb", 0.0) or 0.0)

    return {
        # Backward-compatible top-level KPIs used by smoke/readiness scripts.
        "active_couriers": total_geo,
        "messages_processed": total_msgs,
        "status_counts": status_counts,
        "status_counts_detail": status_counts_detail,
        "status_counts_sampled": status_sampled,
        "redis_available": redis_available,
        "cold_parquet_files": parquet_files_count,
        "cold_parquet_total_mb": parquet_total_mb,
        "hot_path": {
            "livreurs_actifs":    total_geo,
            "messages_traites":   total_msgs,
            "statuts":            status_counts,
            "statuts_detail":     status_counts_detail,
            "backend":            "Redis Stack (GEOSEARCH)",
            "ttl_secondes":       HOT_PATH_TTL_SECONDS,
            "redis_available":    redis_available,
        },
        "cold_path": {
            "fichiers_parquet":   parquet_files_count,
            "taille_totale_mb":   parquet_total_mb,
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
    r: aioredis.Redis = app.state.redis
    cache_key = _analytics_cache_key(
        "history",
        livreur_id=livreur_id,
        heures=int(heures),
        reference_ts=(reference_ts or ""),
    )
    fresh_entry = await _analytics_result_cache_get(cache_key, allow_stale=False)
    if fresh_entry:
        return fresh_entry[0]
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None
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
            timeout_sec=ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS,
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
            timeout_sec=ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS,
        )
        dist_row = dist_rows[0] if dist_rows else (0.0,)

    except HTTPException as exc:
        if exc.status_code == 404:
            live_payload = await _history_live_fallback_payload(
                redis_client=r,
                livreur_id=livreur_id,
                heures=heures,
                ref_ts=ref_ts,
                ref_meta=ref_meta,
            )
            if live_payload is not None:
                payload = _analytics_status_payload(
                    live_payload,
                    cold_path_available=False,
                    degraded=False,
                    source="hot_path_live",
                )
                await _analytics_result_cache_set(cache_key, payload)
                return payload
        if not _analytics_should_degrade(exc):
            raise
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        live_payload = await _history_live_fallback_payload(
            redis_client=r,
            livreur_id=livreur_id,
            heures=heures,
            ref_ts=ref_ts,
            ref_meta=ref_meta,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="hot_path_live",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        detail = _analytics_exception_detail(
            exc,
            fallback_label="Historique trajectoire temporairement indisponible.",
        )
        return _analytics_status_payload(
            {
                "livreur_id": livreur_id,
                "heures": heures,
                "resume": _history_resume_from_points([]),
                "trajectory": [],
                "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
            },
            cold_path_available=False,
            degraded=True,
            source="cold_path_fallback",
            detail=detail,
        )
    except Exception as exc:
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        live_payload = await _history_live_fallback_payload(
            redis_client=r,
            livreur_id=livreur_id,
            heures=heures,
            ref_ts=ref_ts,
            ref_meta=ref_meta,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="hot_path_live",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        detail = _analytics_exception_detail(
            exc,
            fallback_label="Historique trajectoire temporairement indisponible.",
        )
        return _analytics_status_payload(
            {
                "livreur_id": livreur_id,
                "heures": heures,
                "resume": _history_resume_from_points([]),
                "trajectory": [],
                "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
            },
            cold_path_available=False,
            degraded=True,
            source="cold_path_fallback",
            detail=detail,
        )

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

    payload = {
        "livreur_id": livreur_id,
        "heures": heures,
        "resume": resume.model_dump(),
        "trajectory": trajectory,
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
    }
    payload = _analytics_status_payload(
        payload,
        cold_path_available=True,
        degraded=False,
        source="live",
    )
    await _analytics_result_cache_set(cache_key, payload)
    return payload


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
            timeout_sec=ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS,
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
    summary="Benchmark Redis live - Preuve SLA <10ms",
    tags=["Monitoring"],
)
async def performance_check(
    samples: int = Query(200, description="Nombre de GEOSEARCH a mesurer", ge=10, le=1000),
):
    """
    Lance N requetes GEOSEARCH contre Redis et retourne les percentiles.
    Utilise pour prouver que le Hot Path respecte le SLA <10ms (p99).
    """
    requested_samples = int(samples)
    effective_samples = max(10, min(requested_samples, PERF_BENCH_MAX_SAMPLES))
    now = time.time()
    async with _perf_bench_cache_lock:
        cached = _perf_bench_cache.get(effective_samples)
        if cached and cached[0] > now:
            return cached[1]

    r: aioredis.Redis = app.state.redis

    # Warm-up (5 requetes ignorees)
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
    for _ in range(effective_samples):
        t0 = time.perf_counter()
        await _run_redis(
            r.geosearch(
                GEO_KEY,
                longitude=-73.9855,
                latitude=40.7580,
                radius=20,
                unit="km",
                withcoord=False,
                withdist=False,
                count=100,
            ),
            op_name="geosearch performance sample",
        )
        latencies_ms.append((time.perf_counter() - t0) * 1000)

    info = await _run_redis(
        r.info("all"),
        op_name="info performance",
    )

    sla_ok = _percentile(latencies_ms, 99) < 10.0

    result = {
        "sla_hot_path_ok": sla_ok,
        "sla_target": "<10ms (p99)",
        "geosearch_benchmark": {
            "samples": effective_samples,
            "samples_requested": requested_samples,
            "p50_ms": _percentile(latencies_ms, 50),
            "p95_ms": _percentile(latencies_ms, 95),
            "p99_ms": _percentile(latencies_ms, 99),
            "mean_ms": round(statistics.mean(latencies_ms), 3),
            "max_ms": round(max(latencies_ms), 3),
        },
        "redis_info": {
            "version": info.get("redis_version"),
            "connected_clients": info.get("connected_clients"),
            "used_memory_human": info.get("used_memory_human"),
            "ops_per_sec": info.get("instantaneous_ops_per_sec"),
            "total_commands": info.get("total_commands_processed"),
            "keyspace_hits": info.get("keyspace_hits"),
            "keyspace_misses": info.get("keyspace_misses"),
        },
        "architecture": "Lambda Architecture (Speed Layer + Batch Layer)",
    }

    async with _perf_bench_cache_lock:
        _perf_bench_cache[effective_samples] = (
            time.time() + PERF_BENCH_CACHE_TTL_SECONDS,
            result,
        )
    return result


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
    ref_ts, ref_meta = await _align_live_reference_ts(ref_ts, ref_meta, redis_client=r)
    window_start = ref_ts - timedelta(hours=heures)
    scan_sql = _parquet_scan_sql(window_start, ref_ts)
    cache_key = _analytics_cache_key(
        "fleet-insights",
        heures=int(heures),
        reference_ts=(reference_ts or ""),
    )
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None

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
    status_counts = _status_bucket_counts_template()
    status_counts_detail = _status_detail_counts_template()
    idle_suspects: list[dict] = []
    low_battery: list[dict] = []
    results: list[list[Any] | tuple[Any, ...]] = []
    hot_speed_samples: list[float] = []
    hot_sample_count = 0
    hot_delivering_samples = 0
    hot_speed_alerts = 0

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
        pipe = r.pipeline(transaction=False)
        for lid in livreur_ids:
            pipe.lrange(f"{TRACK_KEY_PREFIX}{lid}", 0, FLEET_INSIGHTS_TRACK_POINTS - 1)
        track_payloads = await _run_redis(
            pipe.execute(),
            op_name="pipeline recent-track fleet-insights",
            default=[],
            strict=False,
        ) or []
        if len(track_payloads) != len(livreur_ids):
            track_payloads = [None] * len(livreur_ids)
        for lid, vals, track_raw in zip(livreur_ids, results, track_payloads):
            status, speed, ts, lat, lon, battery = vals
            canonical_status = _canonical_status(status)
            detail_key = canonical_status if canonical_status in status_counts_detail else "unknown"
            status_counts_detail[detail_key] += 1
            bucket = _status_bucket(canonical_status)
            if bucket in status_counts:
                status_counts[bucket] += 1
            try:
                recent_track = _parse_recent_track(track_raw if isinstance(track_raw, list) else None)
                track_window = _track_points_in_window(
                    recent_track,
                    window_start=window_start,
                    window_end=ref_ts,
                )
                for point in track_window:
                    point_speed = float(point.get("speed_kmh", 0.0) or 0.0)
                    hot_speed_samples.append(point_speed)
                    hot_sample_count += 1
                    if _canonical_status(point.get("status")) == "delivering":
                        hot_delivering_samples += 1
                    if point_speed > ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH:
                        hot_speed_alerts += 1
                stationary_seconds, stationary_samples, frozen = _live_stationary_issue(
                    track_window or recent_track,
                    speed_threshold_kmh=FLEET_INSIGHTS_STATIONARY_SPEED_KMH,
                    min_duration_s=FLEET_INSIGHTS_STATIONARY_MIN_SECONDS,
                )
                if (
                    canonical_status == "delivering"
                    and stationary_seconds >= FLEET_INSIGHTS_STATIONARY_MIN_SECONDS
                ):
                    idle_suspects.append({
                        "livreur_id": lid,
                        "status": canonical_status,
                        "speed_kmh": round(float(speed or 0), 1),
                        "ts": ts,
                        "immobile_duration_s": int(round(stationary_seconds)),
                        "immobile_duration_min": round(stationary_seconds / 60.0, 1),
                        "samples": stationary_samples,
                        "reason": (
                            "gps_frozen"
                            if frozen is not None
                            else "routing_hold"
                        ),
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

    active_work_count = (
        status_counts["delivering"]
        + status_counts["pickup"]
        + status_counts["repositioning"]
    )
    utilisation_pct = round(
        status_counts["delivering"] / total_actifs * 100 if total_actifs else 0, 1
    )
    engagement_pct = round(
        active_work_count / total_actifs * 100 if total_actifs else 0, 1
    )
    pickup_pct = round(
        status_counts["pickup"] / total_actifs * 100 if total_actifs else 0, 1
    )
    repositioning_pct = round(
        status_counts["repositioning"] / total_actifs * 100 if total_actifs else 0, 1
    )
    disponibilite_pct = round(
        status_counts["available"] / total_actifs * 100 if total_actifs else 0, 1
    )
    inactivite_pct = round(
        status_counts["idle"] / total_actifs * 100 if total_actifs else 0, 1
    )
    if not hot_speed_samples and results:
        for vals in results:
            if not vals:
                continue
            raw_status = vals[0] if len(vals) > 0 else None
            raw_speed = vals[1] if len(vals) > 1 else 0.0
            speed_value = float(raw_speed or 0.0)
            hot_speed_samples.append(speed_value)
            if _canonical_status(raw_status) == "delivering":
                hot_delivering_samples += 1
            if speed_value > ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH:
                hot_speed_alerts += 1
        hot_sample_count = len(hot_speed_samples)

    live_productivity_stats = {
        "livreurs_actifs_periode": total_actifs,
        "vitesse_moyenne_kmh": round(statistics.mean(hot_speed_samples), 1) if hot_speed_samples else 0.0,
        "taux_livraison_pct": (
            round(hot_delivering_samples / hot_sample_count * 100, 1)
            if hot_sample_count
            else utilisation_pct
        ),
        "alertes_vitesse_exces": hot_speed_alerts,
        "stops_suspects_total": len(idle_suspects),
    }

    # ── Cold Path : productivité historique ──────────────────────────────────
    cold_stats = {
        "livreurs_actifs_periode": 0,
        "vitesse_moyenne_kmh": 0.0,
        "taux_livraison_pct": 0.0,
        "alertes_vitesse_exces": 0,
        "stops_suspects_total": 0,
    }
    cold_path_available = False
    analytics_degraded = False
    analytics_source = "live"
    analytics_detail = ""
    try:
        rows = await _dq(
            f"""
            SELECT
                COUNT(DISTINCT livreur_id)                              AS nb_livreurs,
                ROUND(AVG(speed_kmh), 1)                                AS vitesse_moy,
                ROUND(AVG(CASE WHEN status = 'delivering' THEN 1.0 ELSE 0.0 END) * 100, 1)
                                                                        AS taux_livraison_pct,
                COUNT(*) FILTER (WHERE speed_kmh > ?)                   AS alertes_vitesse,
                COUNT(*) FILTER (WHERE speed_kmh < 1 AND status = 'delivering')
                                                                        AS stops_suspects
            FROM {scan_sql}
            WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
            """,
            [
                ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH,
                _dt_to_iso(window_start),
                _dt_to_iso(ref_ts),
            ],
            timeout_sec=ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS,
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
        else:
            cold_stats = copy.deepcopy(live_productivity_stats)
            analytics_source = "hot_live_blended"
    except Exception as exc:
        log.warning("fleet-insights cold path error: %s", exc)
        if cached_payload and isinstance(cached_payload.get("productivite_historique"), dict):
            cold_stats = copy.deepcopy(cached_payload["productivite_historique"])
            analytics_source = "stale_cache_blended"
        else:
            cold_stats = copy.deepcopy(live_productivity_stats)
            analytics_source = "hot_live_fallback"

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

    payload = {
        "periode_heures": heures,
        "sante_operationnelle": {
            "score": score,
            "niveau": "excellent" if score >= 85 else "bon" if score >= 65 else "attention" if score >= 45 else "critique",
        },
        "flotte_temps_reel": {
            "total_actifs":        total_actifs,
            "statuts":             status_counts,
            "statuts_detail":      status_counts_detail,
            "taux_utilisation_pct": utilisation_pct,
            "taux_engagement_pct": engagement_pct,
            "taux_pickup_pct":     pickup_pct,
            "taux_reposition_pct": repositioning_pct,
            "taux_disponibilite_pct": disponibilite_pct,
            "taux_inactivite_pct": inactivite_pct,
            "redis_available":     redis_available,
        },
        "alertes": {
            "livreurs_immobiles_en_livraison": len(idle_suspects),
            "detail_suspects": sorted(
                idle_suspects,
                key=lambda item: item.get("immobile_duration_s", 0),
                reverse=True,
            )[:5],
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
    payload = _analytics_status_payload(
        payload,
        cold_path_available=cold_path_available,
        degraded=analytics_degraded,
        source=analytics_source,
        detail=analytics_detail,
        cached_at=cached_at if analytics_source.startswith("stale_cache") else None,
    )
    if cold_path_available:
        await _analytics_result_cache_set(cache_key, payload)
    return payload


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
            f"{speed_alerts} enregistrements à vitesse excessive (>{ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH:.0f} km/h) — "
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
    r: aioredis.Redis = app.state.redis
    cache_key = _analytics_cache_key(
        "driver-score",
        livreur_id=livreur_id,
        heures=int(heures),
        reference_ts=(reference_ts or ""),
    )
    fresh_entry = await _analytics_result_cache_get(cache_key, allow_stale=False)
    if fresh_entry:
        return fresh_entry[0]
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None
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
            timeout_sec=ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS,
        )
        row = rows[0] if rows else None
    except HTTPException as exc:
        if exc.status_code == 404:
            live_payload = await _driver_score_live_fallback_payload(
                redis_client=r,
                livreur_id=livreur_id,
                heures=heures,
                ref_ts=ref_ts,
                ref_meta=ref_meta,
            )
            if live_payload is not None:
                payload = _analytics_status_payload(
                    live_payload,
                    cold_path_available=False,
                    degraded=False,
                    source="hot_path_live",
                )
                await _analytics_result_cache_set(cache_key, payload)
                return payload
        if not _analytics_should_degrade(exc):
            raise
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        live_payload = await _driver_score_live_fallback_payload(
            redis_client=r,
            livreur_id=livreur_id,
            heures=heures,
            ref_ts=ref_ts,
            ref_meta=ref_meta,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="hot_path_live",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        detail = _analytics_exception_detail(
            exc,
            fallback_label="Score livreur temporairement indisponible.",
        )
        return _analytics_status_payload(
            {
                "livreur_id": livreur_id,
                "periode_heures": heures,
                "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
            },
            cold_path_available=False,
            degraded=True,
            source="cold_path_fallback",
            detail=detail,
        )
    except Exception as exc:
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        live_payload = await _driver_score_live_fallback_payload(
            redis_client=r,
            livreur_id=livreur_id,
            heures=heures,
            ref_ts=ref_ts,
            ref_meta=ref_meta,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="hot_path_live",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        detail = _analytics_exception_detail(
            exc,
            fallback_label="Score livreur temporairement indisponible.",
        )
        return _analytics_status_payload(
            {
                "livreur_id": livreur_id,
                "periode_heures": heures,
                "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
            },
            cold_path_available=False,
            degraded=True,
            source="cold_path_fallback",
            detail=detail,
        )

    if not row or row[0] == 0:
        live_payload = await _driver_score_live_fallback_payload(
            redis_client=r,
            livreur_id=livreur_id,
            heures=heures,
            ref_ts=ref_ts,
            ref_meta=ref_meta,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="hot_path_live",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
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

    payload = {
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
    payload = _analytics_status_payload(
        payload,
        cold_path_available=True,
        degraded=False,
        source="live",
    )
    await _analytics_result_cache_set(cache_key, payload)
    return payload


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
    cache_key = _analytics_cache_key(
        "zone-coverage",
        resolution=round(float(resolution), 6),
        heures=int(heures),
        reference_ts=(reference_ts or ""),
    )
    fresh_entry = await _analytics_result_cache_get(cache_key, allow_stale=False)
    if fresh_entry:
        return fresh_entry[0]
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None

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
    cold_path_error: Exception | None = None
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
        cold_path_error = exc

    if cold_path_error is not None and cached_payload:
        return _analytics_status_payload(
            cached_payload,
            cold_path_available=False,
            degraded=False,
            source="stale_cache",
            detail="",
            cached_at=cached_at,
        )

    if not demand:
        live_zone_contexts = await _load_live_zone_context_payloads(r)
        live_payload = _build_zone_coverage_live_fallback(
            zone_contexts=live_zone_contexts,
            supply=supply,
            resolution=resolution,
            heures=heures,
            ref_meta=ref_meta,
            ref_ts=ref_ts,
            window_start=window_start,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="live_zone_context",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        payload = {
            "periode_heures": heures,
            "resolution_deg": resolution,
            "nb_zones_analysees": 0,
            "resume": {
                "zones_sous_couvertes": 0,
                "zones_sur_couvertes": 0,
                "zones_equilibrees": 0,
            },
            "alertes_dispatch": {
                "zones_prioritaires": [],
                "zones_saturees": [],
            },
            "toutes_zones": [],
            "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
        }
        detail = (
            _analytics_exception_detail(
                cold_path_error,
                fallback_label="Données historiques indisponibles pour la couverture territoriale.",
            )
            if cold_path_error is not None
            else "Aucune donnée historique exploitable sur la fenêtre demandée."
        )
        return _analytics_status_payload(
            payload,
            cold_path_available=False,
            degraded=(cold_path_error is not None),
            source="hot_only_fallback" if cold_path_error is not None else "cold_path_empty",
            detail=detail,
        )

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

    payload = {
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
    payload = _analytics_status_payload(
        payload,
        cold_path_available=True,
        degraded=False,
        source="live",
    )
    await _analytics_result_cache_set(cache_key, payload)
    return payload


# ────────────────────────────────────────────────────────────────────────────────

@app.get(
    "/analytics/anomalies",
    summary="Détection d'anomalies comportementales — sécurité & opérationnel",
    tags=["Business Intelligence"],
)
async def detect_anomalies(
    fenetre_minutes: int = Query(10, description="Fenêtre d'analyse en minutes", ge=5, le=60),
    seuil_vitesse:   float = Query(ANOMALY_DEFAULT_SPEED_THRESHOLD_KMH, description="Seuil excès de vitesse (km/h)", ge=20.0, le=120.0),
    seuil_immobile:  float = Query(2.0, description="Vitesse max pour considérer un livreur immobile (km/h)", ge=0.5, le=10.0),
    reference_ts: str | None = Query(
        None,
        description="Horodatage ISO-8601 de reference (UTC). Defaut: horloge replay TLC.",
    ),
):
    """
    Analyse statistique du comportement de chaque livreur sur les N dernières minutes.
    Détecte 3 types d'anomalies :

    - VITESSE_EXCESSIVE : dépassement du seuil configurable (défaut 65 km/h)
    - IMMOBILISATION_SUSPECTE : livreur en livraison réelle quasi à l'arrêt
      pendant une durée minimale configurable (défaut 2 min)
    - DEVIATION_COMPORTEMENTALE : vitesse actuelle dévie fortement de sa propre baseline (z-score)

    Cas d'usage : sécurité routière, détection pannes/accidents, conformité assurance.
    """
    r: aioredis.Redis = app.state.redis
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    ref_ts, ref_meta = await _align_live_reference_ts(ref_ts, ref_meta, redis_client=r)
    baseline_start = ref_ts - timedelta(minutes=fenetre_minutes * 2)
    analysis_start = ref_ts - timedelta(minutes=fenetre_minutes)
    cache_key = _analytics_cache_key(
        "anomalies",
        fenetre_minutes=int(fenetre_minutes),
        seuil_vitesse=round(float(seuil_vitesse), 3),
        seuil_immobile=round(float(seuil_immobile), 3),
        reference_ts=(reference_ts or ""),
    )
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None
    redis_available = bool(
        await _run_redis(
            r.ping(),
            op_name="ping anomalies",
            default=False,
            strict=False,
        )
    )
    if not redis_available:
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        fallback_payload = {
            "fenetre_minutes": fenetre_minutes,
            "seuils": {
                "vitesse_excessive_kmh": seuil_vitesse,
                "vitesse_excessive_min_occurrences": ANOMALY_SPEED_MIN_EXCEEDANCES,
                "immobilisation_kmh_max": seuil_immobile,
                "immobilisation_duree_min_secondes": ANOMALY_STATIONARY_MIN_SECONDS,
                "z_score_deviation": ANOMALY_ZSCORE_THRESHOLD,
            },
            "resume": {
                "livreurs_scannes": 0,
                "anomalies_detectees": 0,
                "critiques": 0,
                "warnings": 0,
            },
            "anomalies": [],
            "methodologie": (
                "Analyse live sur les trajectoires Redis validées. Les points dégradés "
                "ou bloqués sont exclus de la baseline et seuls les signaux réellement "
                "suspicious remontent."
            ),
            "time_reference": _build_time_reference(
                ref_meta,
                ref_ts,
                analysis_start,
                {"baseline_window_start": _dt_to_iso(baseline_start)},
            ),
        }
        return _analytics_status_payload(
            fallback_payload,
            cold_path_available=False,
            degraded=True,
            source="hot_path_unavailable",
            detail="Analyse des anomalies live temporairement indisponible.",
        )

    livreur_ids = (
        await _run_redis(
            r.zrange(GEO_KEY, 0, -1),
            op_name="zrange anomalies",
            default=[],
            strict=False,
        ) or []
    )
    track_payloads: list[Any] = []
    if livreur_ids:
        pipe = r.pipeline(transaction=False)
        for lid in livreur_ids:
            pipe.lrange(f"{TRACK_KEY_PREFIX}{lid}", 0, FLEET_INSIGHTS_TRACK_POINTS - 1)
        track_payloads = await _run_redis(
            pipe.execute(),
            op_name="pipeline recent-track anomalies",
            default=[],
            strict=False,
        ) or []
    if len(track_payloads) != len(livreur_ids):
        track_payloads = [None] * len(livreur_ids)

    anomalies: list[dict[str, Any]] = []
    total_scanned = 0
    for lid, track_raw in zip(livreur_ids, track_payloads):
        recent_track = _parse_recent_track(track_raw if isinstance(track_raw, list) else None)
        track_window = _track_points_in_window(
            recent_track,
            window_start=baseline_start,
            window_end=ref_ts,
        )
        analysis_points = [
            point for point in track_window
            if isinstance(point.get("_parsed_ts"), datetime) and point["_parsed_ts"] >= analysis_start
        ]
        if not analysis_points:
            continue
        total_scanned += 1

        trusted_motion = _trusted_live_motion_points(
            track_window,
            speed_cap_kmh=ANOMALY_BASELINE_SPEED_CAP_KMH,
        )
        baseline_motion = [
            point for point in trusted_motion
            if isinstance(point.get("_parsed_ts"), datetime) and point["_parsed_ts"] < analysis_start
        ]
        recent_motion = [
            point for point in trusted_motion
            if isinstance(point.get("_parsed_ts"), datetime) and point["_parsed_ts"] >= analysis_start
        ]
        recent_speeds = [
            _float_or_default(point.get("speed_kmh"), 0.0)
            for point in recent_motion
        ]
        baseline_speeds = [
            _float_or_default(point.get("speed_kmh"), 0.0)
            for point in baseline_motion
        ]

        speed_max = max(recent_speeds) if recent_speeds else 0.0
        speed_mean = statistics.mean(recent_speeds) if recent_speeds else 0.0
        nb_exces = sum(1 for speed in recent_speeds if speed > seuil_vitesse)
        stationary_seconds, stationary_samples, frozen = _live_stationary_issue(
            analysis_points,
            speed_threshold_kmh=seuil_immobile,
            min_duration_s=ANOMALY_STATIONARY_MIN_SECONDS,
        )
        z_score = 0.0
        if len(baseline_speeds) >= 4 and len(recent_speeds) >= 2:
            baseline_std = statistics.pstdev(baseline_speeds)
            if baseline_std >= 2.0:
                z_score = abs(statistics.mean(recent_speeds) - statistics.mean(baseline_speeds)) / baseline_std

        anomaly_types: list[dict[str, Any]] = []
        level = "info"
        if nb_exces >= ANOMALY_SPEED_MIN_EXCEEDANCES:
            anomaly_types.append(
                {
                    "type": "VITESSE_EXCESSIVE",
                    "detail": (
                        f"Vitesse max {round(speed_max, 1)} km/h — "
                        f"{nb_exces} dépassement(s) du seuil {seuil_vitesse} km/h"
                    ),
                    "risque": "Sécurité routière, responsabilité assurance",
                }
            )
            level = "critique" if speed_max > max(seuil_vitesse + 10.0, 80.0) else "warning"
        if stationary_seconds >= ANOMALY_STATIONARY_MIN_SECONDS:
            anomaly_types.append(
                {
                    "type": "IMMOBILISATION_SUSPECTE",
                    "detail": (
                        "Livreur en livraison réelle quasi à l'arrêt "
                        f"pendant {round(float(stationary_seconds) / 60.0, 1)} min "
                        f"({stationary_samples} point(s) sous {seuil_immobile} km/h)"
                    ),
                    "risque": (
                        "GPS figé probable sur flux live validé"
                        if frozen is not None
                        else "Routage dégradé ou progression interrompue"
                    ),
                }
            )
            level = "critique"
        if z_score > ANOMALY_ZSCORE_THRESHOLD:
            anomaly_types.append(
                {
                    "type": "DEVIATION_COMPORTEMENTALE",
                    "detail": (
                        f"Comportement anormal — z-score {round(z_score, 2)} "
                        f"(>{ANOMALY_ZSCORE_THRESHOLD} = déviation significative)"
                    ),
                    "risque": "Variation de comportement inhabituelle à vérifier",
                }
            )
            if level == "info":
                level = "warning"

        if anomaly_types:
            anomalies.append(
                {
                    "livreur_id": lid,
                    "niveau": level,
                    "anomalies": anomaly_types,
                    "vitesse_max_kmh": round(speed_max, 1),
                    "vitesse_moy_kmh": round(speed_mean, 1),
                    "z_score": round(z_score, 2),
                    "immobile_duration_s": int(round(float(stationary_seconds or 0.0))),
                    "derniere_position": str(analysis_points[-1].get("ts") or ""),
                }
            )

    nb_critique = sum(1 for anomaly in anomalies if anomaly["niveau"] == "critique")
    nb_warning = sum(1 for anomaly in anomalies if anomaly["niveau"] == "warning")

    payload = {
        "fenetre_minutes": fenetre_minutes,
        "seuils": {
            "vitesse_excessive_kmh": seuil_vitesse,
            "vitesse_excessive_min_occurrences": ANOMALY_SPEED_MIN_EXCEEDANCES,
            "immobilisation_kmh_max": seuil_immobile,
            "immobilisation_duree_min_secondes": ANOMALY_STATIONARY_MIN_SECONDS,
            "z_score_deviation": ANOMALY_ZSCORE_THRESHOLD,
        },
        "resume": {
            "livreurs_scannes": total_scanned,
            "anomalies_detectees": len(anomalies),
            "critiques": nb_critique,
            "warnings": nb_warning,
        },
        "anomalies": anomalies,
        "methodologie": (
            "Analyse live sur les trajectoires Redis validées. Les pauses normales "
            "de trafic/feux restent ignorées, les points dégradés sortent de la baseline, "
            f"et il faut au moins {ANOMALY_SPEED_MIN_EXCEEDANCES} dépassements vitesse "
            "pour remonter une alerte."
        ),
        "time_reference": _build_time_reference(
            ref_meta,
            ref_ts,
            analysis_start,
            {"baseline_window_start": _dt_to_iso(baseline_start)},
        ),
    }
    payload = _analytics_status_payload(
        payload,
        cold_path_available=False,
        degraded=False,
        source="hot_path_live",
    )
    await _analytics_result_cache_set(cache_key, payload)
    return payload


# ────────────────────────────────────────────────────────────────────────────────

@app.get(
    "/analytics/gps-fraud",
    summary="Qualité GPS live — positions figées et sauts bloqués",
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
    Analyse live de la qualité GPS sur les positions validées du Hot Path.

    Les sauts physiquement impossibles sont désormais bloqués en amont par le
    consumer Redis, donc cet endpoint n'expose plus de téléportations visibles.
    Il remonte seulement les GPS figés crédibles encore observables sur le flux
    live validé.
    """
    r: aioredis.Redis = app.state.redis
    ref_ts, ref_meta = await _resolve_reference_ts(reference_ts)
    ref_ts, ref_meta = await _align_live_reference_ts(ref_ts, ref_meta, redis_client=r)
    window_start = ref_ts - timedelta(minutes=fenetre_minutes)
    cache_key = _analytics_cache_key(
        "gps-fraud",
        fenetre_minutes=int(fenetre_minutes),
        seuil_teleport_km=round(float(seuil_teleport_km), 3),
        vitesse_max_physique_kmh=round(float(vitesse_max_physique_kmh), 3),
        reference_ts=(reference_ts or ""),
    )
    fresh_entry = await _analytics_result_cache_get(cache_key, allow_stale=False)
    if fresh_entry:
        return fresh_entry[0]
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None
    redis_available = bool(
        await _run_redis(
            r.ping(),
            op_name="ping gps-fraud",
            default=False,
            strict=False,
        )
    )
    if not redis_available:
        detail = "Analyse GPS live temporairement indisponible."
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        return _analytics_status_payload(
            {
                "fenetre_minutes": fenetre_minutes,
                "seuils": {
                    "teleportation_km": seuil_teleport_km,
                    "vitesse_max_physique_kmh": vitesse_max_physique_kmh,
                    "min_interval_seconds": GPS_FRAUD_MIN_INTERVAL_SECONDS,
                    "position_figee_min_rep": GPS_FRAUD_FROZEN_MIN_REPETITIONS,
                    "position_figee_min_duration_s": GPS_FRAUD_FROZEN_MIN_DURATION_SECONDS,
                },
                "resume": {
                    "livreurs_scannes": 0,
                    "teleportations": 0,
                    "positions_figees": 0,
                    "critiques": 0,
                    "total_fraudes": 0,
                },
                "teleportations": [],
                "positions_figees": [],
                "methodologie": (
                    "Flux live valide indisponible. Les sauts impossibles sont normalement bloques en amont "
                    "et seuls les GPS figes credibles sont exposes ici."
                ),
                "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
            },
            cold_path_available=False,
            degraded=True,
            source="hot_path_unavailable",
            detail=detail,
        )

    livreur_ids = (
        await _run_redis(
            r.zrange(GEO_KEY, 0, -1),
            op_name="zrange gps-fraud",
            default=[],
            strict=False,
        ) or []
    )
    track_payloads: list[Any] = []
    if livreur_ids:
        pipe = r.pipeline(transaction=False)
        for lid in livreur_ids:
            pipe.lrange(f"{TRACK_KEY_PREFIX}{lid}", 0, FLEET_INSIGHTS_TRACK_POINTS - 1)
        track_payloads = await _run_redis(
            pipe.execute(),
            op_name="pipeline recent-track gps-fraud",
            default=[],
            strict=False,
        ) or []
    if len(track_payloads) != len(livreur_ids):
        track_payloads = [None] * len(livreur_ids)

    positions_figees: list[dict[str, Any]] = []
    total_scanned = 0
    for lid, track_raw in zip(livreur_ids, track_payloads):
        recent_track = _parse_recent_track(track_raw if isinstance(track_raw, list) else None)
        track_window = _track_points_in_window(
            recent_track,
            window_start=window_start,
            window_end=ref_ts,
        )
        if not track_window:
            continue
        total_scanned += 1
        frozen = _best_live_frozen_streak(track_window)
        if not frozen:
            continue
        positions_figees.append({
            "livreur_id": lid,
            "type": "POSITION_FIGEE",
            "niveau": "warning",
            "lat": float(frozen["lat"]),
            "lon": float(frozen["lon"]),
            "nb_repetitions": int(frozen["nb_repetitions"]),
            "duration_s": int(frozen["duration_s"]),
            "debut": str(frozen["debut"]),
            "fin": str(frozen["fin"]),
            "risque": "GPS gele / spoofing — coordonnees identiques repetees",
        })

    positions_figees.sort(
        key=lambda item: (item.get("nb_repetitions", 0), item.get("duration_s", 0)),
        reverse=True,
    )
    teleportations: list[dict[str, Any]] = []
    nb_critique = 0

    payload = {
        "fenetre_minutes":    fenetre_minutes,
        "seuils": {
            "teleportation_km":         seuil_teleport_km,
            "vitesse_max_physique_kmh": vitesse_max_physique_kmh,
            "min_interval_seconds":     GPS_FRAUD_MIN_INTERVAL_SECONDS,
            "position_figee_min_rep":   GPS_FRAUD_FROZEN_MIN_REPETITIONS,
            "position_figee_min_duration_s": GPS_FRAUD_FROZEN_MIN_DURATION_SECONDS,
        },
        "resume": {
            "livreurs_scannes":     total_scanned,
            "teleportations":       0,
            "positions_figees":     len(positions_figees),
            "critiques":            nb_critique,
            "total_fraudes":        len(positions_figees),
        },
        "teleportations": teleportations,
        "positions_figees": positions_figees,
        "methodologie": (
            "Analyse sur le flux Redis valide du hot path. Les sauts physiquement impossibles sont bloques "
            "avant stockage, donc les teleportations visibles restent a zero. Les GPS figes sont detectes "
            "sur les memes coordonnees avec repetition et duree minimales."
        ),
        "time_reference": _build_time_reference(ref_meta, ref_ts, window_start),
    }
    payload = _analytics_status_payload(
        payload,
        cold_path_available=False,
        degraded=False,
        source="hot_path_live",
    )
    await _analytics_result_cache_set(cache_key, payload)
    return payload


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
    cache_key = _analytics_cache_key(
        "predict-demand",
        horizon_minutes=int(horizon_minutes),
        reference_ts=(reference_ts or ""),
    )
    fresh_entry = await _analytics_result_cache_get(cache_key, allow_stale=False)
    if fresh_entry:
        return fresh_entry[0]
    cached_entry = await _analytics_result_cache_get(cache_key, allow_stale=True)
    cached_payload = cached_entry[0] if cached_entry else None
    cached_at = cached_entry[1] if cached_entry else None
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
            timeout_sec=ANALYTICS_COLD_QUERY_TIMEOUT_SECONDS,
        )
    except HTTPException as exc:
        if not _analytics_should_degrade(exc):
            raise
        live_zone_contexts = await _load_live_zone_context_payloads(app.state.redis)
        live_payload = _build_predict_demand_live_fallback(
            zone_contexts=live_zone_contexts,
            horizon_minutes=horizon_minutes,
            ref_meta=ref_meta,
            ref_ts=ref_ts,
            recent_start=recent_start,
            baseline_start=baseline_start,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="live_zone_context",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        detail = _analytics_exception_detail(
            exc,
            fallback_label="Dispatch predictif temporairement indisponible.",
        )
        return _analytics_status_payload(
            {
                "horizon_minutes": horizon_minutes,
                "methode": "Extrapolation tendancielle T-15min vs baseline T-1h",
                "nb_zones_analysees": 0,
                "resume": {
                    "zones_en_hausse": 0,
                    "zones_stables": 0,
                    "zones_en_baisse": 0,
                },
                "dispatch_prioritaire": [],
                "zones_a_decouvrir": [],
                "toutes_zones": [],
                "time_reference": _build_time_reference(
                    ref_meta,
                    ref_ts,
                    recent_start,
                    {"baseline_window_start": _dt_to_iso(baseline_start)},
                ),
            },
            cold_path_available=False,
            degraded=True,
            source="cold_path_fallback",
            detail=detail,
        )
    except Exception as exc:
        live_zone_contexts = await _load_live_zone_context_payloads(app.state.redis)
        live_payload = _build_predict_demand_live_fallback(
            zone_contexts=live_zone_contexts,
            horizon_minutes=horizon_minutes,
            ref_meta=ref_meta,
            ref_ts=ref_ts,
            recent_start=recent_start,
            baseline_start=baseline_start,
        )
        if live_payload is not None:
            payload = _analytics_status_payload(
                live_payload,
                cold_path_available=False,
                degraded=False,
                source="live_zone_context",
            )
            await _analytics_result_cache_set(cache_key, payload)
            return payload
        if cached_payload:
            return _analytics_status_payload(
                cached_payload,
                cold_path_available=False,
                degraded=False,
                source="stale_cache",
                detail="",
                cached_at=cached_at,
            )
        detail = _analytics_exception_detail(
            exc,
            fallback_label="Dispatch predictif temporairement indisponible.",
        )
        return _analytics_status_payload(
            {
                "horizon_minutes": horizon_minutes,
                "methode": "Extrapolation tendancielle T-15min vs baseline T-1h",
                "nb_zones_analysees": 0,
                "resume": {
                    "zones_en_hausse": 0,
                    "zones_stables": 0,
                    "zones_en_baisse": 0,
                },
                "dispatch_prioritaire": [],
                "zones_a_decouvrir": [],
                "toutes_zones": [],
                "time_reference": _build_time_reference(
                    ref_meta,
                    ref_ts,
                    recent_start,
                    {"baseline_window_start": _dt_to_iso(baseline_start)},
                ),
            },
            cold_path_available=False,
            degraded=True,
            source="cold_path_fallback",
            detail=detail,
        )

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

    payload = {
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
    payload = _analytics_status_payload(
        payload,
        cold_path_available=True,
        degraded=False,
        source="live",
    )
    await _analytics_result_cache_set(cache_key, payload)
    return payload


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
    nb_pickup = 0
    nb_repositioning = 0
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
            bucket = _status_bucket(status)
            if bucket == "available":
                nb_available += 1
            elif bucket == "pickup":
                nb_pickup += 1
            elif bucket == "repositioning":
                nb_repositioning += 1
            elif bucket == "delivering":
                nb_delivering += 1
            elif bucket == "idle":
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
            "pickup":            nb_pickup,
            "repositioning":     nb_repositioning,
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
