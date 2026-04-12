from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import httpx
import joblib
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from copilot_logic import (
    FEATURE_COLUMNS,
    build_feature_map,
    heuristic_score,
    model_score,
    validate_model_payload,
)

logger = logging.getLogger("copilot-router")

load_dotenv()

EVENTS_PATH = Path(os.getenv("EVENTS_PATH", "/data/parquet_events"))
MODEL_PATH = Path(os.getenv("MODEL_PATH", "/data/models/copilot_model.joblib"))
OSRM_URL = os.getenv("OSRM_URL", "http://osrm:5000")
MODEL_MIN_ROWS = int(os.getenv("COPILOT_MODEL_MIN_ROWS", "300"))
MODEL_MIN_AUC = float(os.getenv("COPILOT_MODEL_MIN_AUC", "0.62"))
MODEL_MIN_AVG_PRECISION = float(os.getenv("COPILOT_MODEL_MIN_AVG_PRECISION", "0.45"))

WEATHER_KEY = "copilot:context:weather"
GBFS_KEY = "copilot:context:gbfs"
IRVE_KEY = "copilot:context:irve"
TLC_REPLAY_KEY = "copilot:replay:tlc:status"
ZONE_CONTEXT_PREFIX = "copilot:context:zone:"
OFFER_KEY_PREFIX = "copilot:offer:"
DRIVER_OFFERS_PREFIX = "copilot:driver:"

GBFS_STATION_INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
GBFS_STATION_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
OPENCHARGEMAP_URL = "https://api.openchargemap.io/v3/poi/"
OPENCHARGEMAP_API_KEY = os.getenv("OPENCHARGEMAP_API_KEY", "").strip()

# NYC bounding box covers all 5 boroughs
NYC_LAT_MIN, NYC_LAT_MAX = 40.49, 40.92
NYC_LON_MIN, NYC_LON_MAX = -74.27, -73.68
NYC_CENTER_LAT, NYC_CENTER_LON = 40.7580, -73.9855
ZONE_STEP = 0.02

copilot_router = APIRouter(prefix="/copilot", tags=["Copilot"])


class ScoreOfferRequest(BaseModel):
    offer_id: str | None = None
    courier_id: str | None = None
    estimated_fare_eur: float = Field(0.0, ge=0)
    estimated_distance_km: float = Field(0.0, ge=0)
    estimated_duration_min: float = Field(1.0, ge=0.1)
    demand_index: float = Field(1.0, ge=0)
    supply_index: float = Field(1.0, ge=0)
    weather_factor: float = Field(1.0, ge=0)
    traffic_factor: float = Field(1.0, ge=0)


class ScoreOfferResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    offer_id: str | None = None
    courier_id: str | None = None
    accept_score: float
    decision: str
    eur_per_hour_net: float
    model_used: str
    explanation: list[str]


def _run_duck_query(request: Request, sql: str, params: list[Any] | None = None) -> list[tuple]:
    def _execute() -> list[tuple]:
        cur = request.app.state.duckdb.cursor()
        return cur.execute(sql, params or []).fetchall()

    return _execute()


async def _duck_query(request: Request, sql: str, params: list[Any] | None = None) -> list[tuple]:
    return await asyncio.to_thread(_run_duck_query, request, sql, params)


def load_copilot_model() -> tuple[dict[str, Any] | None, dict[str, Any]]:
    quality_gate = {
        "accepted": False,
        "reason": "missing_model_file",
        "model_path": str(MODEL_PATH),
        "thresholds": {
            "min_rows": MODEL_MIN_ROWS,
            "min_auc": MODEL_MIN_AUC,
            "min_average_precision": MODEL_MIN_AVG_PRECISION,
        },
    }

    if not MODEL_PATH.exists():
        return None, quality_gate
    try:
        payload = joblib.load(MODEL_PATH)
        if not isinstance(payload, dict):
            quality_gate["reason"] = "invalid_payload_type"
            return None, quality_gate

        is_valid, reason = validate_model_payload(
            payload,
            min_auc=MODEL_MIN_AUC,
            min_avg_precision=MODEL_MIN_AVG_PRECISION,
            min_rows=MODEL_MIN_ROWS,
        )
        quality_gate["reason"] = reason
        quality_gate["trained_rows"] = int(payload.get("trained_rows") or 0)
        quality_gate["metrics"] = payload.get("metrics", {})

        if not is_valid:
            logger.warning("copilot model rejected by quality gate: %s", reason)
            return None, quality_gate

        quality_gate["accepted"] = True
        logger.info("copilot model accepted by quality gate")
        return payload, quality_gate
    except Exception as exc:  # pragma: no cover
        quality_gate["reason"] = f"load_error:{type(exc).__name__}"
        logger.warning("failed to load model from %s: %s", MODEL_PATH, exc)
        return None, quality_gate


async def _weather_loop(app) -> None:
    await asyncio.sleep(3)
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": NYC_CENTER_LAT,
        "longitude": NYC_CENTER_LON,
        "current": "temperature_2m,precipitation,wind_speed_10m",
    }
    async with httpx.AsyncClient(timeout=12) as client:
        while True:
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                payload = resp.json().get("current", {})
                await app.state.redis.hset(
                    WEATHER_KEY,
                    mapping={
                        "temperature_2m": payload.get("temperature_2m", ""),
                        "precipitation": payload.get("precipitation", ""),
                        "wind_speed_10m": payload.get("wind_speed_10m", ""),
                        "updated_at": str(asyncio.get_event_loop().time()),
                    },
                )
                await app.state.redis.expire(WEATHER_KEY, 3600)
            except Exception as exc:
                logger.info("weather loop degraded: %s", exc)
            await asyncio.sleep(600)


def _zone_id(lat: float, lon: float) -> str:
    lat_cell = round(round(lat / ZONE_STEP) * ZONE_STEP, 3)
    lon_cell = round(round(lon / ZONE_STEP) * ZONE_STEP, 3)
    return f"{lat_cell:.3f}_{lon_cell:.3f}"


async def _gbfs_loop(app) -> None:
    """Poll Citi Bike GBFS station_status every 60s and materialize zone-level demand signals."""
    await asyncio.sleep(5)

    station_coords: dict[str, tuple[float, float]] = {}

    async with httpx.AsyncClient(timeout=15) as client:
        # Load station coordinates once
        try:
            resp = await client.get(GBFS_STATION_INFO_URL)
            resp.raise_for_status()
            for s in resp.json().get("data", {}).get("stations", []):
                sid = str(s.get("station_id", ""))
                lat = float(s.get("lat", 0))
                lon = float(s.get("lon", 0))
                if NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LON_MIN <= lon <= NYC_LON_MAX:
                    station_coords[sid] = (lat, lon)
            logger.info("gbfs loaded %d Citi Bike stations in NYC", len(station_coords))
        except Exception as exc:
            logger.warning("gbfs station_information fetch failed: %s", exc)

        while True:
            try:
                resp = await client.get(GBFS_STATION_STATUS_URL)
                resp.raise_for_status()
                stations = resp.json().get("data", {}).get("stations", [])

                zone_bikes: dict[str, list[int]] = {}
                zone_docks: dict[str, list[int]] = {}
                total_stations = 0

                for s in stations:
                    sid = str(s.get("station_id", ""))
                    if sid not in station_coords:
                        continue
                    lat, lon = station_coords[sid]
                    zid = _zone_id(lat, lon)
                    avail = int(s.get("num_bikes_available", 0))
                    docks = int(s.get("num_docks_available", 0))
                    zone_bikes.setdefault(zid, []).append(avail)
                    zone_docks.setdefault(zid, []).append(docks)
                    total_stations += 1

                r = app.state.redis
                pipe = r.pipeline(transaction=False)
                zones_updated = 0
                for zid in zone_bikes:
                    bikes = zone_bikes[zid]
                    docks = zone_docks.get(zid, [0])
                    total_capacity = sum(bikes) + sum(docks)
                    occupancy = sum(bikes) / max(total_capacity, 1)
                    # High occupancy = lots of bikes = low taxi demand (people have bikes)
                    # Low occupancy = no bikes = people need transport = higher taxi demand
                    demand_boost = max(0.0, 1.0 - occupancy) * 0.5
                    key = f"copilot:context:gbfs:zone:{zid}"
                    pipe.hset(key, mapping={
                        "zone_id": zid,
                        "stations_count": len(bikes),
                        "bikes_available": sum(bikes),
                        "docks_available": sum(docks),
                        "occupancy_ratio": round(occupancy, 3),
                        "demand_boost": round(demand_boost, 3),
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                    pipe.expire(key, 300)

                    # Also update the main zone context with GBFS demand signal
                    ctx_key = f"{ZONE_CONTEXT_PREFIX}{zid}"
                    pipe.hset(ctx_key, mapping={
                        "gbfs_demand_boost": round(demand_boost, 3),
                        "gbfs_occupancy": round(occupancy, 3),
                        "gbfs_stations": len(bikes),
                    })
                    zones_updated += 1
                await pipe.execute()

                await r.hset(GBFS_KEY, mapping={
                    "status": "ok",
                    "stations_polled": str(total_stations),
                    "zones_updated": str(zones_updated),
                    "updated_at": str(asyncio.get_event_loop().time()),
                })
                await r.expire(GBFS_KEY, 600)

                if zones_updated:
                    logger.info("gbfs updated %d zones from %d stations", zones_updated, total_stations)
            except Exception as exc:
                logger.warning("gbfs poll failed: %s", exc)
                try:
                    await app.state.redis.hset(GBFS_KEY, mapping={
                        "status": f"degraded:{type(exc).__name__}",
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                except Exception:
                    pass
            await asyncio.sleep(60)


async def _irve_loop(app) -> None:
    """Load NYC EV charging stations from OpenChargeMap and store in Redis. Refreshes every 6h.

    Field name "irve" kept for Redis key compatibility — the source is OpenChargeMap (NYC).
    """
    await asyncio.sleep(10)

    params = {
        "output": "json",
        "countrycode": "US",
        "latitude": NYC_CENTER_LAT,
        "longitude": NYC_CENTER_LON,
        "distance": 30,
        "distanceunit": "KM",
        "maxresults": 2000,
        "compact": "true",
        "verbose": "false",
    }
    headers = {"User-Agent": "FleetStream/1.0 (driver-revenue-copilot)"}
    if OPENCHARGEMAP_API_KEY:
        headers["X-API-Key"] = OPENCHARGEMAP_API_KEY

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        while True:
            try:
                resp = await client.get(OPENCHARGEMAP_URL, params=params, headers=headers)
                resp.raise_for_status()
                stations = resp.json()
                if not isinstance(stations, list):
                    stations = []

                r = app.state.redis
                pipe = r.pipeline(transaction=False)
                count = 0

                for poi in stations:
                    addr = (poi.get("AddressInfo") or {})
                    try:
                        lat = float(addr.get("Latitude") or 0)
                        lon = float(addr.get("Longitude") or 0)
                    except (ValueError, TypeError):
                        continue

                    if not (NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LON_MIN <= lon <= NYC_LON_MAX):
                        continue

                    station_id = str(poi.get("ID") or f"ocm_{count}")
                    nom = (addr.get("Title") or "")[:100]
                    connections = poi.get("Connections") or []
                    max_kw = 0.0
                    for c in connections:
                        try:
                            pw = float(c.get("PowerKW") or 0)
                            if pw > max_kw:
                                max_kw = pw
                        except (ValueError, TypeError):
                            continue
                    zid = _zone_id(lat, lon)

                    key = f"copilot:context:irve:station:{station_id}"
                    pipe.hset(key, mapping={
                        "station_id": station_id,
                        "lat": lat,
                        "lon": lon,
                        "nom": nom,
                        "puissance_kw": str(max_kw),
                        "zone_id": zid,
                        "source": "openchargemap",
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                    pipe.expire(key, 86400)

                    pipe.geoadd("copilot:irve:geo", [lon, lat, station_id])
                    count += 1

                pipe.expire("copilot:irve:geo", 86400)
                await pipe.execute()

                await r.hset(IRVE_KEY, mapping={
                    "status": "ok",
                    "stations_loaded": str(count),
                    "source": "openchargemap_nyc",
                    "updated_at": str(asyncio.get_event_loop().time()),
                })
                await r.expire(IRVE_KEY, 86400)
                logger.info("openchargemap loaded %d EV charging stations in NYC area", count)

            except Exception as exc:
                logger.warning("openchargemap load failed: %s", exc)
                try:
                    await app.state.redis.hset(IRVE_KEY, mapping={
                        "status": f"degraded:{type(exc).__name__}",
                        "updated_at": str(asyncio.get_event_loop().time()),
                    })
                except Exception:
                    pass
            await asyncio.sleep(21600)  # Refresh every 6 hours


def start_copilot_background_tasks(app) -> list[asyncio.Task]:
    model_payload, quality_gate = load_copilot_model()
    app.state.copilot_model = model_payload
    app.state.copilot_model_quality_gate = quality_gate
    tasks = [
        asyncio.create_task(_weather_loop(app)),
        asyncio.create_task(_gbfs_loop(app)),
        asyncio.create_task(_irve_loop(app)),
    ]
    return tasks


def stop_copilot_background_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()


@copilot_router.get("", include_in_schema=False)
async def copilot_web() -> FileResponse:
    page = Path(__file__).parent / "static" / "copilot" / "index.html"
    if page.exists():
        return FileResponse(str(page), media_type="text/html")
    raise HTTPException(status_code=404, detail="copilot PWA page not found")


@copilot_router.post("/score-offer", response_model=ScoreOfferResponse)
async def score_offer(request: Request, body: ScoreOfferRequest) -> ScoreOfferResponse:
    redis_client: aioredis.Redis = request.app.state.redis

    payload: dict[str, Any] = body.model_dump()
    if body.offer_id:
        offer_map = await redis_client.hgetall(f"{OFFER_KEY_PREFIX}{body.offer_id}")
        if offer_map:
            payload.update(offer_map)

    features = build_feature_map(payload)
    heuristic_prob, eur_per_hour, reasons = heuristic_score(features)

    model_payload = getattr(request.app.state, "copilot_model", None)
    model_prob, model_used = model_score(model_payload, features)
    accept_prob = model_prob if model_prob is not None else heuristic_prob

    decision = "accept" if accept_prob >= 0.5 else "reject"

    return ScoreOfferResponse(
        offer_id=payload.get("offer_id"),
        courier_id=payload.get("courier_id"),
        accept_score=round(float(accept_prob), 4),
        decision=decision,
        eur_per_hour_net=round(float(eur_per_hour), 2),
        model_used=model_used,
        explanation=reasons,
    )


@copilot_router.get("/driver/{driver_id}/offers")
async def driver_offers(request: Request, driver_id: str, limit: int = Query(20, ge=1, le=200)):
    redis_client: aioredis.Redis = request.app.state.redis
    offer_ids = await redis_client.lrange(f"{DRIVER_OFFERS_PREFIX}{driver_id}:offers", 0, limit - 1)
    if not offer_ids:
        return {"driver_id": driver_id, "count": 0, "offers": []}

    pipe = redis_client.pipeline(transaction=False)
    for offer_id in offer_ids:
        pipe.hgetall(f"{OFFER_KEY_PREFIX}{offer_id}")
    raw_offers = await pipe.execute()

    offers = []
    for off in raw_offers:
        if not off:
            continue
        features = build_feature_map(off)
        heur_prob, eur_per_hour, reasons = heuristic_score(features)
        model_payload = getattr(request.app.state, "copilot_model", None)
        model_prob, model_used = model_score(model_payload, features)
        accept_prob = model_prob if model_prob is not None else heur_prob

        offers.append(
            {
                "offer_id": off.get("offer_id"),
                "courier_id": off.get("courier_id"),
                "zone_id": off.get("zone_id"),
                "ts": off.get("ts"),
                "accept_score": round(float(accept_prob), 4),
                "decision": "accept" if accept_prob >= 0.5 else "reject",
                "eur_per_hour_net": round(float(eur_per_hour), 2),
                "model_used": model_used,
                "explanation": reasons,
            }
        )

    return {"driver_id": driver_id, "count": len(offers), "offers": offers}


@copilot_router.get("/driver/{driver_id}/next-best-zone")
async def next_best_zone(request: Request, driver_id: str, top_k: int = Query(5, ge=1, le=20)):
    redis_client: aioredis.Redis = request.app.state.redis

    zones = []
    async for key in redis_client.scan_iter(match=f"{ZONE_CONTEXT_PREFIX}*"):
        zone = await redis_client.hgetall(key)
        if not zone:
            continue
        demand = float(zone.get("demand_index", 1.0))
        supply = max(float(zone.get("supply_index", 1.0)), 0.2)
        weather = float(zone.get("weather_factor", 1.0))
        traffic = max(float(zone.get("traffic_factor", 1.0)), 0.2)

        opportunity = (demand / supply) * weather / traffic
        zones.append(
            {
                "zone_id": zone.get("zone_id"),
                "demand_index": round(demand, 3),
                "supply_index": round(supply, 3),
                "weather_factor": round(weather, 3),
                "traffic_factor": round(traffic, 3),
                "opportunity_score": round(opportunity, 3),
            }
        )

    zones.sort(key=lambda z: z["opportunity_score"], reverse=True)
    return {"driver_id": driver_id, "count": len(zones), "recommendations": zones[:top_k]}


@copilot_router.get("/replay")
async def replay(
    request: Request,
    from_ts: str = Query(..., alias="from"),
    to_ts: str = Query(..., alias="to"),
    driver_id: str | None = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    if not EVENTS_PATH.exists():
        raise HTTPException(status_code=404, detail=f"events path missing: {EVENTS_PATH}")

    glob = str(EVENTS_PATH / "**" / "*.parquet")
    rows = await _duck_query(
        request,
        f"""
        SELECT
            ts,
            topic,
            event_type,
            event_id,
            courier_id,
            offer_id,
            order_id,
            status,
            zone_id,
            estimated_fare_eur,
            actual_fare_eur,
            demand_index,
            supply_index
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE ts BETWEEN CAST(? AS TIMESTAMPTZ) AND CAST(? AS TIMESTAMPTZ)
          AND (? IS NULL OR courier_id = ?)
        ORDER BY ts
        LIMIT ?
        """,
        [from_ts, to_ts, driver_id, driver_id, limit],
    )

    return {
        "from": from_ts,
        "to": to_ts,
        "driver_id": driver_id,
        "count": len(rows),
        "events": [
            {
                "ts": str(r[0]),
                "topic": r[1],
                "event_type": r[2],
                "event_id": r[3],
                "courier_id": r[4],
                "offer_id": r[5],
                "order_id": r[6],
                "status": r[7],
                "zone_id": r[8],
                "estimated_fare_eur": r[9],
                "actual_fare_eur": r[10],
                "demand_index": r[11],
                "supply_index": r[12],
            }
            for r in rows
        ],
    }


@copilot_router.get("/route")
async def osrm_route(
    request: Request,
    origin_lat: float = Query(...),
    origin_lon: float = Query(...),
    dest_lat: float = Query(...),
    dest_lon: float = Query(...),
):
    """Get route information from OSRM (distance, duration, geometry)."""
    coords = f"{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
    url = f"{OSRM_URL}/route/v1/driving/{coords}?overview=simplified&geometries=geojson"
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != "Ok" or not data.get("routes"):
                raise HTTPException(status_code=502, detail=f"OSRM error: {data.get('code')}")
            route = data["routes"][0]
            return {
                "distance_km": round(route["distance"] / 1000, 3),
                "duration_min": round(route["duration"] / 60, 2),
                "geometry": route.get("geometry"),
            }
        except httpx.ConnectError:
            raise HTTPException(
                status_code=503,
                detail="OSRM not available. Run: bash scripts/prepare_osrm.sh && docker compose --profile routing up osrm",
            )


@copilot_router.get("/irve/nearby")
async def irve_nearby(
    request: Request,
    lat: float = Query(..., ge=NYC_LAT_MIN, le=NYC_LAT_MAX),
    lon: float = Query(..., ge=NYC_LON_MIN, le=NYC_LON_MAX),
    radius_km: float = Query(2.0, ge=0.1, le=20.0),
    limit: int = Query(10, ge=1, le=50),
):
    """Find nearest EV charging stations (IRVE) from a given position."""
    redis_client: aioredis.Redis = request.app.state.redis
    results = await redis_client.geosearch(
        "copilot:irve:geo",
        longitude=lon,
        latitude=lat,
        radius=radius_km,
        unit="km",
        sort="ASC",
        count=limit,
        withcoord=True,
        withdist=True,
    )
    stations = []
    for item in results:
        station_id = item[0] if isinstance(item, (list, tuple)) else item
        dist = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else None
        coord = item[2] if isinstance(item, (list, tuple)) and len(item) > 2 else None
        info = await redis_client.hgetall(f"copilot:context:irve:station:{station_id}")
        stations.append({
            "station_id": station_id,
            "distance_km": round(float(dist), 3) if dist else None,
            "lon": float(coord[0]) if coord else None,
            "lat": float(coord[1]) if coord else None,
            "nom": info.get("nom", ""),
            "puissance_kw": info.get("puissance_kw", ""),
        })
    return {"lat": lat, "lon": lon, "radius_km": radius_km, "count": len(stations), "stations": stations}


@copilot_router.get("/gbfs/zones")
async def gbfs_zones(request: Request, top_k: int = Query(20, ge=1, le=100)):
    """Return Citi Bike zone signals sorted by demand boost (proxy for taxi demand)."""
    redis_client: aioredis.Redis = request.app.state.redis
    zones = []
    async for key in redis_client.scan_iter(match="copilot:context:gbfs:zone:*"):
        z = await redis_client.hgetall(key)
        if z:
            zones.append({
                "zone_id": z.get("zone_id", ""),
                "bikes_available": int(z.get("bikes_available", 0)),
                "docks_available": int(z.get("docks_available", 0)),
                "occupancy_ratio": float(z.get("occupancy_ratio", 0)),
                "demand_boost": float(z.get("demand_boost", 0)),
                "stations_count": int(z.get("stations_count", 0)),
            })
    zones.sort(key=lambda x: x["demand_boost"], reverse=True)
    return {"count": len(zones), "zones": zones[:top_k]}


@copilot_router.get("/health")
async def copilot_health(request: Request):
    redis_client: aioredis.Redis = request.app.state.redis
    model_payload = getattr(request.app.state, "copilot_model", None)
    quality_gate = getattr(request.app.state, "copilot_model_quality_gate", {})

    weather = await redis_client.hgetall(WEATHER_KEY)
    gbfs = await redis_client.hgetall(GBFS_KEY)
    irve = await redis_client.hgetall(IRVE_KEY)
    tlc_replay = await redis_client.hgetall(TLC_REPLAY_KEY)

    return {
        "model_loaded": bool(model_payload),
        "feature_columns": (model_payload or {}).get("feature_columns", FEATURE_COLUMNS),
        "model_metrics": (model_payload or {}).get("metrics", {}),
        "model_quality_gate": quality_gate,
        "weather_context": weather,
        "gbfs_context": gbfs,
        "irve_context": irve,
        "tlc_replay": tlc_replay,
        "events_path": str(EVENTS_PATH),
        "model_path": str(MODEL_PATH),
    }
