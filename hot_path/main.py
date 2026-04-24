"""
FleetStream hot consumer for protobuf courier positions.

Consumes courier positions and writes Redis GEO + hashes with strict validation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Literal

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

from copilot_events_pb2 import CourierPositionV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("hot-consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
COURIER_TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
TTL_SECONDS = int(os.getenv("GPS_TTL_SECONDS", "30"))

CONSUMER_GROUP = "hot-consumer"
GEO_KEY = "fleet:geo"
GEO_TS_KEY = "fleet:geo:lastseen"
HASH_PREFIX = "fleet:livreur:"
TRACK_KEY_PREFIX = "fleet:track:"
ANOMALY_KEY = "fleet:anomalies"
STATS_MSGS_KEY = "fleet:stats:total_messages"
STATS_PURGED_KEY = "fleet:stats:ghosts_purged"
STATS_DLQ_KEY = "fleet:stats:dlq_count"
DLQ_KEY = "fleet:dlq"
DLQ_MAX_LEN = 1000
ANOMALY_MAX_LEN = max(100, int(os.getenv("FLEET_ANOMALY_MAX_LEN", "1000") or "1000"))
TRACK_MAX_POINTS = max(8, int(os.getenv("FLEET_TRACK_MAX_POINTS", "64") or "64"))
TRACK_TTL_SECONDS = max(TTL_SECONDS, int(os.getenv("FLEET_TRACK_TTL_SECONDS", "180") or "180"))
PURGE_INTERVAL_S = float(os.getenv("GEO_PURGE_INTERVAL_S", "5.0"))

GPS_GUARD_MAX_SPEED_KMH = max(1.0, float(os.getenv("GPS_GUARD_MAX_SPEED_KMH", "120.0") or "120.0"))
GPS_GUARD_MAX_JUMP_KM = max(0.1, float(os.getenv("GPS_GUARD_MAX_JUMP_KM", "4.0") or "4.0"))
GPS_GUARD_BACKTRACK_KM = max(0.1, float(os.getenv("GPS_GUARD_BACKTRACK_KM", "0.8") or "0.8"))
GPS_GUARD_BACKTRACK_WINDOW_S = max(
    5.0,
    float(os.getenv("GPS_GUARD_BACKTRACK_WINDOW_S", "90.0") or "90.0"),
)
GPS_GUARD_FROZEN_EPSILON_M = max(
    0.5,
    float(os.getenv("GPS_GUARD_FROZEN_EPSILON_M", "8.0") or "8.0"),
)
GPS_GUARD_FROZEN_STREAK = max(2, int(os.getenv("GPS_GUARD_FROZEN_STREAK", "4") or "4"))

EARTH_RADIUS_KM = 6371.0
MOVING_STATUSES = {"delivering", "pickup_assigned", "pickup_en_route", "pickup_arrived", "repositioning"}


class Position(BaseModel):
    courier_id: str = Field(..., min_length=1, max_length=64)
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed_kmh: float = Field(0.0, ge=0.0, le=300.0)
    heading_deg: float = Field(0.0, ge=0.0, le=360.0)
    status: Literal[
        "available",
        "delivering",
        "idle",
        "pickup_assigned",
        "pickup_en_route",
        "pickup_arrived",
        "repositioning",
        "unknown",
    ] = "unknown"
    accuracy_m: float = Field(0.0, ge=0.0, le=10000.0)
    battery_pct: float = Field(0.0, ge=0.0, le=100.0)
    ts: str = Field(..., min_length=1)
    source_platform: str = Field("", max_length=255)


Position.model_rebuild()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    token = str(value).strip()
    if not token:
        return None
    try:
        parsed = datetime.fromisoformat(token.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2.0) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2.0) ** 2
    )
    return EARTH_RADIUS_KM * 2.0 * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _parse_route_source(source_platform: str | None) -> str:
    raw = str(source_platform or "").strip().lower()
    for part in raw.split("|"):
        if part.startswith("route="):
            route = part.split("=", 1)[1].strip().lower()
            if route in {"osrm", "linear", "hold"}:
                return route
            if route:
                return route
    return "unknown"


def _decode_track_items(raw_items: list[str] | None) -> list[dict[str, Any]]:
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
        item["lat"] = lat
        item["lon"] = lon
        points.append(item)
    return points


def _safe_float(mapping: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(mapping.get(key, default) or default)
    except (TypeError, ValueError):
        return default


def _safe_int(mapping: dict[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(mapping.get(key, default) or default)
    except (TypeError, ValueError):
        return default


def _build_reject(reason: str, msg: Position, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "reason": reason,
        "courier_id": msg.courier_id,
        "lat": msg.lat,
        "lon": msg.lon,
        "speed_kmh": msg.speed_kmh,
        "status": msg.status,
        "ts": msg.ts,
        "source_platform": msg.source_platform,
        "ts_rejected": _utc_now_iso(),
    }
    if details:
        payload["details"] = details
    return payload


def _guardrail_decision(
    msg: Position,
    prev_hash: dict[str, Any],
    recent_track: list[dict[str, Any]],
) -> tuple[str | None, dict[str, Any]]:
    current_ts = _parse_iso_ts(msg.ts)
    if current_ts is None:
        return "invalid_ts", {}

    prev_ts = _parse_iso_ts(prev_hash.get("last_valid_ts") or prev_hash.get("ts"))
    prev_lat = _safe_float(prev_hash, "lat", msg.lat)
    prev_lon = _safe_float(prev_hash, "lon", msg.lon)
    frozen_count = _safe_int(prev_hash, "frozen_count", 0)

    details: dict[str, Any] = {
        "frozen_count": 0,
        "distance_km": 0.0,
        "delta_s": 0.0,
        "implied_speed_kmh": 0.0,
    }
    if prev_ts is None:
        return None, details

    delta_s = (current_ts - prev_ts).total_seconds()
    details["delta_s"] = round(delta_s, 3)
    if delta_s <= 0:
        return "non_monotonic_ts", details

    distance_km = _haversine_km(prev_lat, prev_lon, msg.lat, msg.lon)
    details["distance_km"] = round(distance_km, 4)
    if distance_km > GPS_GUARD_MAX_JUMP_KM:
        return "jump_too_large", details

    implied_speed_kmh = distance_km / (delta_s / 3600.0) if delta_s > 0 else float("inf")
    details["implied_speed_kmh"] = round(implied_speed_kmh, 2)
    if implied_speed_kmh > GPS_GUARD_MAX_SPEED_KMH:
        return "implied_speed_too_high", details

    if recent_track and len(recent_track) >= 2:
        prior = recent_track[-2]
        prior_ts = _parse_iso_ts(prior.get("ts"))
        if prior_ts is not None and (current_ts - prior_ts).total_seconds() <= GPS_GUARD_BACKTRACK_WINDOW_S:
            prior_lat = float(prior.get("lat", msg.lat))
            prior_lon = float(prior.get("lon", msg.lon))
            step_out_km = _haversine_km(prior_lat, prior_lon, prev_lat, prev_lon)
            bounce_back_km = _haversine_km(prior_lat, prior_lon, msg.lat, msg.lon)
            if step_out_km >= GPS_GUARD_BACKTRACK_KM and bounce_back_km <= 0.05:
                details["step_out_km"] = round(step_out_km, 4)
                details["bounce_back_km"] = round(bounce_back_km, 4)
                return "implausible_backtrack", details

    if distance_km * 1000.0 <= GPS_GUARD_FROZEN_EPSILON_M and msg.status in MOVING_STATUSES and msg.speed_kmh >= 2.0:
        frozen_count += 1
    else:
        frozen_count = 0
    details["frozen_count"] = frozen_count
    if frozen_count >= GPS_GUARD_FROZEN_STREAK:
        return "gps_frozen", details

    return None, details


def parse_position(raw_value: bytes) -> tuple[Position | None, dict | None]:
    try:
        msg = CourierPositionV1()
        msg.ParseFromString(raw_value)
        position = Position(
            courier_id=msg.courier_id,
            lat=msg.lat,
            lon=msg.lon,
            speed_kmh=msg.speed_kmh,
            heading_deg=msg.heading_deg,
            status=msg.status or "unknown",
            accuracy_m=msg.accuracy_m,
            battery_pct=msg.battery_pct,
            ts=msg.ts,
            source_platform=msg.source_platform or "",
        )
        return position, None
    except ValidationError as exc:
        return None, {
            "reason": exc.errors(include_url=False, include_input=False),
            "raw_size": len(raw_value),
            "ts_rejected": _utc_now_iso(),
        }
    except Exception as exc:
        try:
            payload = json.loads(raw_value.decode())
            position = Position(
                courier_id=payload.get("livreur_id") or payload.get("courier_id", ""),
                lat=payload.get("lat"),
                lon=payload.get("lon"),
                speed_kmh=payload.get("speed_kmh", 0.0),
                heading_deg=payload.get("heading_deg", 0.0),
                status=payload.get("status", "unknown"),
                accuracy_m=payload.get("accuracy_m", 0.0),
                battery_pct=payload.get("battery_pct", 0.0),
                ts=payload.get("timestamp") or payload.get("ts") or "",
                source_platform=payload.get("source_platform", ""),
            )
            return position, None
        except Exception:
            return None, {
                "reason": str(exc),
                "raw_size": len(raw_value),
                "ts_rejected": _utc_now_iso(),
            }


async def push_dlq(r: aioredis.Redis, rejects: list[dict]) -> None:
    if not rejects:
        return
    pipe = r.pipeline(transaction=False)
    for item in rejects:
        pipe.lpush(DLQ_KEY, json.dumps(item, default=str, separators=(",", ":")))
    pipe.ltrim(DLQ_KEY, 0, DLQ_MAX_LEN - 1)
    pipe.incrby(STATS_DLQ_KEY, len(rejects))
    await pipe.execute()


async def flush_to_redis(r: aioredis.Redis, batch: list[Position]) -> tuple[int, list[dict[str, Any]]]:
    if not batch:
        return 0, []

    prefetch = r.pipeline(transaction=False)
    for msg in batch:
        prefetch.hgetall(f"{HASH_PREFIX}{msg.courier_id}")
        prefetch.lrange(f"{TRACK_KEY_PREFIX}{msg.courier_id}", 0, TRACK_MAX_POINTS - 1)
    prefetched = await prefetch.execute()

    state_by_courier: dict[str, dict[str, Any]] = {}
    cursor = 0
    for msg in batch:
        prev_hash = prefetched[cursor] or {}
        cursor += 1
        prev_track = prefetched[cursor] or []
        cursor += 1
        if msg.courier_id not in state_by_courier:
            state_by_courier[msg.courier_id] = {
                "hash": prev_hash if isinstance(prev_hash, dict) else {},
                "track": _decode_track_items(prev_track if isinstance(prev_track, list) else []),
            }

    now_ms = int(time.time() * 1000)
    pipe = r.pipeline(transaction=False)
    accepted = 0
    rejects: list[dict[str, Any]] = []
    anomaly_events: list[dict[str, Any]] = []

    for msg in batch:
        state = state_by_courier[msg.courier_id]
        prev_hash = state["hash"]
        recent_track = state["track"]
        anomaly_reason, details = _guardrail_decision(msg, prev_hash, recent_track)
        route_source = _parse_route_source(msg.source_platform)
        hash_key = f"{HASH_PREFIX}{msg.courier_id}"

        if anomaly_reason:
            reject = _build_reject(anomaly_reason, msg, details=details)
            rejects.append(reject)
            anomaly_events.append(reject)
            if prev_hash:
                anomaly_mapping = {
                    "anomaly_state": "anomalous",
                    "stale_reason": anomaly_reason,
                    "last_anomaly_ts": msg.ts,
                    "last_anomaly_reason": anomaly_reason,
                }
                if route_source != "unknown":
                    anomaly_mapping["route_source"] = route_source
                pipe.hset(hash_key, mapping=anomaly_mapping)
                pipe.expire(hash_key, TTL_SECONDS)
                state["hash"] = {**prev_hash, **anomaly_mapping}
            continue

        track_point = {
            "lat": round(msg.lat, 6),
            "lon": round(msg.lon, 6),
            "ts": msg.ts,
            "speed_kmh": round(float(msg.speed_kmh), 2),
            "status": msg.status,
            "route_source": route_source,
            "anomaly_state": "ok",
        }
        mapping = {
            "lat": msg.lat,
            "lon": msg.lon,
            "speed_kmh": msg.speed_kmh,
            "heading_deg": msg.heading_deg,
            "status": msg.status,
            "accuracy_m": msg.accuracy_m,
            "battery_pct": msg.battery_pct,
            "ts": msg.ts,
            "last_valid_ts": msg.ts,
            "source_platform": msg.source_platform,
            "route_source": route_source,
            "anomaly_state": "ok",
            "stale_reason": "routing_hold" if route_source == "hold" else "",
            "frozen_count": details.get("frozen_count", 0),
        }

        pipe.geoadd(GEO_KEY, [msg.lon, msg.lat, msg.courier_id])
        pipe.zadd(GEO_TS_KEY, {msg.courier_id: now_ms})
        pipe.hset(hash_key, mapping=mapping)
        pipe.expire(hash_key, TTL_SECONDS)

        track_key = f"{TRACK_KEY_PREFIX}{msg.courier_id}"
        pipe.lpush(track_key, json.dumps(track_point, separators=(",", ":")))
        pipe.ltrim(track_key, 0, TRACK_MAX_POINTS - 1)
        pipe.expire(track_key, TRACK_TTL_SECONDS)

        state["hash"] = mapping
        state["track"] = (recent_track + [track_point])[-TRACK_MAX_POINTS:]
        accepted += 1

    for item in anomaly_events:
        pipe.lpush(ANOMALY_KEY, json.dumps(item, default=str, separators=(",", ":")))
    if anomaly_events:
        pipe.ltrim(ANOMALY_KEY, 0, ANOMALY_MAX_LEN - 1)
        pipe.expire(ANOMALY_KEY, TRACK_TTL_SECONDS)
    if accepted:
        pipe.incrby(STATS_MSGS_KEY, accepted)

    await pipe.execute()
    return accepted, rejects


async def purge_ghosts_loop(r: aioredis.Redis, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            cutoff_ms = int((time.time() - TTL_SECONDS) * 1000)
            expired = await r.zrangebyscore(GEO_TS_KEY, "-inf", f"({cutoff_ms}")
            if expired:
                pipe = r.pipeline(transaction=False)
                pipe.zrem(GEO_KEY, *expired)
                pipe.zrem(GEO_TS_KEY, *expired)
                pipe.incrby(STATS_PURGED_KEY, len(expired))
                await pipe.execute()
                log.info("purged %d stale couriers from geo index", len(expired))
        except Exception as exc:
            log.warning("purge loop error: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=PURGE_INTERVAL_S)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("signal received, stopping hot consumer")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    consumer = AIOKafkaConsumer(
        COURIER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        fetch_max_wait_ms=50,
        max_poll_records=500,
    )

    for attempt in range(1, 11):
        try:
            await consumer.start()
            log.info("connected to topic=%s", COURIER_TOPIC)
            break
        except Exception as exc:  # pragma: no cover
            log.warning("attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to connect to kafka")
        return

    purge_task = asyncio.create_task(purge_ghosts_loop(redis_client, stop_event))
    processed = 0
    rejected_total = 0

    try:
        while not stop_event.is_set():
            raw = await consumer.getmany(timeout_ms=100, max_records=500)
            values = [msg.value for msgs in raw.values() for msg in msgs]
            if not values:
                continue

            valid: list[Position] = []
            rejects: list[dict[str, Any]] = []
            for value in values:
                pos, reject = parse_position(value)
                if pos is not None:
                    valid.append(pos)
                elif reject is not None:
                    rejects.append(reject)

            if valid:
                accepted, live_rejects = await flush_to_redis(redis_client, valid)
                processed += accepted
                if live_rejects:
                    rejects.extend(live_rejects)

            if rejects:
                await push_dlq(redis_client, rejects)
                rejected_total += len(rejects)

            if processed and processed % 10_000 == 0:
                log.info("processed=%d rejected=%d", processed, rejected_total)
    finally:
        purge_task.cancel()
        try:
            await purge_task
        except asyncio.CancelledError:
            pass
        await consumer.stop()
        await redis_client.aclose()
        log.info("hot consumer stopped processed=%d rejected=%d", processed, rejected_total)


if __name__ == "__main__":
    asyncio.run(main())
