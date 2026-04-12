"""
FleetStream hot consumer for protobuf courier positions.

Consumes topic courier positions and writes Redis GEO + hashes with strict validation.
"""
import asyncio
import json
import logging
import os
import signal
import time
from typing import Literal

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
STATS_MSGS_KEY = "fleet:stats:total_messages"
STATS_PURGED_KEY = "fleet:stats:ghosts_purged"
STATS_DLQ_KEY = "fleet:stats:dlq_count"
DLQ_KEY = "fleet:dlq"
DLQ_MAX_LEN = 1000
PURGE_INTERVAL_S = float(os.getenv("GEO_PURGE_INTERVAL_S", "5.0"))


class Position(BaseModel):
    courier_id: str = Field(..., min_length=1, max_length=64)
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed_kmh: float = Field(0.0, ge=0.0, le=300.0)
    heading_deg: float = Field(0.0, ge=0.0, le=360.0)
    status: Literal["available", "delivering", "idle", "unknown"] = "unknown"
    accuracy_m: float = Field(0.0, ge=0.0, le=10000.0)
    battery_pct: float = Field(0.0, ge=0.0, le=100.0)
    ts: str = Field(..., min_length=1)


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
        )
        return position, None
    except ValidationError as exc:
        return None, {
            "reason": exc.errors(include_url=False, include_input=False),
            "raw_size": len(raw_value),
            "ts_rejected": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    except Exception as exc:
        # Backward compatibility: tolerate legacy json payloads if present.
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
            )
            return position, None
        except Exception:
            return None, {
                "reason": str(exc),
                "raw_size": len(raw_value),
                "ts_rejected": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }


async def push_dlq(r: aioredis.Redis, rejects: list[dict]) -> None:
    if not rejects:
        return
    pipe = r.pipeline(transaction=False)
    for item in rejects:
        pipe.lpush(DLQ_KEY, json.dumps(item, default=str))
    pipe.ltrim(DLQ_KEY, 0, DLQ_MAX_LEN - 1)
    pipe.incrby(STATS_DLQ_KEY, len(rejects))
    await pipe.execute()


async def flush_to_redis(r: aioredis.Redis, batch: list[Position]) -> None:
    now_ms = int(time.time() * 1000)
    pipe = r.pipeline(transaction=False)
    for msg in batch:
        courier_id = msg.courier_id
        pipe.geoadd(GEO_KEY, [msg.lon, msg.lat, courier_id])
        pipe.zadd(GEO_TS_KEY, {courier_id: now_ms})
        pipe.hset(
            f"{HASH_PREFIX}{courier_id}",
            mapping={
                "lat": msg.lat,
                "lon": msg.lon,
                "speed_kmh": msg.speed_kmh,
                "heading_deg": msg.heading_deg,
                "status": msg.status,
                "accuracy_m": msg.accuracy_m,
                "battery_pct": msg.battery_pct,
                "ts": msg.ts,
            },
        )
        pipe.expire(f"{HASH_PREFIX}{courier_id}", TTL_SECONDS)

    pipe.incrby(STATS_MSGS_KEY, len(batch))
    await pipe.execute()


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
            rejects: list[dict] = []
            for value in values:
                pos, reject = parse_position(value)
                if pos is not None:
                    valid.append(pos)
                elif reject is not None:
                    rejects.append(reject)

            if rejects:
                await push_dlq(redis_client, rejects)
                rejected_total += len(rejects)
            if valid:
                await flush_to_redis(redis_client, valid)
                processed += len(valid)

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
