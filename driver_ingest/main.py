"""
FleetStream driver ingest gateway.

Receives live courier GPS from mobile clients and publishes protobuf
CourierPositionV1 events to Kafka topic `livreurs-gps`.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, Field

from copilot_events_pb2 import CourierPositionV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("driver-ingest")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
COURIER_TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
REDIS_CONNECT_TIMEOUT_SECONDS = float(os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "1.5"))
REDIS_OP_TIMEOUT_SECONDS = float(os.getenv("REDIS_OP_TIMEOUT_SECONDS", "2.0"))
INGEST_RATE_LIMIT_PER_MIN = int(os.getenv("DRIVER_INGEST_RATE_LIMIT_PER_MIN", "1800"))
INGEST_MAX_BATCH_SIZE = int(os.getenv("DRIVER_INGEST_MAX_BATCH_SIZE", "200"))
INGEST_EVENT_PREFIX = os.getenv("DRIVER_INGEST_EVENT_PREFIX", "drv_ingest")
INGEST_COMPRESSION = (os.getenv("DRIVER_INGEST_COMPRESSION", "gzip") or "").strip() or None

_TOKEN_MAP_JSON = (os.getenv("DRIVER_INGEST_TOKENS_JSON", "") or "").strip()
_SINGLE_TOKEN = (os.getenv("DRIVER_INGEST_TOKEN", "") or "").strip()
_ALLOW_INSECURE_DEV = (os.getenv("DRIVER_INGEST_ALLOW_INSECURE_DEV", "false") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalized_ts(value: str | None) -> str:
    if not value:
        return _now_iso()
    raw = value.strip()
    if not raw:
        return _now_iso()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return _now_iso()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def _parse_token_map() -> dict[str, str]:
    if _TOKEN_MAP_JSON:
        try:
            payload = json.loads(_TOKEN_MAP_JSON)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid DRIVER_INGEST_TOKENS_JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("DRIVER_INGEST_TOKENS_JSON must be a json object {token: courier_scope}")
        out: dict[str, str] = {}
        for token, scope in payload.items():
            t = str(token).strip()
            if not t:
                continue
            s = str(scope).strip() if scope is not None else "*"
            out[t] = s or "*"
        return out
    if _SINGLE_TOKEN:
        return {_SINGLE_TOKEN: "*"}
    if _ALLOW_INSECURE_DEV:
        return {"dev-insecure-token": "*"}
    raise RuntimeError(
        "No ingest token configured. Set DRIVER_INGEST_TOKENS_JSON or DRIVER_INGEST_TOKEN "
        "(or DRIVER_INGEST_ALLOW_INSECURE_DEV=true for local dev only)."
    )


TOKEN_MAP = _parse_token_map()
TOKEN_HASHES = {hashlib.sha256(token.encode()).hexdigest()[:10] for token in TOKEN_MAP}


class PositionIn(BaseModel):
    courier_id: str = Field(..., min_length=1, max_length=64)
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed_kmh: float = Field(0.0, ge=0.0, le=300.0)
    heading_deg: float = Field(0.0, ge=0.0, le=360.0)
    status: Literal["available", "delivering", "idle", "pickup_arrived", "unknown"] = "unknown"
    accuracy_m: float = Field(8.0, ge=0.0, le=10000.0)
    battery_pct: float = Field(100.0, ge=0.0, le=100.0)
    source_platform: str = Field("driver_ingest", min_length=2, max_length=64)
    ts: str | None = None


class PositionsBatchIn(BaseModel):
    positions: list[PositionIn] = Field(..., min_length=1, max_length=INGEST_MAX_BATCH_SIZE)


class IngestAck(BaseModel):
    accepted: int
    failed: int
    event_ids: list[str]


def _extract_bearer(auth_header: str | None, fallback_token: str | None) -> str | None:
    if auth_header:
        raw = auth_header.strip()
        if raw.lower().startswith("bearer "):
            return raw[7:].strip()
        return raw
    if fallback_token:
        return fallback_token.strip()
    return None


async def _check_rate_limit(redis_client: aioredis.Redis | None, token: str) -> None:
    if redis_client is None or INGEST_RATE_LIMIT_PER_MIN <= 0:
        return
    bucket_min = int(time.time() // 60)
    token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
    key = f"ingest:ratelimit:{token_hash}:{bucket_min}"
    current = await redis_client.incr(key)
    if current == 1:
        await redis_client.expire(key, 120)
    if current > INGEST_RATE_LIMIT_PER_MIN:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded ({INGEST_RATE_LIMIT_PER_MIN}/min)",
        )


async def _authorize_ingest(
    request: Request,
    position: PositionIn,
    authorization: str | None = Header(default=None),
    x_driver_token: str | None = Header(default=None),
) -> str:
    token = _extract_bearer(authorization, x_driver_token)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    scope = TOKEN_MAP.get(token)
    if scope is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    if scope != "*" and scope != position.courier_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token scope mismatch: expected '{scope}', got '{position.courier_id}'",
        )

    redis_client: aioredis.Redis | None = getattr(request.app.state, "redis", None)
    await _check_rate_limit(redis_client, token)
    return token


async def _authorize_batch(
    request: Request,
    batch: PositionsBatchIn,
    authorization: str | None = Header(default=None),
    x_driver_token: str | None = Header(default=None),
) -> str:
    token = _extract_bearer(authorization, x_driver_token)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    scope = TOKEN_MAP.get(token)
    if scope is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    if scope != "*":
        mismatch = next((p.courier_id for p in batch.positions if p.courier_id != scope), None)
        if mismatch is not None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Token scope mismatch: expected '{scope}', got '{mismatch}'",
            )

    redis_client: aioredis.Redis | None = getattr(request.app.state, "redis", None)
    await _check_rate_limit(redis_client, token)
    return token


def _to_proto(position: PositionIn) -> tuple[str, bytes]:
    ts = _normalized_ts(position.ts)
    event_id = f"{INGEST_EVENT_PREFIX}_{uuid.uuid4().hex[:22]}"
    payload = CourierPositionV1(
        event_id=event_id,
        event_type="courier.position.v1",
        ts=ts,
        courier_id=position.courier_id,
        lat=float(position.lat),
        lon=float(position.lon),
        speed_kmh=float(position.speed_kmh),
        heading_deg=float(position.heading_deg),
        status=position.status,
        accuracy_m=float(position.accuracy_m),
        battery_pct=float(position.battery_pct),
        source_platform=position.source_platform,
    )
    return event_id, payload.SerializeToString()


async def _publish_positions(
    producer: AIOKafkaProducer,
    positions: list[PositionIn],
) -> IngestAck:
    event_ids: list[str] = []
    send_futures = []
    for pos in positions:
        event_id, encoded = _to_proto(pos)
        event_ids.append(event_id)
        send_futures.append(
            producer.send(
                COURIER_TOPIC,
                key=pos.courier_id.encode("utf-8", errors="ignore"),
                value=encoded,
            )
        )

    results = await asyncio.gather(*send_futures, return_exceptions=True)
    failed = sum(1 for item in results if isinstance(item, Exception))
    accepted = len(results) - failed
    return IngestAck(accepted=accepted, failed=failed, event_ids=event_ids[:accepted])


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type=INGEST_COMPRESSION,
        acks=1,
        linger_ms=5,
        max_batch_size=262_144,
    )
    await producer.start()
    app.state.producer = producer

    redis_client = aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_OP_TIMEOUT_SECONDS,
        retry_on_timeout=True,
    )
    try:
        await redis_client.ping()
        app.state.redis = redis_client
        log.info("driver-ingest online kafka=%s topic=%s redis=%s", KAFKA_BOOTSTRAP, COURIER_TOPIC, REDIS_URL)
        log.info("driver-ingest token hashes loaded=%s", sorted(TOKEN_HASHES))
        yield
    finally:
        try:
            await producer.stop()
        finally:
            await redis_client.aclose()


app = FastAPI(
    title="FleetStream Driver Ingest Gateway",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/healthz")
async def healthz(request: Request):
    redis_ok = False
    redis_client: aioredis.Redis | None = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        try:
            redis_ok = bool(await redis_client.ping())
        except Exception:
            redis_ok = False

    return {
        "status": "ok",
        "kafka_bootstrap": KAFKA_BOOTSTRAP,
        "topic": COURIER_TOPIC,
        "redis_ok": redis_ok,
        "rate_limit_per_min": INGEST_RATE_LIMIT_PER_MIN,
        "max_batch_size": INGEST_MAX_BATCH_SIZE,
        "token_hashes": sorted(TOKEN_HASHES),
        "ts": _now_iso(),
    }


@app.post("/ingest/v1/position", response_model=IngestAck)
async def ingest_position(
    request: Request,
    position: PositionIn,
    _token: str = Depends(_authorize_ingest),
):
    producer: AIOKafkaProducer = request.app.state.producer
    ack = await _publish_positions(producer, [position])
    if ack.failed > 0:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"kafka publish partial failure: accepted={ack.accepted} failed={ack.failed}",
        )
    return ack


@app.post("/ingest/v1/positions", response_model=IngestAck)
async def ingest_positions(
    request: Request,
    body: PositionsBatchIn,
    _token: str = Depends(_authorize_batch),
):
    if len(body.positions) > INGEST_MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch too large, max={INGEST_MAX_BATCH_SIZE}",
        )
    producer: AIOKafkaProducer = request.app.state.producer
    ack = await _publish_positions(producer, body.positions)
    if ack.failed > 0:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"kafka publish partial failure: accepted={ack.accepted} failed={ack.failed}",
        )
    return ack


@app.get("/")
async def root():
    return {
        "service": "driver-ingest",
        "usage": {
            "single": "POST /ingest/v1/position",
            "batch": "POST /ingest/v1/positions",
            "health": "GET /healthz",
            "auth": "Authorization: Bearer <token> or X-Driver-Token",
        },
    }
