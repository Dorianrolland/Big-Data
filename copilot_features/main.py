"""
Realtime copilot feature consumer.

Consumes offers + context signals and materializes feature vectors in Redis for API scoring.
"""
import asyncio
import json
import logging
import math
import os
import signal
import time

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

from copilot_events_pb2 import ContextSignalV1, OrderOfferV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("copilot-features")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
ORDER_OFFERS_TOPIC = os.getenv("ORDER_OFFERS_TOPIC", "order-offers-v1")
CONTEXT_SIGNALS_TOPIC = os.getenv("CONTEXT_SIGNALS_TOPIC", "context-signals-v1")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

OFFER_TTL_SECONDS = int(os.getenv("COPILOT_OFFER_TTL_SECONDS", "21600"))
MAX_OFFERS_PER_DRIVER = int(os.getenv("COPILOT_MAX_OFFERS_PER_DRIVER", "100"))
FUEL_COST_EUR_PER_KM = float(os.getenv("FUEL_COST_EUR_PER_KM", "0.35"))
CONSUMER_GROUP = "copilot-features"

COURIER_HASH_PREFIX = "fleet:livreur:"
OFFER_KEY_PREFIX = "copilot:offer:"
DRIVER_OFFERS_PREFIX = "copilot:driver:"
ZONE_CONTEXT_PREFIX = "copilot:context:zone:"
STATS_FEATURES = "copilot:stats:offers_materialized"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.asin(math.sqrt(a))


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _parse_source_event_pressure(source: str) -> tuple[str, float]:
    raw = str(source or "").strip()
    if not raw:
        return "", 0.0
    if ";" not in raw:
        return raw, 0.0

    base, *metadata = raw.split(";")
    source_tag = base.strip()
    pressure = 0.0
    for item in metadata:
        key, sep, value = item.strip().partition("=")
        if not sep:
            continue
        if key.strip().lower() != "event_pressure":
            continue
        try:
            pressure = float(value.strip())
        except (TypeError, ValueError):
            pressure = 0.0
        if not math.isfinite(pressure):
            pressure = 0.0
        break
    pressure = max(0.0, min(2.0, pressure))
    return source_tag, pressure


async def enrich_offer(redis_client: aioredis.Redis, offer: OrderOfferV1) -> dict:
    courier_state = await redis_client.hgetall(f"{COURIER_HASH_PREFIX}{offer.courier_id}")

    courier_lat = _safe_float(courier_state.get("lat"), float(offer.pickup_lat))
    courier_lon = _safe_float(courier_state.get("lon"), float(offer.pickup_lon))
    courier_speed = max(_safe_float(courier_state.get("speed_kmh"), 18.0), 6.0)
    courier_status = courier_state.get("status", "unknown")

    distance_to_pickup_km = max(0.0, haversine_km(courier_lat, courier_lon, offer.pickup_lat, offer.pickup_lon))
    eta_to_pickup_min = (distance_to_pickup_km / courier_speed) * 60

    zone_key = f"{ZONE_CONTEXT_PREFIX}{offer.zone_id}"
    zone_state = await redis_client.hgetall(zone_key)
    demand_index = _safe_float(zone_state.get("demand_index"), float(offer.demand_index or 1.0))
    supply_index = max(_safe_float(zone_state.get("supply_index"), 1.0), 0.2)
    weather_factor = _safe_float(zone_state.get("weather_factor"), float(offer.weather_factor or 1.0))
    traffic_factor = max(_safe_float(zone_state.get("traffic_factor"), float(offer.traffic_factor or 1.0)), 0.2)
    event_pressure = max(0.0, _safe_float(zone_state.get("event_pressure"), 0.0))

    # Enrich with GBFS demand boost (Citi Bike station availability as taxi demand proxy)
    gbfs_demand_boost = max(0.0, _safe_float(zone_state.get("gbfs_demand_boost"), 0.0))
    demand_index = demand_index + gbfs_demand_boost

    distance_total_km = distance_to_pickup_km + max(_safe_float(offer.estimated_distance_km), 0.0)
    variable_cost_eur = distance_total_km * FUEL_COST_EUR_PER_KM
    net_revenue_eur = max(-2.0, _safe_float(offer.estimated_fare_eur) - variable_cost_eur)

    total_trip_time_min = max(1.0, _safe_float(offer.estimated_duration_min) + eta_to_pickup_min)
    eur_per_hour_net = (net_revenue_eur / total_trip_time_min) * 60

    pressure_ratio = demand_index / max(supply_index, 0.2)
    score_logits = (
        (eur_per_hour_net - 16.0) / 10.0
        + (pressure_ratio - 1.0) * 0.9
        + (weather_factor - 1.0) * 0.4
        - (traffic_factor - 1.0) * 0.35
    )
    accept_score = max(0.01, min(sigmoid(score_logits), 0.99))

    explanation = []
    if eur_per_hour_net >= 22:
        explanation.append("high_net_revenue")
    elif eur_per_hour_net < 14:
        explanation.append("low_net_revenue")

    if pressure_ratio >= 1.25:
        explanation.append("high_demand_pressure")
    elif pressure_ratio < 0.9:
        explanation.append("low_demand_pressure")

    if traffic_factor > 1.2:
        explanation.append("traffic_penalty")
    if courier_status == "delivering":
        explanation.append("courier_busy")

    return {
        "offer_id": offer.offer_id,
        "courier_id": offer.courier_id,
        "zone_id": offer.zone_id,
        "ts": offer.ts,
        "courier_lat": round(float(courier_lat), 6),
        "courier_lon": round(float(courier_lon), 6),
        "pickup_lat": round(float(offer.pickup_lat), 6),
        "pickup_lon": round(float(offer.pickup_lon), 6),
        "dropoff_lat": round(float(offer.dropoff_lat), 6),
        "dropoff_lon": round(float(offer.dropoff_lon), 6),
        "estimated_fare_eur": round(float(offer.estimated_fare_eur), 3),
        "estimated_distance_km": round(float(offer.estimated_distance_km), 3),
        "estimated_duration_min": round(float(offer.estimated_duration_min), 3),
        "distance_to_pickup_km": round(distance_to_pickup_km, 3),
        "eta_to_pickup_min": round(eta_to_pickup_min, 3),
        "demand_index": round(demand_index, 3),
        "supply_index": round(supply_index, 3),
        "weather_factor": round(weather_factor, 3),
        "traffic_factor": round(traffic_factor, 3),
        "event_pressure": round(event_pressure, 4),
        "pressure_ratio": round(pressure_ratio, 3),
        "eur_per_hour_net": round(eur_per_hour_net, 3),
        "accept_score_heuristic": round(accept_score, 4),
        "explanation": json.dumps(explanation),
        "source": "copilot-features",
    }


async def store_offer(redis_client: aioredis.Redis, feature_map: dict) -> None:
    offer_id = feature_map["offer_id"]
    courier_id = feature_map["courier_id"]

    offer_key = f"{OFFER_KEY_PREFIX}{offer_id}"
    driver_list = f"{DRIVER_OFFERS_PREFIX}{courier_id}:offers"

    pipe = redis_client.pipeline(transaction=False)
    pipe.hset(offer_key, mapping=feature_map)
    pipe.expire(offer_key, OFFER_TTL_SECONDS)
    pipe.lpush(driver_list, offer_id)
    pipe.ltrim(driver_list, 0, MAX_OFFERS_PER_DRIVER - 1)
    pipe.expire(driver_list, OFFER_TTL_SECONDS)
    pipe.incr(STATS_FEATURES)
    await pipe.execute()


async def upsert_context(redis_client: aioredis.Redis, signal_msg: ContextSignalV1) -> None:
    key = f"{ZONE_CONTEXT_PREFIX}{signal_msg.zone_id}"
    source_tag, event_pressure = _parse_source_event_pressure(signal_msg.source)
    prev_demand_raw, prev_trend_ema_raw, prev_updated_raw = await redis_client.hmget(
        key, ["demand_index", "demand_trend_ema", "updated_at"]
    )
    demand_index = round(float(signal_msg.demand_index), 3)

    try:
        prev_demand = float(prev_demand_raw) if prev_demand_raw is not None else demand_index
    except (TypeError, ValueError):
        prev_demand = demand_index
    try:
        prev_trend_ema = float(prev_trend_ema_raw) if prev_trend_ema_raw is not None else 0.0
    except (TypeError, ValueError):
        prev_trend_ema = 0.0

    demand_trend = demand_index - prev_demand
    demand_trend_ema = (0.65 * demand_trend) + (0.35 * prev_trend_ema)
    forecast_demand_15m = max(0.3, demand_index + (demand_trend_ema * 1.4))

    now_s = time.time()
    try:
        prev_updated = float(prev_updated_raw) if prev_updated_raw is not None else None
    except (TypeError, ValueError):
        prev_updated = None
    context_tick_s = round(max(0.0, now_s - prev_updated), 3) if prev_updated else None

    await redis_client.hset(
        key,
        mapping={
            "ts": signal_msg.ts,
            "zone_id": signal_msg.zone_id,
            "demand_index": demand_index,
            "supply_index": round(float(signal_msg.supply_index), 3),
            "weather_factor": round(float(signal_msg.weather_factor), 3),
            "traffic_factor": round(float(signal_msg.traffic_factor), 3),
            "demand_trend": round(float(demand_trend), 3),
            "demand_trend_ema": round(float(demand_trend_ema), 3),
            "forecast_demand_index_15m": round(float(forecast_demand_15m), 3),
            "context_tick_s": context_tick_s if context_tick_s is not None else "",
            "event_pressure": round(float(event_pressure), 4),
            "source": source_tag or signal_msg.source,
            "updated_at": str(now_s),
        },
    )
    await redis_client.expire(key, OFFER_TTL_SECONDS)


async def main() -> None:
    stop_event = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("signal received, stopping copilot-features")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    consumer = AIOKafkaConsumer(
        ORDER_OFFERS_TOPIC,
        CONTEXT_SIGNALS_TOPIC,
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
            log.info("connected to topics=%s,%s", ORDER_OFFERS_TOPIC, CONTEXT_SIGNALS_TOPIC)
            break
        except Exception as exc:  # pragma: no cover
            log.warning("attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to connect to kafka")
        return

    offers_count = 0
    context_count = 0

    try:
        while not stop_event.is_set():
            raw = await consumer.getmany(timeout_ms=200, max_records=500)
            if not raw:
                continue

            for tp, msgs in raw.items():
                if not msgs:
                    continue
                for msg in msgs:
                    try:
                        if tp.topic == ORDER_OFFERS_TOPIC:
                            offer = OrderOfferV1()
                            offer.ParseFromString(msg.value)
                            feature_map = await enrich_offer(redis_client, offer)
                            await store_offer(redis_client, feature_map)
                            offers_count += 1
                        elif tp.topic == CONTEXT_SIGNALS_TOPIC:
                            signal_msg = ContextSignalV1()
                            signal_msg.ParseFromString(msg.value)
                            await upsert_context(redis_client, signal_msg)
                            context_count += 1
                    except Exception as exc:
                        log.warning("failed to process topic=%s err=%s", tp.topic, exc)

            if offers_count and offers_count % 1000 == 0:
                log.info("features materialized=%d context_updates=%d", offers_count, context_count)
    finally:
        await consumer.stop()
        await redis_client.aclose()
        log.info("copilot-features stopped offers=%d context=%d", offers_count, context_count)


if __name__ == "__main__":
    asyncio.run(main())
