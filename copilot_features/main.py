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
KM_TO_MILES = 0.621371
FUEL_PRICE_USD_GALLON = float(os.getenv("COPILOT_FUEL_PRICE_USD_GALLON", "3.65"))
VEHICLE_MPG = float(os.getenv("COPILOT_VEHICLE_MPG", "31.0"))
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


def _safe_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_source_metadata(source: str) -> tuple[str, dict[str, str]]:
    raw = str(source or "").strip()
    if not raw:
        return "", {}
    if ";" not in raw:
        return raw, {}

    base, *metadata = raw.split(";")
    source_tag = base.strip()
    parsed: dict[str, str] = {}
    for item in metadata:
        key, sep, value = item.strip().partition("=")
        if not sep:
            continue
        k = key.strip().lower()
        if not k:
            continue
        parsed[k] = value.strip()
    return source_tag, parsed


def _metadata_float(
    metadata: dict[str, str],
    key: str,
    *,
    default: float = 0.0,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = metadata.get(key.strip().lower())
    try:
        out = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        out = float(default)
    if not math.isfinite(out):
        out = float(default)
    if min_value is not None:
        out = max(float(min_value), out)
    if max_value is not None:
        out = min(float(max_value), out)
    return float(out)


def _metadata_int(
    metadata: dict[str, str],
    key: str,
    *,
    default: int = 0,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw = metadata.get(key.strip().lower())
    try:
        out = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        out = int(default)
    if min_value is not None:
        out = max(int(min_value), out)
    if max_value is not None:
        out = min(int(max_value), out)
    return int(out)


def _metadata_bool(metadata: dict[str, str], key: str, *, default: bool = False) -> bool:
    raw = metadata.get(key.strip().lower())
    if raw is None:
        return bool(default)
    return _safe_bool(raw, default=default)


def _parse_source_event_pressure(source: str) -> tuple[str, float]:
    source_tag, metadata = _parse_source_metadata(source)
    pressure = _metadata_float(metadata, "event_pressure", default=0.0, min_value=0.0, max_value=2.0)
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
    event_count_nearby = max(0, int(_safe_float(zone_state.get("event_count_nearby"), 0.0)))
    weather_precip_mm = max(0.0, _safe_float(zone_state.get("weather_precip_mm"), 0.0))
    weather_wind_kmh = max(0.0, _safe_float(zone_state.get("weather_wind_kmh"), 0.0))
    weather_intensity = max(0.0, min(_safe_float(zone_state.get("weather_intensity"), 0.0), 1.0))
    temporal_hour_local = _safe_float(zone_state.get("temporal_hour_local"), -1.0)
    is_peak_hour = 1 if _safe_bool(zone_state.get("is_peak_hour"), False) else 0
    is_weekend = 1 if _safe_bool(zone_state.get("is_weekend"), False) else 0
    is_holiday = 1 if _safe_bool(zone_state.get("is_holiday"), False) else 0
    temporal_pressure = max(0.0, _safe_float(zone_state.get("temporal_pressure"), 0.0))
    context_fallback_applied = 1 if _safe_bool(zone_state.get("context_fallback_applied"), False) else 0
    context_stale_sources = max(0, int(_safe_float(zone_state.get("context_stale_sources"), 0.0)))
    freshness_policy = str(zone_state.get("freshness_policy") or "")
    age_gbfs_s = _safe_float(zone_state.get("age_gbfs_s"), -1.0)
    age_weather_s = _safe_float(zone_state.get("age_weather_s"), -1.0)
    age_nyc311_s = _safe_float(zone_state.get("age_nyc311_s"), -1.0)
    age_events_s = _safe_float(zone_state.get("age_events_s"), -1.0)
    age_dot_closure_s = _safe_float(zone_state.get("age_dot_closure_s"), -1.0)
    age_dot_speeds_s = _safe_float(zone_state.get("age_dot_speeds_s"), -1.0)
    stale_weather = 1 if _safe_bool(zone_state.get("stale_weather"), False) else 0
    stale_events = 1 if _safe_bool(zone_state.get("stale_events"), False) else 0

    # Enrich with GBFS demand boost (Citi Bike station availability as taxi demand proxy)
    gbfs_demand_boost = max(0.0, _safe_float(zone_state.get("gbfs_demand_boost"), 0.0))
    demand_index = demand_index + gbfs_demand_boost

    distance_total_km = distance_to_pickup_km + max(_safe_float(offer.estimated_distance_km), 0.0)
    fuel_cost_usd = (distance_total_km * KM_TO_MILES / max(VEHICLE_MPG, 1.0)) * max(FUEL_PRICE_USD_GALLON, 0.0)
    net_revenue_eur = max(-2.0, _safe_float(offer.estimated_fare_eur) - fuel_cost_usd)

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
        "event_count_nearby": int(event_count_nearby),
        "weather_precip_mm": round(weather_precip_mm, 3),
        "weather_wind_kmh": round(weather_wind_kmh, 3),
        "weather_intensity": round(weather_intensity, 4),
        "temporal_hour_local": round(temporal_hour_local, 3) if temporal_hour_local >= 0.0 else -1.0,
        "is_peak_hour": int(is_peak_hour),
        "is_weekend": int(is_weekend),
        "is_holiday": int(is_holiday),
        "temporal_pressure": round(temporal_pressure, 4),
        "context_fallback_applied": int(context_fallback_applied),
        "context_stale_sources": int(context_stale_sources),
        "freshness_policy": freshness_policy,
        "age_gbfs_s": round(age_gbfs_s, 3),
        "age_weather_s": round(age_weather_s, 3),
        "age_nyc311_s": round(age_nyc311_s, 3),
        "age_events_s": round(age_events_s, 3),
        "age_dot_closure_s": round(age_dot_closure_s, 3),
        "age_dot_speeds_s": round(age_dot_speeds_s, 3),
        "stale_weather": int(stale_weather),
        "stale_events": int(stale_events),
        "pressure_ratio": round(pressure_ratio, 3),
        "fuel_cost_usd": round(fuel_cost_usd, 3),
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
    source_tag, metadata = _parse_source_metadata(signal_msg.source)
    event_pressure = _metadata_float(metadata, "event_pressure", default=0.0, min_value=0.0, max_value=2.0)
    event_count_nearby = _metadata_int(metadata, "event_count_nearby", default=0, min_value=0, max_value=5000)
    weather_precip_mm = _metadata_float(metadata, "weather_precip_mm", default=0.0, min_value=0.0, max_value=200.0)
    weather_wind_kmh = _metadata_float(metadata, "weather_wind_kmh", default=0.0, min_value=0.0, max_value=250.0)
    weather_intensity = _metadata_float(metadata, "weather_intensity", default=0.0, min_value=0.0, max_value=1.0)
    temporal_hour_local = _metadata_float(
        metadata, "temporal_hour_local", default=-1.0, min_value=-1.0, max_value=24.0
    )
    is_peak_hour = 1 if _metadata_bool(metadata, "temporal_is_peak", default=False) else 0
    is_weekend = 1 if _metadata_bool(metadata, "temporal_is_weekend", default=False) else 0
    is_holiday = 1 if _metadata_bool(metadata, "temporal_is_holiday", default=False) else 0
    temporal_pressure = _metadata_float(metadata, "temporal_pressure", default=0.0, min_value=0.0, max_value=2.0)
    context_fallback_applied = 1 if _metadata_bool(metadata, "freshness_fallback_applied", default=False) else 0
    context_stale_sources = _metadata_int(metadata, "freshness_stale_sources", default=0, min_value=0, max_value=12)
    freshness_policy = str(metadata.get("freshness_policy") or "").strip()
    age_gbfs_s = _metadata_float(metadata, "age_gbfs_s", default=-1.0)
    age_weather_s = _metadata_float(metadata, "age_weather_s", default=-1.0)
    age_nyc311_s = _metadata_float(metadata, "age_nyc311_s", default=-1.0)
    age_events_s = _metadata_float(metadata, "age_events_s", default=-1.0)
    age_dot_closure_s = _metadata_float(metadata, "age_dot_closure_s", default=-1.0)
    age_dot_speeds_s = _metadata_float(metadata, "age_dot_speeds_s", default=-1.0)
    stale_gbfs = 1 if _metadata_bool(metadata, "stale_gbfs", default=False) else 0
    stale_weather = 1 if _metadata_bool(metadata, "stale_weather", default=False) else 0
    stale_nyc311 = 1 if _metadata_bool(metadata, "stale_nyc311", default=False) else 0
    stale_events = 1 if _metadata_bool(metadata, "stale_events", default=False) else 0
    stale_dot_closure = 1 if _metadata_bool(metadata, "stale_dot_closure", default=False) else 0
    stale_dot_speeds = 1 if _metadata_bool(metadata, "stale_dot_speeds", default=False) else 0
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
            "event_count_nearby": int(event_count_nearby),
            "weather_precip_mm": round(float(weather_precip_mm), 3),
            "weather_wind_kmh": round(float(weather_wind_kmh), 3),
            "weather_intensity": round(float(weather_intensity), 4),
            "temporal_hour_local": round(float(temporal_hour_local), 3),
            "is_peak_hour": int(is_peak_hour),
            "is_weekend": int(is_weekend),
            "is_holiday": int(is_holiday),
            "temporal_pressure": round(float(temporal_pressure), 4),
            "context_fallback_applied": int(context_fallback_applied),
            "context_stale_sources": int(context_stale_sources),
            "freshness_policy": freshness_policy,
            "age_gbfs_s": round(float(age_gbfs_s), 3),
            "age_weather_s": round(float(age_weather_s), 3),
            "age_nyc311_s": round(float(age_nyc311_s), 3),
            "age_events_s": round(float(age_events_s), 3),
            "age_dot_closure_s": round(float(age_dot_closure_s), 3),
            "age_dot_speeds_s": round(float(age_dot_speeds_s), 3),
            "stale_gbfs": int(stale_gbfs),
            "stale_weather": int(stale_weather),
            "stale_nyc311": int(stale_nyc311),
            "stale_events": int(stale_events),
            "stale_dot_closure": int(stale_dot_closure),
            "stale_dot_speeds": int(stale_dot_speeds),
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
