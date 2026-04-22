"""
NYC TLC HVFHV (High Volume For-Hire Vehicle) trip replay service.

Downloads the monthly Uber trip parquet from the NYC Taxi & Limousine Commission,
filters Uber trips (HV0003), sorts them by request_datetime, then drives a
tick-based virtual clock that emits 100% real-data events to Kafka:

- OrderOfferV1             when a trip's request_datetime is reached
- OrderEventV1(accepted)   immediately after the offer
- CourierPositionV1        at pickup, interpolated positions during the trip,
                           and final position at dropoff
- OrderEventV1(dropped_off) at dropoff_datetime

Zone ID -> (lat, lon) centroid lookup is loaded from nyc_zone_centroids.json
(pre-computed from the official NYC TLC taxi_zones shapefile; 263 zones).

This service is the sole source of CourierPositionV1 / OrderOfferV1 /
OrderEventV1 traffic for the platform. There is no simulation layer.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import signal
from collections import deque
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

from copilot_events_pb2 import CourierPositionV1, OrderEventV1, OrderOfferV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("tlc-replay")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
COURIER_TOPIC = os.getenv("KAFKA_TOPIC", "livreurs-gps")
ORDER_OFFERS_TOPIC = os.getenv("ORDER_OFFERS_TOPIC", "order-offers-v1")
ORDER_EVENTS_TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "order-events-v1")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

TLC_DATA_DIR = Path(os.getenv("TLC_DATA_DIR", "/data/tlc"))
TLC_MONTH = os.getenv("TLC_MONTH", "2024-01").strip()
TLC_MONTHS_RAW = os.getenv("TLC_MONTHS", "").strip()
TLC_MONTH_COUNT_RAW = os.getenv("TLC_MONTH_COUNT", "0").strip()
TLC_BASE_URL = os.getenv(
    "TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data",
).rstrip("/")
TLC_LICENSE_FILTER = os.getenv("TLC_LICENSE_FILTER", "HV0003").strip()  # HV0003 = Uber
TLC_SPEED_FACTOR = float(os.getenv("TLC_SPEED_FACTOR", "1"))
TLC_TICK_INTERVAL_SEC = float(os.getenv("TLC_TICK_INTERVAL_SEC", "5"))
TLC_TRIP_SAMPLE_RATE = float(os.getenv("TLC_TRIP_SAMPLE_RATE", "0.15"))
TLC_MAX_ACTIVE_TRIPS = int(os.getenv("TLC_MAX_ACTIVE_TRIPS", "800"))
TLC_INGEST_BATCH_SIZE = int(os.getenv("TLC_INGEST_BATCH_SIZE", "2000"))
TLC_LOOP_ON_FINISH = os.getenv("TLC_LOOP_ON_FINISH", "true").lower() in {"1", "true", "yes", "on"}
TLC_START_HOUR_UTC_RAW = (os.getenv("TLC_START_HOUR_UTC", "") or "").strip()

TLC_SCENARIO = os.getenv("TLC_SCENARIO", "single_driver").strip().lower() or "single_driver"
TLC_RESET_RUNTIME_ON_START = os.getenv("TLC_RESET_RUNTIME_ON_START", "false").lower() in {
    "1", "true", "yes", "on",
}
TLC_TRAIN_MONTH_COUNT = int(os.getenv("TLC_TRAIN_MONTH_COUNT", "0") or "0")
TLC_LIVE_MONTH_COUNT = int(os.getenv("TLC_LIVE_MONTH_COUNT", "0") or "0")
TLC_SOURCE_PLATFORM = (os.getenv("TLC_SOURCE_PLATFORM", "tlc_hvfhv_historical") or "tlc_hvfhv_historical").strip()
TLC_ROUTE_MODE = (os.getenv("TLC_ROUTE_MODE", "osrm") or "osrm").strip().lower()
TLC_ROUTE_OSRM_URL = (os.getenv("TLC_ROUTE_OSRM_URL", os.getenv("OSRM_URL", "http://osrm:5000")) or "http://osrm:5000").strip().rstrip("/")
TLC_ROUTE_OSRM_FALLBACK_URL = (
    os.getenv(
        "TLC_ROUTE_OSRM_FALLBACK_URL",
        os.getenv("ROUTING_OSRM_PUBLIC_URL", "https://router.project-osrm.org"),
    )
    or ""
).strip().rstrip("/")
TLC_ROUTE_VALHALLA_URL = (
    os.getenv(
        "TLC_ROUTE_VALHALLA_URL",
        "https://valhalla1.openstreetmap.de",
    )
    or ""
).strip().rstrip("/")
TLC_ROUTE_OSRM_TIMEOUT_S = float(os.getenv("TLC_ROUTE_OSRM_TIMEOUT_S", "4.0"))
TLC_ROUTE_FETCH_TIMEOUT_S = float(os.getenv("TLC_ROUTE_FETCH_TIMEOUT_S", "2.5"))
TLC_ROUTE_SYNC_ON_CACHE_MISS = os.getenv(
    "TLC_ROUTE_SYNC_ON_CACHE_MISS",
    "false" if TLC_SCENARIO == "fleet" else "true",
).lower() in {"1", "true", "yes", "on"}
TLC_ROUTE_SYNC_KEY_REPEAT_THRESHOLD = max(
    1,
    int(os.getenv("TLC_ROUTE_SYNC_KEY_REPEAT_THRESHOLD", "3") or "3"),
)
TLC_ROUTE_PREFETCH_ENABLED = os.getenv("TLC_ROUTE_PREFETCH_ENABLED", "true").lower() in {
    "1", "true", "yes", "on",
}
TLC_ROUTE_PREFETCH_MAX_PENDING = int(os.getenv("TLC_ROUTE_PREFETCH_MAX_PENDING", "256"))
TLC_ROUTE_PREFETCH_MAX_CONCURRENCY = max(1, int(os.getenv("TLC_ROUTE_PREFETCH_MAX_CONCURRENCY", "8")))
TLC_ROUTE_CACHE_MAX = int(os.getenv("TLC_ROUTE_CACHE_MAX", "4096"))
TLC_ROUTE_DENSIFY_MAX_STEP_KM = max(0.0, float(os.getenv("TLC_ROUTE_DENSIFY_MAX_STEP_KM", "0.05") or "0.05"))
TLC_ROUTE_FAIL_STREAK_TO_COOLDOWN = max(1, int(os.getenv("TLC_ROUTE_FAIL_STREAK_TO_COOLDOWN", "8") or "8"))
TLC_ROUTE_FAIL_COOLDOWN_S = max(0.0, float(os.getenv("TLC_ROUTE_FAIL_COOLDOWN_S", "45.0") or "45.0"))
TLC_NIGHT_ACCEL_START = (os.getenv("TLC_NIGHT_ACCEL_START", "02:00") or "02:00").strip()
TLC_NIGHT_ACCEL_END = (os.getenv("TLC_NIGHT_ACCEL_END", "08:00") or "08:00").strip()
TLC_NIGHT_ACCEL_FACTOR = max(1, int(os.getenv("TLC_NIGHT_ACCEL_FACTOR", "8") or "8"))
TLC_FLEET_DEMO_N_DRIVERS = max(1, int(os.getenv("TLC_FLEET_DEMO_N_DRIVERS", "250") or "250"))
TLC_FLEET_MAX_REPOSITION_MIN = max(
    1.0,
    float(
        os.getenv(
            "TLC_FLEET_MAX_REPOSITION_MIN",
            os.getenv("TLC_SINGLE_MAX_REPOSITION_MIN", "30"),
        )
        or os.getenv("TLC_SINGLE_MAX_REPOSITION_MIN", "30")
    ),
)
TLC_FLEET_REPOSITION_KMH = max(
    5.0,
    float(
        os.getenv(
            "TLC_FLEET_REPOSITION_KMH",
            os.getenv("TLC_SINGLE_REPOSITION_KMH", "22"),
        )
        or os.getenv("TLC_SINGLE_REPOSITION_KMH", "22")
    ),
)
TLC_FLEET_PICKUP_GRACE_MIN = max(0.0, float(os.getenv("TLC_FLEET_PICKUP_GRACE_MIN", "2.0") or "2.0"))
_DEFAULT_COURIER_MODE = "fleet_pool" if TLC_SCENARIO == "fleet" else "trip"
TLC_COURIER_ID_MODE = (os.getenv("TLC_COURIER_ID_MODE", _DEFAULT_COURIER_MODE) or _DEFAULT_COURIER_MODE).strip().lower()

STATUS_KEY = "copilot:replay:tlc:status"
CURSOR_KEY = "copilot:replay:tlc:cursor"

RUNTIME_RESET_KEYS = [
    "copilot:replay:tlc:status",
    "copilot:replay:tlc:cursor",
    "copilot:replay:tlc:single:status",
    "copilot:replay:tlc:single:cursor",
]
RUNTIME_RESET_PATTERNS = [
    "fleet:livreur:*",
    "fleet:dlq*",
    "copilot:driver:*",
    "copilot:offer:*",
    "copilot:offers:driver:*",
    "copilot:mission:*",
]
CENTROIDS_PATH = Path(__file__).parent / "nyc_zone_centroids.json"

EARTH_RADIUS_KM = 6371.0
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

if TLC_ROUTE_MODE not in {"osrm", "linear"}:
    log.warning("invalid TLC_ROUTE_MODE=%s; fallback to linear", TLC_ROUTE_MODE)
    TLC_ROUTE_MODE = "linear"
if TLC_COURIER_ID_MODE not in {"trip", "legacy_zone_day", "fleet_pool"}:
    log.warning(
        "invalid TLC_COURIER_ID_MODE=%s; fallback to trip",
        TLC_COURIER_ID_MODE,
    )
    TLC_COURIER_ID_MODE = "trip"

try:
    TLC_START_HOUR_UTC = int(TLC_START_HOUR_UTC_RAW) if TLC_START_HOUR_UTC_RAW else -1
except ValueError:
    TLC_START_HOUR_UTC = -1
if TLC_START_HOUR_UTC < -1 or TLC_START_HOUR_UTC > 23:
    log.warning("invalid TLC_START_HOUR_UTC=%s; disabling startup fast-forward", TLC_START_HOUR_UTC_RAW)
    TLC_START_HOUR_UTC = -1


def _osrm_route_targets() -> list[tuple[str, str]]:
    targets: list[tuple[str, str]] = [("osrm", TLC_ROUTE_OSRM_URL)]
    valhalla = TLC_ROUTE_VALHALLA_URL.strip()
    if valhalla and valhalla.lower() not in {"off", "none", "disabled"}:
        targets.append(("valhalla_public", valhalla))
    fallback = TLC_ROUTE_OSRM_FALLBACK_URL.strip()
    if fallback and fallback.lower() not in {"off", "none", "disabled"} and fallback != TLC_ROUTE_OSRM_URL:
        targets.append(("osrm_public", fallback))
    return targets


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_from_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def make_event_id(prefix: str, seed: str) -> str:
    return f"{prefix}_{hashlib.sha1(seed.encode()).hexdigest()[:20]}"


def format_fleet_driver_id(index_1_based: int, pool_size: int | None = None) -> str:
    total = max(1, int(pool_size or TLC_FLEET_DEMO_N_DRIVERS))
    width = max(3, len(str(total)))
    return f"drv_demo_{index_1_based:0{width}d}"


def synth_courier_id(
    base_num: str | None,
    pu_loc: int,
    do_loc: int,
    request_ts: datetime,
    pickup_ts: datetime,
    dropoff_ts: datetime,
    *,
    mode: str | None = None,
) -> str:
    base = (base_num or "B00000").strip() or "B00000"
    selected_mode = (mode or TLC_COURIER_ID_MODE).strip().lower()
    if selected_mode == "legacy_zone_day":
        day = request_ts.strftime("%Y-%m-%d")
        raw = f"legacy|{base}|{pu_loc}|{day}"
    elif selected_mode == "fleet_pool":
        raw = (
            f"fleet|{base}|{pu_loc}|{do_loc}|"
            f"{request_ts.strftime('%Y-%m-%dT%H')}"
        )
        slot = (int(hashlib.sha1(raw.encode()).hexdigest()[:8], 16) % TLC_FLEET_DEMO_N_DRIVERS) + 1
        return format_fleet_driver_id(slot, TLC_FLEET_DEMO_N_DRIVERS)
    else:
        # Default mode: one stable synthetic ID per trip instance.
        # This avoids impossible inter-trip teleports caused by the legacy
        # (base+pickup_zone+day) identity collisions.
        raw = (
            f"trip|{base}|{pu_loc}|{do_loc}|"
            f"{int(request_ts.timestamp())}|{int(pickup_ts.timestamp())}|{int(dropoff_ts.timestamp())}"
        )
    return f"tlc_drv_{hashlib.sha1(raw.encode()).hexdigest()[:12]}"


def total_fare_usd(row: dict) -> float:
    parts = [
        row.get("base_passenger_fare") or 0,
        row.get("tolls") or 0,
        row.get("bcf") or 0,
        row.get("sales_tax") or 0,
        row.get("congestion_surcharge") or 0,
        row.get("airport_fee") or 0,
    ]
    try:
        return max(0.0, float(sum(parts)))
    except (TypeError, ValueError):
        return 0.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return EARTH_RADIUS_KM * 2 * math.asin(math.sqrt(a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def project_point(lat: float, lon: float, bearing: float, distance_km: float) -> tuple[float, float]:
    """Project a point from (lat, lon) using bearing+distance on a sphere."""
    if distance_km <= 0:
        return lat, lon
    angular = distance_km / EARTH_RADIUS_KM
    brng = math.radians(bearing % 360.0)
    lat1 = math.radians(lat)
    lon1 = math.radians(lon)

    sin_lat2 = (
        math.sin(lat1) * math.cos(angular)
        + math.cos(lat1) * math.sin(angular) * math.cos(brng)
    )
    lat2 = math.asin(max(-1.0, min(1.0, sin_lat2)))
    lon2 = lon1 + math.atan2(
        math.sin(brng) * math.sin(angular) * math.cos(lat1),
        math.cos(angular) - math.sin(lat1) * math.sin(lat2),
    )
    lon_norm = ((math.degrees(lon2) + 540.0) % 360.0) - 180.0
    return math.degrees(lat2), lon_norm


def deterministic_bearing(seed: str) -> float:
    token = seed or "seed"
    value = int(hashlib.sha1(token.encode("utf-8", errors="ignore")).hexdigest()[:8], 16)
    return float(value % 360)


def decode_polyline(encoded: str, precision: int = 5) -> list[tuple[float, float]]:
    if not encoded:
        return []
    coords: list[tuple[float, float]] = []
    index = 0
    lat = 0
    lon = 0
    factor = 10 ** precision
    length = len(encoded)
    while index < length:
        result = 0
        shift = 0
        while True:
            if index >= length:
                return coords
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        result = 0
        shift = 0
        while True:
            if index >= length:
                return coords
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if (result & 1) else (result >> 1)
        lon += dlon
        coords.append((lat / factor, lon / factor))
    return coords


def cumulative_route_distances_km(geometry: list[tuple[float, float]]) -> list[float]:
    if not geometry:
        return [0.0]
    cumulative: list[float] = [0.0]
    total = 0.0
    for idx in range(1, len(geometry)):
        lat1, lon1 = geometry[idx - 1]
        lat2, lon2 = geometry[idx]
        total += haversine_km(lat1, lon1, lat2, lon2)
        cumulative.append(total)
    return cumulative


def densify_geometry(
    geometry: list[tuple[float, float]],
    *,
    max_step_km: float,
) -> list[tuple[float, float]]:
    if max_step_km <= 0 or len(geometry) < 2:
        return list(geometry)
    out: list[tuple[float, float]] = [geometry[0]]
    for idx in range(1, len(geometry)):
        lat1, lon1 = geometry[idx - 1]
        lat2, lon2 = geometry[idx]
        distance = haversine_km(lat1, lon1, lat2, lon2)
        steps = max(1, int(math.ceil(distance / max_step_km)))
        for step in range(1, steps + 1):
            ratio = step / steps
            out.append((lat1 + (lat2 - lat1) * ratio, lon1 + (lon2 - lon1) * ratio))
    return out


def synthetic_grid_geometry(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> list[tuple[float, float]]:
    """Orthogonal fallback path used when routing providers are unavailable.

    It avoids the worst visual "diagonal teleport" effect by adding a
    Manhattan-style turn between origin and destination.
    """
    origin = (origin_lat, origin_lon)
    dest = (dest_lat, dest_lon)
    if haversine_km(origin_lat, origin_lon, dest_lat, dest_lon) < 0.2:
        return [origin, dest]

    def _orthogonal_leg(a: tuple[float, float], b: tuple[float, float]) -> list[tuple[float, float]]:
        a_lat, a_lon = a
        b_lat, b_lon = b
        lat_gap = abs(b_lat - a_lat)
        lon_gap = abs(b_lon - a_lon)
        if max(lat_gap, lon_gap) < 0.0008:
            return [a, b]
        if lat_gap >= lon_gap:
            mid_lat = (a_lat + b_lat) / 2.0
            return [a, (mid_lat, a_lon), (mid_lat, b_lon), b]
        mid_lon = (a_lon + b_lon) / 2.0
        return [a, (a_lat, mid_lon), (b_lat, mid_lon), b]

    def _crosses_east_river(a: tuple[float, float], b: tuple[float, float]) -> bool:
        a_lat, a_lon = a
        b_lat, b_lon = b
        lon_cross = (a_lon <= -73.97 and b_lon >= -73.965) or (b_lon <= -73.97 and a_lon >= -73.965)
        lat_band = 40.67 <= ((a_lat + b_lat) / 2.0) <= 40.82
        return lon_cross and lat_band

    def _crosses_hudson(a: tuple[float, float], b: tuple[float, float]) -> bool:
        a_lat, a_lon = a
        b_lat, b_lon = b
        lon_cross = (a_lon <= -74.03 and b_lon >= -74.02) or (b_lon <= -74.03 and a_lon >= -74.02)
        lat_band = 40.68 <= ((a_lat + b_lat) / 2.0) <= 40.82
        return lon_cross and lat_band

    east_river_pairs = [
        ((40.7132, -74.0048), (40.7057, -73.9964)),  # Brooklyn Bridge
        ((40.7165, -73.9900), (40.7075, -73.9887)),  # Manhattan Bridge
        ((40.7138, -73.9718), (40.7111, -73.9620)),  # Williamsburg Bridge
        ((40.7603, -73.9580), (40.7565, -73.9488)),  # Queensboro Bridge
    ]
    hudson_pairs = [
        ((40.7266, -74.0113), (40.7258, -74.0355)),  # Holland Tunnel
        ((40.7618, -73.9930), (40.7612, -74.0285)),  # Lincoln Tunnel
    ]

    bridge_waypoints: list[tuple[float, float]] = []
    if _crosses_east_river(origin, dest):
        origin_west = origin[1] <= dest[1]
        candidates: list[tuple[float, tuple[float, float], tuple[float, float]]] = []
        for west_anchor, east_anchor in east_river_pairs:
            if origin_west:
                score = haversine_km(origin[0], origin[1], west_anchor[0], west_anchor[1]) + haversine_km(
                    dest[0], dest[1], east_anchor[0], east_anchor[1]
                )
                candidates.append((score, west_anchor, east_anchor))
            else:
                score = haversine_km(origin[0], origin[1], east_anchor[0], east_anchor[1]) + haversine_km(
                    dest[0], dest[1], west_anchor[0], west_anchor[1]
                )
                candidates.append((score, east_anchor, west_anchor))
        _, w0, w1 = min(candidates, key=lambda item: item[0])
        bridge_waypoints = [w0, w1]
    elif _crosses_hudson(origin, dest):
        origin_east = origin[1] >= dest[1]
        candidates = []
        for east_anchor, west_anchor in hudson_pairs:
            if origin_east:
                score = haversine_km(origin[0], origin[1], east_anchor[0], east_anchor[1]) + haversine_km(
                    dest[0], dest[1], west_anchor[0], west_anchor[1]
                )
                candidates.append((score, east_anchor, west_anchor))
            else:
                score = haversine_km(origin[0], origin[1], west_anchor[0], west_anchor[1]) + haversine_km(
                    dest[0], dest[1], east_anchor[0], east_anchor[1]
                )
                candidates.append((score, west_anchor, east_anchor))
        _, w0, w1 = min(candidates, key=lambda item: item[0])
        bridge_waypoints = [w0, w1]

    if bridge_waypoints:
        route_points = [origin, *bridge_waypoints, dest]
        out: list[tuple[float, float]] = []
        for idx in range(1, len(route_points)):
            leg = _orthogonal_leg(route_points[idx - 1], route_points[idx])
            if out:
                out.extend(leg[1:])
            else:
                out.extend(leg)
        return out

    return _orthogonal_leg(origin, dest)


def route_geometry_for_replay(geometry: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if TLC_ROUTE_DENSIFY_MAX_STEP_KM <= 0:
        return geometry
    return densify_geometry(geometry, max_step_km=TLC_ROUTE_DENSIFY_MAX_STEP_KM)


def interpolate_on_geometry(
    geometry: list[tuple[float, float]],
    cumulative_km: list[float],
    progress: float,
) -> tuple[float, float, float]:
    if not geometry:
        return 40.7580, -73.9855, 0.0
    if len(geometry) == 1 or not cumulative_km or cumulative_km[-1] <= 0:
        lat, lon = geometry[-1]
        return lat, lon, 0.0

    p = max(0.0, min(1.0, float(progress)))
    target_km = p * cumulative_km[-1]
    for idx in range(1, len(cumulative_km)):
        if cumulative_km[idx] >= target_km:
            start_km = cumulative_km[idx - 1]
            end_km = cumulative_km[idx]
            span = end_km - start_km
            ratio = 0.0 if span <= 0 else (target_km - start_km) / span
            lat1, lon1 = geometry[idx - 1]
            lat2, lon2 = geometry[idx]
            lat = lat1 + (lat2 - lat1) * ratio
            lon = lon1 + (lon2 - lon1) * ratio
            return lat, lon, bearing_deg(lat1, lon1, lat2, lon2)

    lat_last, lon_last = geometry[-1]
    lat_prev, lon_prev = geometry[-2]
    return lat_last, lon_last, bearing_deg(lat_prev, lon_prev, lat_last, lon_last)


def source_platform_for_route(base_platform: str, route_source: str) -> str:
    base = (base_platform or "unknown").strip() or "unknown"
    if "|route=" in base:
        base_parts = [part for part in base.split("|") if part and not part.startswith("route=")]
        base = "|".join(base_parts) or "unknown"
    route = (route_source or "").strip().lower()
    source = "linear" if route in {"", "linear"} else "osrm"
    return f"{base}|route={source}"


def _parse_hhmm(value: str, fallback: dtime) -> dtime:
    token = (value or "").strip()
    try:
        hh, mm = token.split(":", 1)
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        return fallback


def _in_time_window(now: datetime, start: dtime, end: dtime) -> bool:
    current = now.time()
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def _parse_month(month: str) -> tuple[int, int]:
    value = month.strip()
    if not MONTH_RE.match(value):
        raise ValueError(f"invalid month '{month}' (expected YYYY-MM)")
    year_s, month_s = value.split("-")
    return int(year_s), int(month_s)


def _format_month(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    index = (year * 12 + (month - 1)) + delta
    return index // 12, (index % 12) + 1


def _build_month_sequence(start_month: str, count: int) -> list[str]:
    if count <= 0:
        return [start_month]
    start_year, start_month_num = _parse_month(start_month)
    months: list[str] = []
    for offset in range(count):
        year, month = _add_months(start_year, start_month_num, offset)
        months.append(_format_month(year, month))
    return months


def resolve_replay_months() -> list[str]:
    if TLC_MONTHS_RAW:
        months = [m.strip() for m in TLC_MONTHS_RAW.split(",") if m.strip()]
        if not months:
            raise ValueError("TLC_MONTHS is set but empty after parsing")
        validated: list[str] = []
        for month in months:
            _parse_month(month)
            validated.append(month)
        return validated

    try:
        month_count = int(TLC_MONTH_COUNT_RAW)
    except ValueError as exc:
        raise ValueError(f"invalid TLC_MONTH_COUNT '{TLC_MONTH_COUNT_RAW}'") from exc

    if month_count < 0:
        raise ValueError("TLC_MONTH_COUNT must be >= 0")

    _parse_month(TLC_MONTH)
    return _build_month_sequence(TLC_MONTH, month_count) if month_count > 0 else [TLC_MONTH]


class Trip:
    __slots__ = (
        "trip_key",
        "courier_id",
        "offer_id",
        "order_id",
        "request_ts",
        "pickup_ts",
        "dropoff_ts",
        "pu_loc",
        "do_loc",
        "pickup_lat",
        "pickup_lon",
        "dropoff_lat",
        "dropoff_lon",
        "trip_km",
        "trip_min",
        "fare_usd",
        "avg_speed_kmh",
        "bearing",
        "position_zone_id",
        "reposition_start_ts",
        "reposition_start_lat",
        "reposition_start_lon",
        "reposition_source",
        "reposition_geometry",
        "reposition_cumulative_km",
        "route_source",
        "route_geometry",
        "route_cumulative_km",
    )

    def __init__(
        self,
        trip_key: str,
        courier_id: str,
        offer_id: str,
        order_id: str,
        request_ts: datetime,
        pickup_ts: datetime,
        dropoff_ts: datetime,
        pu_loc: int,
        do_loc: int,
        pickup_lat: float,
        pickup_lon: float,
        dropoff_lat: float,
        dropoff_lon: float,
        trip_km: float,
        trip_min: float,
        fare_usd: float,
    ) -> None:
        self.trip_key = trip_key
        self.courier_id = courier_id
        self.offer_id = offer_id
        self.order_id = order_id
        self.request_ts = request_ts
        self.pickup_ts = pickup_ts
        self.dropoff_ts = dropoff_ts
        self.pu_loc = pu_loc
        self.do_loc = do_loc
        self.pickup_lat = pickup_lat
        self.pickup_lon = pickup_lon
        self.dropoff_lat = dropoff_lat
        self.dropoff_lon = dropoff_lon
        self.trip_km = trip_km
        self.trip_min = trip_min
        self.fare_usd = fare_usd
        duration_h = max(trip_min / 60.0, 0.01)
        self.avg_speed_kmh = min(90.0, max(5.0, trip_km / duration_h))
        self.bearing = bearing_deg(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
        self.position_zone_id = f"nyc_{pu_loc}"
        self.reposition_start_ts = request_ts
        self.reposition_start_lat = pickup_lat
        self.reposition_start_lon = pickup_lon
        self.reposition_source = "linear"
        self.reposition_geometry = [(pickup_lat, pickup_lon)]
        self.reposition_cumulative_km = [0.0]
        self.route_source = "linear"
        self.route_geometry = [
            (pickup_lat, pickup_lon),
            (dropoff_lat, dropoff_lon),
        ]
        self.route_cumulative_km = cumulative_route_distances_km(self.route_geometry)


class TLCReplay:
    def __init__(self, month: str) -> None:
        self.producer: AIOKafkaProducer | None = None
        self.redis: aioredis.Redis | None = None
        self.osrm_client: httpx.AsyncClient | None = None
        self.month = month
        self.sorted_path = TLC_DATA_DIR / f"hvfhv_sorted_{self.month}.parquet"
        self.raw_path = TLC_DATA_DIR / f"fhvhv_tripdata_{self.month}.parquet"
        self.duck = duckdb.connect(":memory:")
        self.centroids: dict[int, tuple[float, float]] = {}
        self.active_trips: dict[str, Trip] = {}
        self.virtual_time: datetime | None = None
        self.stats_emitted_offers = 0
        self.stats_emitted_positions = 0
        self.stats_emitted_events = 0
        self.stats_dropped_no_capacity = 0
        self.stats_skipped_sample = 0
        self.stats_route_osrm_success = 0
        self.stats_route_linear_mode = 0
        self.stats_route_linear_fallback = 0
        self.stats_route_osrm_errors = 0
        self.stats_route_osrm_public_success = 0
        self.stats_route_valhalla_public_success = 0
        self.stats_route_prefetch_started = 0
        self.stats_route_prefetch_success = 0
        self.stats_route_prefetch_errors = 0
        self.stats_route_cache_hits = 0
        self.stats_route_cache_misses = 0
        self.last_route_source = "linear"
        self._stop_flag = False
        self.route_cache: dict[tuple[int, int], tuple[str, list[tuple[float, float]], list[float]]] = {}
        self.route_cache_order: deque[tuple[int, int]] = deque()
        self.route_key_seen_count: dict[tuple[int, int], int] = {}
        self.route_prefetch_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}
        self.route_prefetch_semaphore = asyncio.Semaphore(TLC_ROUTE_PREFETCH_MAX_CONCURRENCY)
        self.route_osrm_targets: list[tuple[str, str]] = _osrm_route_targets()
        self._last_osrm_provider = "osrm"
        self.route_failure_streak = 0
        self.route_cooldown_until: datetime | None = None
        self.fleet_pool_enabled = TLC_COURIER_ID_MODE == "fleet_pool"
        self.fleet_driver_ids: list[str] = []
        self.fleet_driver_state: dict[str, tuple[Any, ...]] = {}
        # Exposed to scenario modules (single_driver) so they don't reach
        # into module-level globals.
        self.speed_factor = TLC_SPEED_FACTOR
        self.tick_interval_sec = TLC_TICK_INTERVAL_SEC
        self.night_accel_start = _parse_hhmm(TLC_NIGHT_ACCEL_START, dtime(2, 0))
        self.night_accel_end = _parse_hhmm(TLC_NIGHT_ACCEL_END, dtime(8, 0))

    def _tick_accel_factor(self, vt: datetime) -> int:
        if TLC_NIGHT_ACCEL_FACTOR <= 1:
            return 1
        return TLC_NIGHT_ACCEL_FACTOR if _in_time_window(vt, self.night_accel_start, self.night_accel_end) else 1

    def configure_month(self, month: str) -> None:
        self.month = month
        self.sorted_path = TLC_DATA_DIR / f"hvfhv_sorted_{self.month}.parquet"
        self.raw_path = TLC_DATA_DIR / f"fhvhv_tripdata_{self.month}.parquet"
        self.active_trips = {}
        self.virtual_time = None
        self.stats_emitted_offers = 0
        self.stats_emitted_positions = 0
        self.stats_emitted_events = 0
        self.stats_dropped_no_capacity = 0
        self.stats_skipped_sample = 0
        self.stats_route_osrm_success = 0
        self.stats_route_linear_mode = 0
        self.stats_route_linear_fallback = 0
        self.stats_route_osrm_errors = 0
        self.stats_route_osrm_public_success = 0
        self.stats_route_valhalla_public_success = 0
        self.stats_route_prefetch_started = 0
        self.stats_route_prefetch_success = 0
        self.stats_route_prefetch_errors = 0
        self.stats_route_cache_hits = 0
        self.stats_route_cache_misses = 0
        self.last_route_source = "linear"
        self.route_failure_streak = 0
        self.route_cooldown_until = None
        self.route_cache = {}
        self.route_cache_order = deque()
        self.route_key_seen_count = {}

    async def start(self) -> None:
        TLC_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._load_centroids()
        self._init_fleet_driver_pool()
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            compression_type="lz4",
            acks=1,
            linger_ms=5,
            max_batch_size=262_144,
        )
        await self.producer.start()
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        if TLC_ROUTE_MODE == "osrm":
            self.osrm_client = httpx.AsyncClient(timeout=TLC_ROUTE_OSRM_TIMEOUT_S)
            await self._prefer_working_route_provider()
        await self._status(state="starting", message="connected_kafka_redis")

    async def close(self) -> None:
        if self.route_prefetch_tasks:
            pending = list(self.route_prefetch_tasks.values())
            self.route_prefetch_tasks.clear()
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if self.producer is not None:
            await self.producer.stop()
        if self.redis is not None:
            await self.redis.aclose()
        if self.osrm_client is not None:
            await self.osrm_client.aclose()
            self.osrm_client = None
        try:
            self.duck.close()
        except Exception:
            pass

    def _load_centroids(self) -> None:
        if not CENTROIDS_PATH.exists():
            raise FileNotFoundError(
                f"NYC zone centroids not found at {CENTROIDS_PATH}. "
                "Run scripts/gen_nyc_zone_centroids.py to regenerate."
            )
        raw = json.loads(CENTROIDS_PATH.read_text(encoding="utf-8"))
        self.centroids = {int(k): (float(v[0]), float(v[1])) for k, v in raw.items()}
        log.info("loaded %d NYC zone centroids", len(self.centroids))

    def _centroid(self, loc_id: int) -> tuple[float, float]:
        return self.centroids.get(int(loc_id), (40.7580, -73.9855))

    def _centroid_or_none(self, loc_id: int) -> tuple[float, float] | None:
        point = self.centroids.get(int(loc_id))
        if point is None:
            return None
        return float(point[0]), float(point[1])

    def _init_fleet_driver_pool(self) -> None:
        if not self.fleet_pool_enabled or self.fleet_driver_ids:
            return
        pool_size = max(1, int(TLC_FLEET_DEMO_N_DRIVERS))
        initial_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        centroid_items = list(self.centroids.items()) or [(161, (40.7580, -73.9855))]
        self.fleet_driver_ids = [format_fleet_driver_id(idx + 1, pool_size) for idx in range(pool_size)]
        self.fleet_driver_state = {}
        for idx, cid in enumerate(self.fleet_driver_ids):
            zone_id, (lat, lon) = centroid_items[idx % len(centroid_items)]
            self.fleet_driver_state[cid] = (initial_ts, float(lat), float(lon), int(zone_id))
        log.info("fleet_pool enabled with %d synthetic drivers", pool_size)

    def _fleet_driver_state_parts(
        self,
        state: tuple[Any, ...],
    ) -> tuple[datetime, float, float, int | None]:
        available_at = state[0]
        last_lat = float(state[1])
        last_lon = float(state[2])
        zone_id: int | None = None
        if len(state) >= 4 and state[3] is not None:
            zone_id = int(state[3])
        return available_at, last_lat, last_lon, zone_id

    def _fleet_reposition_eta_min(
        self,
        origin_lat: float,
        origin_lon: float,
        dest_lat: float,
        dest_lon: float,
    ) -> float:
        distance_km = haversine_km(origin_lat, origin_lon, dest_lat, dest_lon)
        return (distance_km / max(TLC_FLEET_REPOSITION_KMH, 5.0)) * 60.0

    def _trip_needs_reposition(self, trip: Trip) -> bool:
        return haversine_km(
            trip.reposition_start_lat,
            trip.reposition_start_lon,
            trip.pickup_lat,
            trip.pickup_lon,
        ) >= 0.05

    def _pick_fleet_driver(self, trip: Trip) -> str | None:
        if not self.fleet_pool_enabled or not self.fleet_driver_ids:
            return trip.courier_id

        request_ts = trip.request_ts
        pickup_ts = trip.pickup_ts
        if request_ts.tzinfo is None:
            request_ts = request_ts.replace(tzinfo=timezone.utc)
        if pickup_ts.tzinfo is None:
            pickup_ts = pickup_ts.replace(tzinfo=timezone.utc)
        preferred_cid = trip.courier_id if trip.courier_id in self.fleet_driver_state else None

        best_cid: str | None = None
        best_score = float("inf")
        pickup_grace = timedelta(minutes=TLC_FLEET_PICKUP_GRACE_MIN)

        for cid in self.fleet_driver_ids:
            if cid in self.active_trips:
                continue
            state = self.fleet_driver_state.get(cid)
            if state is None:
                continue
            available_at, last_lat, last_lon, _ = self._fleet_driver_state_parts(state)
            if available_at.tzinfo is None:
                available_at = available_at.replace(tzinfo=timezone.utc)
            if available_at > pickup_ts + pickup_grace:
                continue

            distance_km = haversine_km(last_lat, last_lon, trip.pickup_lat, trip.pickup_lon)
            reposition_eta_min = self._fleet_reposition_eta_min(
                last_lat,
                last_lon,
                trip.pickup_lat,
                trip.pickup_lon,
            )
            if reposition_eta_min > TLC_FLEET_MAX_REPOSITION_MIN:
                continue

            reposition_start_ts = available_at if available_at > request_ts else request_ts
            arrival_at = reposition_start_ts + timedelta(minutes=reposition_eta_min)
            if arrival_at > pickup_ts + pickup_grace:
                continue

            wait_min = max(0.0, (reposition_start_ts - request_ts).total_seconds() / 60.0)
            lateness_min = max(0.0, (arrival_at - pickup_ts).total_seconds() / 60.0)
            preference_bonus = -0.35 if preferred_cid and cid == preferred_cid else 0.0
            score = distance_km + (wait_min * 1.7) + (reposition_eta_min * 0.35) + (lateness_min * 4.0) + preference_bonus
            if score < best_score:
                best_score = score
                best_cid = cid

        return best_cid

    def _mark_fleet_driver_available(self, trip: Trip) -> None:
        if not self.fleet_pool_enabled:
            return
        cid = str(trip.courier_id or "").strip()
        if not cid:
            return
        dropoff_ts = trip.dropoff_ts
        if dropoff_ts.tzinfo is None:
            dropoff_ts = dropoff_ts.replace(tzinfo=timezone.utc)
        self.fleet_driver_state[cid] = (
            dropoff_ts,
            float(trip.dropoff_lat),
            float(trip.dropoff_lon),
            int(trip.do_loc),
        )

    async def _prepare_fleet_reposition(self, trip: Trip) -> None:
        if not self.fleet_pool_enabled:
            return
        cid = str(trip.courier_id or "").strip()
        state = self.fleet_driver_state.get(cid)
        if not cid or state is None:
            return

        available_at, last_lat, last_lon, _ = self._fleet_driver_state_parts(state)
        if available_at.tzinfo is None:
            available_at = available_at.replace(tzinfo=timezone.utc)

        trip.reposition_start_ts = available_at if available_at > trip.request_ts else trip.request_ts
        trip.reposition_start_lat = last_lat
        trip.reposition_start_lon = last_lon

        if (
            trip.reposition_start_ts >= trip.pickup_ts
            or haversine_km(last_lat, last_lon, trip.pickup_lat, trip.pickup_lon) < 0.05
        ):
            trip.reposition_start_ts = trip.request_ts
            trip.reposition_start_lat = trip.pickup_lat
            trip.reposition_start_lon = trip.pickup_lon
            trip.reposition_source = "identity"
            trip.reposition_geometry = [(trip.pickup_lat, trip.pickup_lon)]
            trip.reposition_cumulative_km = [0.0]
            return

        source = "linear"
        geometry = route_geometry_for_replay(
            synthetic_grid_geometry(
                last_lat,
                last_lon,
                trip.pickup_lat,
                trip.pickup_lon,
            )
        )
        cumulative_km = cumulative_route_distances_km(geometry)

        if TLC_ROUTE_MODE == "osrm":
            if self.osrm_client is None or self._route_fetch_in_cooldown():
                self.stats_route_linear_fallback += 1
            else:
                try:
                    provider_budget_s = min(
                        2.5,
                        max(0.2, TLC_ROUTE_OSRM_TIMEOUT_S * max(1, len(self.route_osrm_targets)) * 0.5),
                    )
                    route_result = await asyncio.wait_for(
                        self._fetch_osrm_geometry(
                            last_lat,
                            last_lon,
                            trip.pickup_lat,
                            trip.pickup_lon,
                        ),
                        timeout=max(0.1, min(TLC_ROUTE_FETCH_TIMEOUT_S, provider_budget_s)),
                    )
                    source, geometry = self._normalize_route_fetch_result(route_result)
                    geometry = route_geometry_for_replay(geometry)
                    cumulative_km = cumulative_route_distances_km(geometry)
                    if source == "osrm_public":
                        self.stats_route_osrm_public_success += 1
                    elif source == "valhalla_public":
                        self.stats_route_valhalla_public_success += 1
                    else:
                        self.stats_route_osrm_success += 1
                    self._mark_route_fetch_success()
                except Exception:
                    self.stats_route_osrm_errors += 1
                    self.stats_route_linear_fallback += 1
                    self._mark_route_fetch_failure()
        else:
            self.stats_route_linear_mode += 1

        trip.reposition_source = source
        trip.reposition_geometry = geometry
        trip.reposition_cumulative_km = cumulative_km

    def _route_fetch_in_cooldown(self) -> bool:
        if self.route_cooldown_until is None:
            return False
        return datetime.now(timezone.utc) < self.route_cooldown_until

    def _mark_route_fetch_success(self) -> None:
        self.route_failure_streak = 0
        self.route_cooldown_until = None

    def _mark_route_fetch_failure(self) -> None:
        self.route_failure_streak += 1
        if (
            TLC_ROUTE_FAIL_COOLDOWN_S > 0
            and self.route_failure_streak >= TLC_ROUTE_FAIL_STREAK_TO_COOLDOWN
        ):
            self.route_cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=TLC_ROUTE_FAIL_COOLDOWN_S)

    def _route_cache_put(
        self,
        key: tuple[int, int],
        source: str,
        geometry: list[tuple[float, float]],
        cumulative_km: list[float],
        *,
        replace_linear: bool = False,
    ) -> bool:
        existing = self.route_cache.get(key)
        if existing is not None:
            existing_source = str(existing[0] or "").strip().lower()
            incoming_source = str(source or "").strip().lower()
            can_replace = (
                replace_linear
                and existing_source == "linear"
                and incoming_source != "linear"
            )
            if not can_replace:
                return False
            self.route_cache[key] = (source, geometry, cumulative_km)
            return True
        self.route_cache[key] = (source, geometry, cumulative_km)
        self.route_cache_order.append(key)
        max_size = max(int(TLC_ROUTE_CACHE_MAX), 1)
        if len(self.route_cache_order) > max_size:
            oldest = self.route_cache_order.popleft()
            self.route_cache.pop(oldest, None)
        return True

    async def _prefer_working_route_provider(self) -> None:
        """Rotate provider order to the first healthy provider when possible."""
        if self.osrm_client is None or len(self.route_osrm_targets) <= 1:
            return
        healthy_idx = -1
        for idx, (name, base_url) in enumerate(self.route_osrm_targets):
            if await self._probe_route_provider(name, base_url):
                healthy_idx = idx
                break
        if healthy_idx <= 0:
            return
        self.route_osrm_targets = self.route_osrm_targets[healthy_idx:] + self.route_osrm_targets[:healthy_idx]
        log.warning(
            "preferred route provider switched to: %s",
            ",".join(name for name, _ in self.route_osrm_targets),
        )

    async def _probe_route_provider(self, source: str, base_url: str) -> bool:
        if self.osrm_client is None:
            return False
        try:
            if source == "valhalla_public":
                probe_query = {
                    "locations": [
                        {"lat": 40.7580, "lon": -73.9855},
                        {"lat": 40.7610, "lon": -73.9790},
                    ],
                    "costing": "auto",
                }
                probe_url = f"{base_url}/route"
                resp = await self.osrm_client.get(
                    probe_url,
                    params={"json": json.dumps(probe_query, separators=(",", ":"))},
                    headers={"User-Agent": "FleetStream/1.0"},
                    timeout=1.2,
                )
                if resp.status_code != 200:
                    return False
                payload = resp.json()
                return bool(((payload.get("trip") or {}).get("legs") or []))

            probe_url = f"{base_url}/nearest/v1/driving/-73.985500,40.758000"
            resp = await self.osrm_client.get(
                probe_url,
                params={"number": 1},
                timeout=0.8,
            )
            return resp.status_code == 200
        except Exception:
            return False

    async def _fetch_osrm_geometry(
        self,
        origin_lat: float,
        origin_lon: float,
        dest_lat: float,
        dest_lon: float,
    ) -> tuple[str, list[tuple[float, float]]]:
        if self.osrm_client is None:
            raise RuntimeError("osrm_client_not_initialized")
        last_error: Exception | None = None
        coords = f"{origin_lon:.6f},{origin_lat:.6f};{dest_lon:.6f},{dest_lat:.6f}"
        params = {
            "overview": "full",
            "geometries": "polyline",
            "steps": "false",
            "annotations": "false",
        }
        for source, base_url in self.route_osrm_targets:
            try:
                if source == "valhalla_public":
                    valhalla_query = {
                        "locations": [
                            {"lat": origin_lat, "lon": origin_lon},
                            {"lat": dest_lat, "lon": dest_lon},
                        ],
                        "costing": "auto",
                        "directions_options": {"units": "kilometers"},
                    }
                    url = f"{base_url}/route"
                    response = await self.osrm_client.get(
                        url,
                        params={"json": json.dumps(valhalla_query, separators=(",", ":"))},
                        headers={"User-Agent": "FleetStream/1.0"},
                    )
                    response.raise_for_status()
                    payload = response.json()
                    trip_payload = payload.get("trip") or {}
                    legs = trip_payload.get("legs") or []
                    if not legs:
                        raise RuntimeError("valhalla_no_routes")
                    shape = str(legs[0].get("shape") or "")
                    geometry = decode_polyline(shape, precision=6)
                    if len(geometry) < 2:
                        raise RuntimeError("valhalla_empty_geometry")
                else:
                    url = f"{base_url}/route/v1/driving/{coords}"
                    response = await self.osrm_client.get(url, params=params)
                    response.raise_for_status()
                    payload = response.json()
                    if payload.get("code") != "Ok":
                        raise RuntimeError(f"osrm_code_{payload.get('code')}")
                    routes = payload.get("routes") or []
                    if not routes:
                        raise RuntimeError("osrm_no_routes")
                    geometry = decode_polyline(routes[0].get("geometry", ""), precision=5)
                    if len(geometry) < 2:
                        raise RuntimeError("osrm_empty_geometry")
                self._last_osrm_provider = source
                return source, geometry
            except Exception as exc:
                last_error = exc
                log.debug(
                    "trip route provider failed provider=%s url=%s err=%s",
                    source,
                    base_url,
                    exc,
                )
        raise RuntimeError(f"osrm_all_providers_failed:{last_error}")

    def _normalize_route_fetch_result(
        self,
        fetch_result: Any,
    ) -> tuple[str, list[tuple[float, float]]]:
        if (
            isinstance(fetch_result, tuple)
            and len(fetch_result) == 2
            and isinstance(fetch_result[0], str)
            and isinstance(fetch_result[1], list)
        ):
            return fetch_result[0], fetch_result[1]
        source = self._last_osrm_provider if self._last_osrm_provider else "osrm"
        return source, fetch_result

    def _schedule_route_prefetch(self, trip: Trip, key: tuple[int, int]) -> None:
        cached = self.route_cache.get(key)
        cached_source = str(cached[0]).strip().lower() if cached is not None else ""
        if (
            TLC_ROUTE_MODE != "osrm"
            or not TLC_ROUTE_PREFETCH_ENABLED
            or self.osrm_client is None
            or self._route_fetch_in_cooldown()
            or (cached is not None and cached_source != "linear")
            or key in self.route_prefetch_tasks
        ):
            return
        if len(self.route_prefetch_tasks) >= TLC_ROUTE_PREFETCH_MAX_PENDING:
            return

        async def _runner() -> None:
            try:
                async with self.route_prefetch_semaphore:
                    source, prefetched_geometry = await self._fetch_osrm_geometry(
                        trip.pickup_lat,
                        trip.pickup_lon,
                        trip.dropoff_lat,
                        trip.dropoff_lon,
                    )
                prefetched_geometry = route_geometry_for_replay(prefetched_geometry)
                prefetched_cumulative_km = cumulative_route_distances_km(prefetched_geometry)
                self._route_cache_put(
                    key,
                    source,
                    prefetched_geometry,
                    prefetched_cumulative_km,
                    replace_linear=True,
                )
                self.stats_route_prefetch_success += 1
            except Exception:
                self.stats_route_prefetch_errors += 1
            finally:
                self.route_prefetch_tasks.pop(key, None)

        self.route_prefetch_tasks[key] = asyncio.create_task(_runner())
        self.stats_route_prefetch_started += 1

    async def _prepare_trip_route(self, trip: Trip) -> None:
        key = (int(trip.pu_loc), int(trip.do_loc))
        cached = self.route_cache.get(key)
        if cached is not None:
            self.stats_route_cache_hits += 1
            source, geometry, cumulative_km = cached
            trip.route_source = source
            trip.route_geometry = list(geometry)
            trip.route_cumulative_km = list(cumulative_km)
            self.last_route_source = source
            if TLC_ROUTE_MODE == "osrm" and str(source).strip().lower() == "linear":
                linear_hits = int(self.route_key_seen_count.get(key, 0)) + 1
                self.route_key_seen_count[key] = linear_hits
                should_try_sync_upgrade = (
                    (not TLC_ROUTE_SYNC_ON_CACHE_MISS)
                    and (linear_hits % max(1, TLC_ROUTE_SYNC_KEY_REPEAT_THRESHOLD) == 0)
                    and (not self._route_fetch_in_cooldown())
                    and self.osrm_client is not None
                    and key not in self.route_prefetch_tasks
                )
                if should_try_sync_upgrade:
                    try:
                        provider_budget_s = min(
                            2.0,
                            max(0.2, TLC_ROUTE_OSRM_TIMEOUT_S * max(1, len(self.route_osrm_targets)) * 0.5),
                        )
                        route_result = await asyncio.wait_for(
                            self._fetch_osrm_geometry(
                                trip.pickup_lat,
                                trip.pickup_lon,
                                trip.dropoff_lat,
                                trip.dropoff_lon,
                            ),
                            timeout=max(0.1, min(TLC_ROUTE_FETCH_TIMEOUT_S, provider_budget_s)),
                        )
                        upgraded_source, upgraded_geometry = self._normalize_route_fetch_result(route_result)
                        upgraded_geometry = route_geometry_for_replay(upgraded_geometry)
                        upgraded_cumulative = cumulative_route_distances_km(upgraded_geometry)
                        self._route_cache_put(
                            key,
                            upgraded_source,
                            upgraded_geometry,
                            upgraded_cumulative,
                            replace_linear=True,
                        )
                        trip.route_source = upgraded_source
                        trip.route_geometry = upgraded_geometry
                        trip.route_cumulative_km = upgraded_cumulative
                        self.last_route_source = upgraded_source
                        if upgraded_source == "osrm_public":
                            self.stats_route_osrm_public_success += 1
                        elif upgraded_source == "valhalla_public":
                            self.stats_route_valhalla_public_success += 1
                        else:
                            self.stats_route_osrm_success += 1
                        self._mark_route_fetch_success()
                    except Exception:
                        self.stats_route_osrm_errors += 1
                        self._mark_route_fetch_failure()
                        self._schedule_route_prefetch(trip, key)
                else:
                    self._schedule_route_prefetch(trip, key)
            return

        self.stats_route_cache_misses += 1
        seen_count = int(self.route_key_seen_count.get(key, 0)) + 1
        self.route_key_seen_count[key] = seen_count
        source = "linear"
        geometry = route_geometry_for_replay(
            synthetic_grid_geometry(
                trip.pickup_lat,
                trip.pickup_lon,
                trip.dropoff_lat,
                trip.dropoff_lon,
            )
        )
        cumulative_km = cumulative_route_distances_km(geometry)

        if TLC_ROUTE_MODE == "osrm":
            force_sync_for_hot_key = seen_count >= TLC_ROUTE_SYNC_KEY_REPEAT_THRESHOLD
            if not TLC_ROUTE_SYNC_ON_CACHE_MISS and not force_sync_for_hot_key:
                self.stats_route_linear_fallback += 1
            elif self._route_fetch_in_cooldown():
                self.stats_route_linear_fallback += 1
            else:
                try:
                    provider_budget_s = min(
                        3.5,
                        max(0.2, TLC_ROUTE_OSRM_TIMEOUT_S * max(1, len(self.route_osrm_targets)) * 0.65),
                    )
                    effective_timeout_s = max(0.1, min(TLC_ROUTE_FETCH_TIMEOUT_S, provider_budget_s))
                    route_result = await asyncio.wait_for(
                        self._fetch_osrm_geometry(
                            trip.pickup_lat,
                            trip.pickup_lon,
                            trip.dropoff_lat,
                            trip.dropoff_lon,
                        ),
                        timeout=effective_timeout_s,
                    )
                    source, geometry = self._normalize_route_fetch_result(route_result)
                    geometry = route_geometry_for_replay(geometry)
                    cumulative_km = cumulative_route_distances_km(geometry)
                    if source == "osrm_public":
                        self.stats_route_osrm_public_success += 1
                    elif source == "valhalla_public":
                        self.stats_route_valhalla_public_success += 1
                    else:
                        self.stats_route_osrm_success += 1
                    self._mark_route_fetch_success()
                except Exception as exc:
                    self.stats_route_osrm_errors += 1
                    self.stats_route_linear_fallback += 1
                    self._mark_route_fetch_failure()
                    log.debug(
                        "trip %s route fallback to linear (%s -> %s): %s",
                        trip.trip_key,
                        trip.pu_loc,
                        trip.do_loc,
                        exc,
                    )
            if source == "linear":
                self._schedule_route_prefetch(trip, key)
        else:
            self.stats_route_linear_mode += 1

        trip.route_source = source
        trip.route_geometry = geometry
        trip.route_cumulative_km = cumulative_km
        self.last_route_source = source
        self._route_cache_put(key, source, geometry, cumulative_km)

    async def _status(self, **mapping: Any) -> None:
        if self.redis is None:
            return
        payload = {k: str(v) for k, v in mapping.items()}
        payload["updated_at"] = utc_now_iso()
        payload["month"] = self.month
        payload["speed_factor"] = str(TLC_SPEED_FACTOR)
        payload["sample_rate"] = str(TLC_TRIP_SAMPLE_RATE)
        payload["max_active"] = str(TLC_MAX_ACTIVE_TRIPS)
        payload["start_hour_utc"] = str(TLC_START_HOUR_UTC)
        payload["courier_id_mode"] = TLC_COURIER_ID_MODE
        payload["fleet_pool_size"] = str(TLC_FLEET_DEMO_N_DRIVERS if self.fleet_pool_enabled else 0)
        payload["night_accel_start"] = TLC_NIGHT_ACCEL_START
        payload["night_accel_end"] = TLC_NIGHT_ACCEL_END
        payload["night_accel_factor"] = str(TLC_NIGHT_ACCEL_FACTOR)
        payload["trajectory_mode"] = TLC_ROUTE_MODE
        payload["route_sync_on_cache_miss"] = str(TLC_ROUTE_SYNC_ON_CACHE_MISS).lower()
        payload["route_sync_key_repeat_threshold"] = str(TLC_ROUTE_SYNC_KEY_REPEAT_THRESHOLD)
        payload["route_cache_size"] = str(len(self.route_cache))
        payload["route_cache_hits"] = str(self.stats_route_cache_hits)
        payload["route_cache_misses"] = str(self.stats_route_cache_misses)
        payload["route_osrm_success"] = str(self.stats_route_osrm_success)
        payload["route_osrm_public_success"] = str(self.stats_route_osrm_public_success)
        payload["route_valhalla_public_success"] = str(self.stats_route_valhalla_public_success)
        payload["route_linear_mode"] = str(self.stats_route_linear_mode)
        payload["route_linear_fallback"] = str(self.stats_route_linear_fallback)
        payload["route_osrm_errors"] = str(self.stats_route_osrm_errors)
        payload["route_failure_streak"] = str(self.route_failure_streak)
        payload["route_cooldown_active"] = str(self._route_fetch_in_cooldown()).lower()
        remaining = 0.0
        if self.route_cooldown_until is not None:
            remaining = max(0.0, (self.route_cooldown_until - datetime.now(timezone.utc)).total_seconds())
        payload["route_cooldown_remaining_s"] = str(round(remaining, 2))
        payload["route_prefetch_started"] = str(self.stats_route_prefetch_started)
        payload["route_prefetch_success"] = str(self.stats_route_prefetch_success)
        payload["route_prefetch_errors"] = str(self.stats_route_prefetch_errors)
        payload["route_prefetch_pending"] = str(len(self.route_prefetch_tasks))
        payload["route_osrm_targets"] = ",".join(name for name, _ in self.route_osrm_targets)
        payload["route_source_last"] = self.last_route_source
        await self.redis.hset(STATUS_KEY, mapping=payload)
        await self.redis.expire(STATUS_KEY, 86400)

    async def _ensure_raw_parquet(self) -> bool:
        if self.raw_path.exists() and self.raw_path.stat().st_size > 1_000_000:
            log.info("raw parquet already on disk: %s (%.1f MB)", self.raw_path, self.raw_path.stat().st_size / 1e6)
            return True

        url = f"{TLC_BASE_URL}/fhvhv_tripdata_{self.month}.parquet"
        log.info("downloading TLC HVFHV parquet from %s", url)
        await self._status(state="downloading", url=url)

        tmp_path = self.raw_path.with_suffix(".parquet.tmp")
        try:
            async with httpx.AsyncClient(timeout=600, follow_redirects=True) as client:
                async with client.stream("GET", url) as resp:
                    if resp.status_code != 200:
                        body = (await resp.aread()).decode("utf-8", errors="replace")[:300]
                        log.error("download failed status=%d body=%s", resp.status_code, body)
                        await self._status(state="error", reason=f"download_http_{resp.status_code}")
                        return False
                    total = int(resp.headers.get("content-length", "0"))
                    received = 0
                    last_log = 0
                    with tmp_path.open("wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                            f.write(chunk)
                            received += len(chunk)
                            if received - last_log > 25 * 1024 * 1024:
                                pct = (received / total * 100) if total else 0
                                log.info("download progress: %.1f MB (%.0f%%)", received / 1e6, pct)
                                last_log = received
            tmp_path.rename(self.raw_path)
            log.info("download complete: %.1f MB -> %s", self.raw_path.stat().st_size / 1e6, self.raw_path)
            return True
        except Exception as exc:
            log.error("download error: %s", exc)
            await self._status(state="error", reason=f"download_{type(exc).__name__}")
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

    def _ensure_sorted_parquet(self) -> int:
        if self.sorted_path.exists() and self.sorted_path.stat().st_size > 100_000:
            count = self.duck.execute(
                f"SELECT COUNT(*) FROM read_parquet('{self.sorted_path.as_posix()}')"
            ).fetchone()[0]
            log.info("sorted parquet ready: %s (%d rows)", self.sorted_path, count)
            return int(count)

        log.info("preprocessing: filter %s + sort by request_datetime", TLC_LICENSE_FILTER)
        self.duck.execute(
            f"""
            COPY (
                SELECT
                    hvfhs_license_num,
                    dispatching_base_num,
                    request_datetime,
                    pickup_datetime,
                    dropoff_datetime,
                    PULocationID,
                    DOLocationID,
                    trip_miles,
                    trip_time,
                    base_passenger_fare,
                    tolls,
                    bcf,
                    sales_tax,
                    congestion_surcharge,
                    airport_fee,
                    tips,
                    driver_pay
                FROM read_parquet('{self.raw_path.as_posix()}')
                WHERE hvfhs_license_num = '{TLC_LICENSE_FILTER}'
                  AND request_datetime IS NOT NULL
                  AND pickup_datetime IS NOT NULL
                  AND dropoff_datetime IS NOT NULL
                  AND PULocationID IS NOT NULL
                  AND DOLocationID IS NOT NULL
                ORDER BY request_datetime
            ) TO '{self.sorted_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
        count = self.duck.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.sorted_path.as_posix()}')"
        ).fetchone()[0]
        log.info("sorted parquet written: %d rows", count)
        return int(count)

    async def _load_cursor(self) -> int:
        if self.redis is None:
            return 0
        val = await self.redis.hget(CURSOR_KEY, self.month)
        try:
            return int(val) if val else 0
        except (TypeError, ValueError):
            return 0

    async def _save_cursor(self, offset: int) -> None:
        if self.redis is None:
            return
        await self.redis.hset(CURSOR_KEY, self.month, str(offset))
        await self.redis.expire(CURSOR_KEY, 30 * 86400)

    def _row_to_trip(self, row: dict) -> Trip | None:
        request_ts = row.get("request_datetime")
        pickup_ts = row.get("pickup_datetime")
        dropoff_ts = row.get("dropoff_datetime")
        if not (isinstance(request_ts, datetime) and isinstance(pickup_ts, datetime) and isinstance(dropoff_ts, datetime)):
            return None
        if dropoff_ts <= pickup_ts:
            return None

        pu_loc = int(row.get("PULocationID") or 0)
        do_loc = int(row.get("DOLocationID") or 0)
        if pu_loc <= 0 or do_loc <= 0:
            return None

        trip_miles = float(row.get("trip_miles") or 0)
        trip_time_sec = float(row.get("trip_time") or 0)
        trip_km = round(trip_miles * 1.60934, 3)
        trip_min = round(trip_time_sec / 60.0, 2)
        if trip_min <= 0 or trip_km <= 0:
            return None

        pickup_point = self._centroid_or_none(pu_loc)
        dropoff_point = self._centroid_or_none(do_loc)
        seed = (
            f"{row.get('dispatching_base_num') or ''}|{pu_loc}|{do_loc}|"
            f"{int(request_ts.timestamp())}|{int(pickup_ts.timestamp())}|{int(dropoff_ts.timestamp())}"
        )
        bearing = deterministic_bearing(seed)
        distance_hint_km = max(0.8, min(8.0, trip_km))

        if pickup_point is None and dropoff_point is None:
            # Unknown TLC zones (e.g. 265) should not collapse on Times Square.
            # Build a deterministic synthetic local leg instead of global fallback.
            pickup_lat, pickup_lon = project_point(40.7580, -73.9855, bearing, 1.2)
            dropoff_lat, dropoff_lon = project_point(pickup_lat, pickup_lon, (bearing + 120.0) % 360.0, distance_hint_km)
        elif pickup_point is None and dropoff_point is not None:
            dropoff_lat, dropoff_lon = dropoff_point
            pickup_lat, pickup_lon = project_point(
                dropoff_lat,
                dropoff_lon,
                (bearing + 180.0) % 360.0,
                max(0.6, min(5.0, distance_hint_km * 0.8)),
            )
        elif pickup_point is not None and dropoff_point is None:
            pickup_lat, pickup_lon = pickup_point
            dropoff_lat, dropoff_lon = project_point(
                pickup_lat,
                pickup_lon,
                bearing,
                distance_hint_km,
            )
        else:
            pickup_lat, pickup_lon = pickup_point
            dropoff_lat, dropoff_lon = dropoff_point

        fare_usd = total_fare_usd(row)
        courier_id = synth_courier_id(
            row.get("dispatching_base_num"),
            pu_loc,
            do_loc,
            request_ts,
            pickup_ts,
            dropoff_ts,
        )
        trip_key = f"{int(request_ts.timestamp())}_{pu_loc}_{do_loc}_{int(trip_time_sec)}"
        order_id = f"tlc_{trip_key}"
        offer_id = f"tlc_offer_{trip_key}"

        return Trip(
            trip_key=trip_key,
            courier_id=courier_id,
            offer_id=offer_id,
            order_id=order_id,
            request_ts=request_ts.replace(tzinfo=timezone.utc) if request_ts.tzinfo is None else request_ts,
            pickup_ts=pickup_ts.replace(tzinfo=timezone.utc) if pickup_ts.tzinfo is None else pickup_ts,
            dropoff_ts=dropoff_ts.replace(tzinfo=timezone.utc) if dropoff_ts.tzinfo is None else dropoff_ts,
            pu_loc=pu_loc,
            do_loc=do_loc,
            pickup_lat=pickup_lat,
            pickup_lon=pickup_lon,
            dropoff_lat=dropoff_lat,
            dropoff_lon=dropoff_lon,
            trip_km=trip_km,
            trip_min=trip_min,
            fare_usd=fare_usd,
        )

    def _should_sample(self, trip: Trip) -> bool:
        if TLC_TRIP_SAMPLE_RATE >= 1.0:
            return True
        h = int(hashlib.sha1(trip.trip_key.encode()).hexdigest()[:8], 16)
        return (h % 10_000) / 10_000.0 < TLC_TRIP_SAMPLE_RATE

    async def _send(self, topic: str, key: str, value: bytes) -> None:
        assert self.producer is not None
        await self.producer.send(topic, key=key.encode("utf-8", errors="ignore"), value=value)

    def _build_offer(self, trip: Trip) -> OrderOfferV1:
        return OrderOfferV1(
            event_id=make_event_id("offer", trip.trip_key),
            event_type="order.offer.v1",
            ts=iso_from_dt(trip.request_ts),
            offer_id=trip.offer_id,
            courier_id=trip.courier_id,
            pickup_lat=trip.pickup_lat,
            pickup_lon=trip.pickup_lon,
            dropoff_lat=trip.dropoff_lat,
            dropoff_lon=trip.dropoff_lon,
            estimated_fare_eur=round(trip.fare_usd, 2),
            estimated_distance_km=trip.trip_km,
            estimated_duration_min=trip.trip_min,
            demand_index=1.0,
            weather_factor=1.0,
            traffic_factor=1.0,
            zone_id=f"nyc_{trip.pu_loc}",
            source_platform=source_platform_for_route(TLC_SOURCE_PLATFORM, trip.route_source),
        )

    def _build_event(self, trip: Trip, status: str, ts: datetime, actuals: bool) -> OrderEventV1:
        return OrderEventV1(
            event_id=make_event_id(f"evt_{status}", trip.trip_key),
            event_type="order.event.v1",
            ts=iso_from_dt(ts),
            offer_id=trip.offer_id,
            order_id=trip.order_id,
            courier_id=trip.courier_id,
            status=status,
            actual_fare_eur=round(trip.fare_usd, 2) if actuals else 0.0,
            actual_distance_km=trip.trip_km if actuals else 0.0,
            actual_duration_min=trip.trip_min if actuals else 0.0,
            zone_id=f"nyc_{trip.pu_loc}",
            source_platform=source_platform_for_route(TLC_SOURCE_PLATFORM, trip.route_source),
        )

    def _build_position(
        self,
        trip: Trip,
        lat: float,
        lon: float,
        ts: datetime,
        status: str,
        *,
        heading_deg_override: float | None = None,
        speed_kmh_override: float | None = None,
        route_source_override: str | None = None,
    ) -> CourierPositionV1:
        moving = status in {"delivering", "repositioning"}
        heading = float(heading_deg_override) if (moving and heading_deg_override is not None) else (float(trip.bearing) if moving else 0.0)
        speed_kmh = float(speed_kmh_override) if (moving and speed_kmh_override is not None) else (float(trip.avg_speed_kmh) if moving else 0.0)
        return CourierPositionV1(
            event_id=make_event_id(f"pos_{status}", f"{trip.trip_key}_{int(ts.timestamp())}"),
            event_type="courier.position.v1",
            ts=iso_from_dt(ts),
            courier_id=trip.courier_id,
            lat=round(lat, 6),
            lon=round(lon, 6),
            speed_kmh=round(speed_kmh, 1) if moving else 0.0,
            heading_deg=round(heading, 1),
            status=status,
            accuracy_m=8.0,
            battery_pct=100.0,
            source_platform=source_platform_for_route(TLC_SOURCE_PLATFORM, route_source_override or trip.route_source),
        )

    async def _emit_trip_start(self, trip: Trip) -> None:
        offer = self._build_offer(trip)
        await self._send(ORDER_OFFERS_TOPIC, trip.courier_id, offer.SerializeToString())
        self.stats_emitted_offers += 1

        accepted = self._build_event(trip, "accepted", trip.request_ts, actuals=False)
        await self._send(ORDER_EVENTS_TOPIC, trip.courier_id, accepted.SerializeToString())
        self.stats_emitted_events += 1

        if self._trip_needs_reposition(trip):
            pos = self._build_position(
                trip,
                trip.reposition_start_lat,
                trip.reposition_start_lon,
                trip.reposition_start_ts,
                "repositioning",
                speed_kmh_override=TLC_FLEET_REPOSITION_KMH,
                route_source_override=trip.reposition_source,
            )
        else:
            pos = self._build_position(trip, trip.pickup_lat, trip.pickup_lon, trip.request_ts, "pickup_arrived")
        await self._send(COURIER_TOPIC, trip.courier_id, pos.SerializeToString())
        self.stats_emitted_positions += 1

    async def _emit_trip_progress(self, trip: Trip, now: datetime) -> None:
        if self._trip_needs_reposition(trip) and now < trip.pickup_ts:
            if now < trip.reposition_start_ts:
                return
            total = (trip.pickup_ts - trip.reposition_start_ts).total_seconds()
            if total <= 0:
                return
            elapsed = (now - trip.reposition_start_ts).total_seconds()
            progress = max(0.0, min(1.0, elapsed / total))
            lat, lon, heading = interpolate_on_geometry(
                trip.reposition_geometry,
                trip.reposition_cumulative_km,
                progress,
            )
            pos = self._build_position(
                trip,
                lat,
                lon,
                now,
                "repositioning",
                heading_deg_override=heading,
                speed_kmh_override=TLC_FLEET_REPOSITION_KMH,
                route_source_override=trip.reposition_source,
            )
            await self._send(COURIER_TOPIC, trip.courier_id, pos.SerializeToString())
            self.stats_emitted_positions += 1
            return

        total = (trip.dropoff_ts - trip.pickup_ts).total_seconds()
        if total <= 0:
            return
        elapsed = (now - trip.pickup_ts).total_seconds()
        progress = max(0.0, min(1.0, elapsed / total))
        lat, lon, heading = interpolate_on_geometry(
            trip.route_geometry,
            trip.route_cumulative_km,
            progress,
        )
        pos = self._build_position(
            trip,
            lat,
            lon,
            now,
            "delivering",
            heading_deg_override=heading,
        )
        await self._send(COURIER_TOPIC, trip.courier_id, pos.SerializeToString())
        self.stats_emitted_positions += 1

    async def _emit_trip_finish(self, trip: Trip) -> None:
        pos = self._build_position(trip, trip.dropoff_lat, trip.dropoff_lon, trip.dropoff_ts, "available")
        await self._send(COURIER_TOPIC, trip.courier_id, pos.SerializeToString())
        self.stats_emitted_positions += 1

        done = self._build_event(trip, "dropped_off", trip.dropoff_ts, actuals=True)
        await self._send(ORDER_EVENTS_TOPIC, trip.courier_id, done.SerializeToString())
        self.stats_emitted_events += 1

    async def replay(self, total_rows: int) -> None:
        cursor = await self._load_cursor()
        initial_cursor = cursor
        log.info(
            "replay starting cursor=%d/%d speed=%.1fx sample=%.2f max_active=%d tick=%.1fs trajectory_mode=%s",
            cursor,
            total_rows,
            TLC_SPEED_FACTOR,
            TLC_TRIP_SAMPLE_RATE,
            TLC_MAX_ACTIVE_TRIPS,
            TLC_TICK_INTERVAL_SEC,
            TLC_ROUTE_MODE,
        )

        result = self.duck.execute(
            f"SELECT * FROM read_parquet('{self.sorted_path.as_posix()}') OFFSET {cursor}"
        )
        cols = [d[0] for d in result.description]

        pending: Trip | None = None
        tick_count = 0
        last_log = asyncio.get_event_loop().time()

        def _fetch_next_trip() -> Trip | None:
            nonlocal cursor
            while True:
                rows = result.fetchmany(1)
                if not rows:
                    return None
                cursor += 1
                row = dict(zip(cols, rows[0]))
                trip = self._row_to_trip(row)
                if trip is None:
                    continue
                if not self._should_sample(trip):
                    self.stats_skipped_sample += 1
                    continue
                return trip

        pending = await asyncio.to_thread(_fetch_next_trip)
        if pending is None:
            log.info("no trips to replay")
            return
        skipped_for_start = 0
        if initial_cursor == 0 and TLC_START_HOUR_UTC >= 0:
            target_start = pending.request_ts.replace(
                hour=TLC_START_HOUR_UTC,
                minute=0,
                second=0,
                microsecond=0,
            )
            if pending.request_ts >= target_start:
                target_start = target_start + timedelta(days=1)
            while pending is not None and pending.request_ts < target_start:
                skipped_for_start += 1
                pending = await asyncio.to_thread(_fetch_next_trip)
            if skipped_for_start > 0:
                log.info(
                    "startup fast-forward applied target=%s skipped_rows=%d cursor=%d",
                    target_start.isoformat(),
                    skipped_for_start,
                    cursor,
                )
        if pending is None:
            log.info("no trips to replay after startup fast-forward")
            return
        self.virtual_time = pending.request_ts
        log.info("virtual clock starting at %s", iso_from_dt(self.virtual_time))

        while not self._stop_flag:
            tick_count += 1
            assert self.virtual_time is not None

            while pending is not None and pending.request_ts <= self.virtual_time:
                if len(self.active_trips) >= TLC_MAX_ACTIVE_TRIPS:
                    self.stats_dropped_no_capacity += 1
                else:
                    assigned_courier_id = pending.courier_id
                    if self.fleet_pool_enabled:
                        allocated = self._pick_fleet_driver(pending)
                        if not allocated:
                            self.stats_dropped_no_capacity += 1
                            pending = await asyncio.to_thread(_fetch_next_trip)
                            if pending is None:
                                break
                            continue
                        assigned_courier_id = allocated
                    if assigned_courier_id in self.active_trips:
                        self.stats_dropped_no_capacity += 1
                    else:
                        pending.courier_id = assigned_courier_id
                        await self._prepare_fleet_reposition(pending)
                        await self._prepare_trip_route(pending)
                        self.active_trips[pending.courier_id] = pending
                        await self._emit_trip_start(pending)
                pending = await asyncio.to_thread(_fetch_next_trip)
                if pending is None:
                    break

            finished: list[str] = []
            for cid, trip in self.active_trips.items():
                if trip.dropoff_ts <= self.virtual_time:
                    await self._emit_trip_finish(trip)
                    self._mark_fleet_driver_available(trip)
                    finished.append(cid)
                elif self.virtual_time >= trip.pickup_ts:
                    await self._emit_trip_progress(trip, self.virtual_time)
                elif self.virtual_time >= trip.reposition_start_ts:
                    await self._emit_trip_progress(trip, self.virtual_time)
            for cid in finished:
                self.active_trips.pop(cid, None)

            now_wall = asyncio.get_event_loop().time()
            if now_wall - last_log >= 15.0:
                await self._save_cursor(cursor)
                await self._status(
                    state="running",
                    cursor=cursor,
                    total=total_rows,
                    virtual_time=iso_from_dt(self.virtual_time),
                    active_trips=len(self.active_trips),
                    offers=self.stats_emitted_offers,
                    positions=self.stats_emitted_positions,
                    events=self.stats_emitted_events,
                    skipped_sample=self.stats_skipped_sample,
                    dropped_no_capacity=self.stats_dropped_no_capacity,
                )
                log.info(
                    "tick=%d cursor=%d/%d (%.2f%%) vtime=%s active=%d offers=%d pos=%d evt=%d dropped=%d route(osrm=%d osrm_public=%d valhalla=%d linear_mode=%d linear_fallback=%d prefetch=%d/%d/%d cache=%d/%d cooldown=%s)",
                    tick_count,
                    cursor,
                    total_rows,
                    cursor / max(total_rows, 1) * 100,
                    self.virtual_time.strftime("%Y-%m-%d %H:%M:%S"),
                    len(self.active_trips),
                    self.stats_emitted_offers,
                    self.stats_emitted_positions,
                    self.stats_emitted_events,
                    self.stats_dropped_no_capacity,
                    self.stats_route_osrm_success,
                    self.stats_route_osrm_public_success,
                    self.stats_route_valhalla_public_success,
                    self.stats_route_linear_mode,
                    self.stats_route_linear_fallback,
                    self.stats_route_prefetch_success,
                    self.stats_route_prefetch_errors,
                    len(self.route_prefetch_tasks),
                    self.stats_route_cache_hits,
                    self.stats_route_cache_misses,
                    self._route_fetch_in_cooldown(),
                )
                last_log = now_wall

            if pending is None and not self.active_trips:
                log.info("replay drained - no more trips and no active trips")
                break

            tick_factor = self._tick_accel_factor(self.virtual_time)
            self.virtual_time = self.virtual_time + timedelta(seconds=TLC_TICK_INTERVAL_SEC * tick_factor)
            await asyncio.sleep(TLC_TICK_INTERVAL_SEC / max(TLC_SPEED_FACTOR, 0.01))

        await self._save_cursor(cursor)
        await self._status(
            state="finished" if pending is None else "stopped",
            cursor=cursor,
            total=total_rows,
            offers=self.stats_emitted_offers,
            positions=self.stats_emitted_positions,
            events=self.stats_emitted_events,
        )
        log.info(
            "replay loop ended: cursor=%d offers=%d positions=%d events=%d",
            cursor,
            self.stats_emitted_offers,
            self.stats_emitted_positions,
            self.stats_emitted_events,
        )

        if TLC_LOOP_ON_FINISH and pending is None:
            log.info("loop_on_finish=true -> resetting cursor for next iteration")
            await self._save_cursor(0)


async def reset_runtime_state(redis: aioredis.Redis) -> dict[str, int]:
    """Wipe all replay + fleet runtime keys so the demo restarts clean.

    This intentionally leaves the ML model pickle alone: only Redis-backed
    runtime state (replay cursors, live positions, offers, missions, DLQ
    counters) is cleared. Parquet files on disk are not touched either —
    they are training inputs, not runtime state.
    """
    deleted = 0
    for key in RUNTIME_RESET_KEYS:
        try:
            deleted += int(await redis.delete(key) or 0)
        except Exception as exc:
            log.warning("runtime reset: delete %s failed: %s", key, exc)

    scanned = 0
    for pattern in RUNTIME_RESET_PATTERNS:
        cursor = 0
        try:
            while True:
                cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=500)
                if keys:
                    deleted += int(await redis.delete(*keys) or 0)
                    scanned += len(keys)
                if cursor == 0:
                    break
        except Exception as exc:
            log.warning("runtime reset: scan %s failed: %s", pattern, exc)

    # geo sorted-sets: wipe the main fleet GEO key (hot path will rebuild it)
    try:
        deleted += int(await redis.delete("fleet:geo") or 0)
    except Exception:
        pass

    log.info("runtime reset: deleted=%d scanned=%d", deleted, scanned)
    return {"deleted": deleted, "scanned": scanned}


def _resolve_live_months() -> list[str]:
    """Months actually used by the replay loop.

    When TLC_SCENARIO=single_driver and TLC_TRAIN_MONTH_COUNT/TLC_LIVE_MONTH_COUNT
    are both set, we skip the training window entirely and replay only the live
    tail. Otherwise we fall back to the legacy resolver.
    """
    base = resolve_replay_months()
    if TLC_SCENARIO == "single_driver" and TLC_TRAIN_MONTH_COUNT > 0 and TLC_LIVE_MONTH_COUNT > 0:
        total_needed = TLC_TRAIN_MONTH_COUNT + TLC_LIVE_MONTH_COUNT
        base = _build_month_sequence(TLC_MONTH, total_needed)
        live = base[TLC_TRAIN_MONTH_COUNT:]
        if not live:
            log.warning("live window empty, falling back to last month of training window")
            return [base[-1]]
        log.info(
            "single-driver scenario: train window=%s..%s | live window=%s..%s",
            base[0],
            base[TLC_TRAIN_MONTH_COUNT - 1] if TLC_TRAIN_MONTH_COUNT > 0 else base[0],
            live[0],
            live[-1],
        )
        return live
    return base


async def main() -> None:
    try:
        months = _resolve_live_months()
    except ValueError as exc:
        log.error("invalid month configuration: %s", exc)
        return
    replay = TLCReplay(months[0])
    scenario: Any = None

    def _on_signal(*_: object) -> None:
        log.info("signal received, stopping replay")
        replay._stop_flag = True
        # The single-driver scenario carries its own _stop flag that the
        # replay loop does not mutate mid-run. If we only flipped
        # replay._stop_flag, scenario.run() would keep looping until the
        # next month boundary. Propagate the stop explicitly.
        if scenario is not None:
            scenario.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    for attempt in range(1, 11):
        try:
            await replay.start()
            log.info(
                "tlc replay started scenario=%s months=%s license=%s speed=%.1fx",
                TLC_SCENARIO,
                ",".join(months),
                TLC_LICENSE_FILTER,
                TLC_SPEED_FACTOR,
            )
            break
        except Exception as exc:
            log.warning("start attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to start tlc replay")
        return

    if TLC_RESET_RUNTIME_ON_START and replay.redis is not None:
        await reset_runtime_state(replay.redis)

    if TLC_SCENARIO == "single_driver":
        from single_driver import SingleDriverScenario

        scenario = SingleDriverScenario(replay)
        await scenario.start()

    try:
        while not replay._stop_flag:
            for idx, month in enumerate(months, start=1):
                if replay._stop_flag:
                    break

                replay.configure_month(month)
                log.info("starting month %s (%d/%d) scenario=%s", month, idx, len(months), TLC_SCENARIO)

                ok = await replay._ensure_raw_parquet()
                if not ok:
                    log.warning("month %s skipped: source unavailable", month)
                    await asyncio.sleep(10)
                    continue

                try:
                    total_rows = await asyncio.to_thread(replay._ensure_sorted_parquet)
                except Exception as exc:
                    log.error("preprocessing failed month=%s: %s", month, exc)
                    await replay._status(state="error", reason=f"preprocess_{type(exc).__name__}")
                    await asyncio.sleep(10)
                    continue

                if total_rows <= 0:
                    log.warning("month %s has no rows, skipping", month)
                    await asyncio.sleep(5)
                    continue

                if scenario is not None:
                    await scenario.run(total_rows)
                else:
                    await replay.replay(total_rows)

            if not TLC_LOOP_ON_FINISH:
                log.info("loop_on_finish=false -> all configured months processed, idle until restart")
                while not replay._stop_flag:
                    await asyncio.sleep(60)
    finally:
        if scenario is not None:
            await scenario.close()
        await replay.close()
        log.info("tlc replay stopped")


if __name__ == "__main__":
    asyncio.run(main())
