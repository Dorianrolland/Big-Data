"""
context_poller — streams real NYC market-context signals to Kafka.

Periodically hits public APIs (Citi Bike GBFS, Open-Meteo, NYC 311 Socrata,
NYC Permitted Event Information)
and publishes one ContextSignalV1 per NYC taxi zone every CONTEXT_TICK seconds
on the context-signals-v1 topic.

Signal fields populated from real data:
- demand_index   : 1.0 + f(Citi Bike station scarcity) + event_pressure (capped)
- supply_index   : live courier count (GEOSEARCH around the zone centroid) / scale
- weather_factor : 1.0 + f(precipitation) + f(wind)      — Open-Meteo NYC
- traffic_factor : 1.0 + f(precipitation) + f(311 traffic complaints near zone)
- source         : short tag describing which APIs fed this cycle
"""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta
import hashlib
import json
import logging
import math
import os
import signal
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

from copilot_events_pb2 import ContextSignalV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("context-poller")


def _env_float(name: str, default: float, *, min_value: float | None = None) -> float:
    try:
        out = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        out = float(default)
    if not math.isfinite(out):
        out = float(default)
    if min_value is not None:
        out = max(float(min_value), out)
    return float(out)


def _env_int(name: str, default: int, *, min_value: int | None = None) -> int:
    try:
        out = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        out = int(default)
    if min_value is not None:
        out = max(int(min_value), out)
    return int(out)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
CONTEXT_SIGNALS_TOPIC = os.getenv("CONTEXT_SIGNALS_TOPIC", "context-signals-v1")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
COURIER_GEO_KEY = os.getenv("COURIER_GEO_KEY", "fleet:geo")

CONTEXT_TICK_SECONDS = _env_float("CONTEXT_TICK_SECONDS", 30.0, min_value=1.0)
GBFS_POLL_SECONDS = _env_float("GBFS_POLL_SECONDS", 60.0, min_value=5.0)
WEATHER_POLL_SECONDS = _env_float("WEATHER_POLL_SECONDS", 600.0, min_value=30.0)
NYC311_POLL_SECONDS = _env_float("NYC311_POLL_SECONDS", 300.0, min_value=30.0)
EVENTS_POLL_SECONDS = _env_float("EVENTS_POLL_SECONDS", 600.0, min_value=60.0)
CONTEXT_QUALITY_KEY = os.getenv("CONTEXT_QUALITY_KEY", "copilot:context:quality")
CONTEXT_QUALITY_TTL_SECONDS = _env_int("CONTEXT_QUALITY_TTL_SECONDS", 7200, min_value=300)
CONTEXT_QUALITY_WINDOW_CYCLES = _env_int("CONTEXT_QUALITY_WINDOW_CYCLES", 10, min_value=2)
CONTEXT_SUPPLY_VARIANCE_EPSILON = _env_float("CONTEXT_SUPPLY_VARIANCE_EPSILON", 0.03, min_value=0.0)
CONTEXT_SUPPLY_FLAT_MIN_CYCLES = _env_int("CONTEXT_SUPPLY_FLAT_MIN_CYCLES", 6, min_value=2)
EVENTS_CONTEXT_KEY = os.getenv("EVENTS_CONTEXT_KEY", "copilot:context:events")
EVENTS_CONTEXT_TTL_SECONDS = _env_int("EVENTS_CONTEXT_TTL_SECONDS", 1800, min_value=300)

GBFS_STATION_INFO_URL = os.getenv(
    "GBFS_STATION_INFO_URL",
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json",
)
GBFS_STATION_STATUS_URL = os.getenv(
    "GBFS_STATION_STATUS_URL",
    "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json",
)
OPEN_METEO_URL = os.getenv(
    "OPEN_METEO_URL",
    "https://api.open-meteo.com/v1/forecast?latitude=40.7580&longitude=-73.9855&current=temperature_2m,precipitation,wind_speed_10m,weather_code",
)
NYC311_URL = os.getenv(
    "NYC311_URL",
    "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
)
NYC_EVENTS_URL = os.getenv(
    "NYC_EVENTS_URL",
    "https://data.cityofnewyork.us/resource/tvpp-9vvx.json",
)

HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "FleetStream/1.0 (context-poller; contact=ops@fleetstream.local)")

CENTROIDS_PATH = Path(__file__).parent / "nyc_zone_centroids.json"

DEMAND_RADIUS_KM = _env_float("DEMAND_RADIUS_KM", 0.8, min_value=0.1)
SUPPLY_RADIUS_KM = _env_float("SUPPLY_RADIUS_KM", 1.0, min_value=0.1)
TRAFFIC_RADIUS_KM = _env_float("TRAFFIC_RADIUS_KM", 1.5, min_value=0.1)
EVENT_RADIUS_KM = _env_float("EVENT_RADIUS_KM", 4.5, min_value=0.2)
EVENT_LOOKAHEAD_HOURS = _env_float("EVENT_LOOKAHEAD_HOURS", 6.0, min_value=0.25)
EVENT_POST_WINDOW_MINUTES = _env_float("EVENT_POST_WINDOW_MINUTES", 60.0, min_value=0.0)
EVENT_PRESSURE_SCALE = _env_float("EVENT_PRESSURE_SCALE", 0.55, min_value=0.0)
EVENT_PRESSURE_CAP = _env_float("EVENT_PRESSURE_CAP", 0.75, min_value=0.0)
DEMAND_INDEX_CAP = _env_float("DEMAND_INDEX_CAP", 3.2, min_value=1.0)

EARTH_KM = 6371.0
NYC_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")
BoroughCentroid = tuple[float, float]

BOROUGH_CENTROIDS: dict[str, BoroughCentroid] = {
    "manhattan": (40.7831, -73.9712),
    "brooklyn": (40.6782, -73.9442),
    "queens": (40.7282, -73.7949),
    "bronx": (40.8448, -73.8648),
    "staten_island": (40.5795, -74.1502),
}


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())


def event_id(prefix: str, seed: str) -> str:
    return f"{prefix}_{hashlib.sha1(seed.encode()).hexdigest()[:20]}"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return EARTH_KM * 2 * math.asin(math.sqrt(a))


def _coerce_float(value: object) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _normalize_borough(value: object) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    aliases = {
        "manhattan": "manhattan",
        "mn": "manhattan",
        "m": "manhattan",
        "new york": "manhattan",
        "brooklyn": "brooklyn",
        "bk": "brooklyn",
        "k": "brooklyn",
        "queens": "queens",
        "qn": "queens",
        "q": "queens",
        "bronx": "bronx",
        "bx": "bronx",
        "x": "bronx",
        "staten island": "staten_island",
        "staten_island": "staten_island",
        "si": "staten_island",
        "r": "staten_island",
    }
    return aliases.get(raw)


def _event_borough_centroid(value: object) -> tuple[float, float] | None:
    borough = _normalize_borough(value)
    if not borough:
        return None
    return BOROUGH_CENTROIDS.get(borough)


def _parse_event_datetime(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            try:
                parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return None
        parsed = parsed.replace(tzinfo=NYC_TZ)
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=NYC_TZ)
    return parsed.astimezone(UTC_TZ)


def _event_intensity(event_type: object, event_name: object) -> float:
    text = f"{event_type or ''} {event_name or ''}".lower()
    if not text.strip():
        return 1.0
    if any(token in text for token in ("parade", "festival", "fair", "concert", "marathon", "carnival")):
        return 1.25
    if any(token in text for token in ("street", "block party", "community", "cultural")):
        return 1.1
    if any(token in text for token in ("sport", "baseball", "soccer", "basketball", "tournament")):
        return 0.95
    return 1.0


def _event_time_weight(
    *,
    now_ts: float,
    start_ts: float,
    end_ts: float,
    lookahead_hours: float,
    post_window_minutes: float,
) -> float:
    lookahead_s = max(900.0, lookahead_hours * 3600.0)
    post_s = max(0.0, post_window_minutes * 60.0)
    if end_ts < (now_ts - post_s):
        return 0.0
    if start_ts <= now_ts <= end_ts:
        return 1.0
    if now_ts < start_ts:
        lead_s = start_ts - now_ts
        if lead_s > lookahead_s:
            return 0.0
        # Upcoming events increase progressively as we approach start.
        return 0.25 + (0.75 * (1.0 - (lead_s / lookahead_s)))
    if post_s <= 0:
        return 0.0
    tail_s = now_ts - end_ts
    if tail_s > post_s:
        return 0.0
    return 0.35 * (1.0 - (tail_s / post_s))


def _compute_event_pressure(
    events: list[dict[str, object]],
    *,
    lat: float,
    lon: float,
    now_ts: float,
    radius_km: float = EVENT_RADIUS_KM,
    lookahead_hours: float = EVENT_LOOKAHEAD_HOURS,
    post_window_minutes: float = EVENT_POST_WINDOW_MINUTES,
    scale: float = EVENT_PRESSURE_SCALE,
    cap: float = EVENT_PRESSURE_CAP,
) -> float:
    if not events:
        return 0.0

    score = 0.0
    effective_radius = max(0.2, radius_km)
    deg_lat = effective_radius / 111.32
    deg_lon = effective_radius / max(111.32 * math.cos(math.radians(lat)), 1e-6)
    lat_lo, lat_hi = lat - deg_lat, lat + deg_lat
    lon_lo, lon_hi = lon - deg_lon, lon + deg_lon
    for evt in events:
        evt_lat = _coerce_float(evt.get("lat"))
        evt_lon = _coerce_float(evt.get("lon"))
        evt_start = _coerce_float(evt.get("start_ts"))
        evt_end = _coerce_float(evt.get("end_ts"))
        evt_intensity = _coerce_float(evt.get("intensity")) or 1.0
        if evt_lat is None or evt_lon is None or evt_start is None or evt_end is None:
            continue
        if evt_lat < lat_lo or evt_lat > lat_hi or evt_lon < lon_lo or evt_lon > lon_hi:
            continue

        time_w = _event_time_weight(
            now_ts=now_ts,
            start_ts=evt_start,
            end_ts=evt_end,
            lookahead_hours=lookahead_hours,
            post_window_minutes=post_window_minutes,
        )
        if time_w <= 0.0:
            continue

        distance_km = haversine_km(lat, lon, evt_lat, evt_lon)
        if distance_km > effective_radius:
            continue

        distance_w = max(0.0, 1.0 - (distance_km / effective_radius)) ** 2
        score += evt_intensity * time_w * distance_w

    pressure = max(0.0, min(float(cap), float(scale) * score))
    return round(pressure, 4)


def _build_event_record(row: dict[str, object]) -> dict[str, object] | None:
    start_dt = _parse_event_datetime(row.get("start_date_time"))
    end_dt = _parse_event_datetime(row.get("end_date_time"))
    if start_dt is None or end_dt is None:
        return None
    if end_dt <= start_dt:
        return None

    coords = _event_borough_centroid(row.get("event_borough"))
    if coords is None:
        return None

    lat, lon = coords
    return {
        "event_id": str(row.get("event_id") or "").strip(),
        "event_name": str(row.get("event_name") or "").strip(),
        "event_type": str(row.get("event_type") or "").strip(),
        "event_borough": str(row.get("event_borough") or "").strip(),
        "event_location": str(row.get("event_location") or "").strip(),
        "lat": lat,
        "lon": lon,
        "start_ts": start_dt.timestamp(),
        "end_ts": end_dt.timestamp(),
        "intensity": _event_intensity(row.get("event_type"), row.get("event_name")),
    }


def load_centroids() -> dict[int, tuple[float, float]]:
    if not CENTROIDS_PATH.exists():
        raise FileNotFoundError(f"missing centroids: {CENTROIDS_PATH}")
    raw = json.loads(CENTROIDS_PATH.read_text(encoding="utf-8"))
    return {int(k): (float(v[0]), float(v[1])) for k, v in raw.items()}


class ContextState:
    """Shared latest-snapshot from each source, updated by pollers, read by publisher."""

    def __init__(self) -> None:
        self.gbfs_stations: list[dict] = []  # each: {lat, lon, bikes, docks}
        self.gbfs_last_updated: float = 0.0
        self.weather_precip_mm: float = 0.0
        self.weather_wind_kmh: float = 0.0
        self.weather_temp_c: float | None = None
        self.weather_code: int = 0
        self.weather_last_updated: float = 0.0
        self.nyc311_incidents: list[tuple[float, float]] = []  # (lat, lon)
        self.nyc311_last_updated: float = 0.0
        self.events: list[dict[str, object]] = []
        self.events_last_updated: float = 0.0
        self.events_source_status: str = "idle"
        self.events_rows: int = 0
        self.sources_active: set[str] = set()


async def poll_gbfs(state: ContextState, stop: asyncio.Event) -> None:
    async with httpx.AsyncClient(timeout=20, headers={"User-Agent": HTTP_USER_AGENT}) as client:
        while not stop.is_set():
            try:
                info_resp = await client.get(GBFS_STATION_INFO_URL)
                info_resp.raise_for_status()
                info = info_resp.json().get("data", {}).get("stations", [])
                info_by_id = {s.get("station_id"): s for s in info if s.get("station_id")}

                status_resp = await client.get(GBFS_STATION_STATUS_URL)
                status_resp.raise_for_status()
                statuses = status_resp.json().get("data", {}).get("stations", [])

                stations: list[dict] = []
                for s in statuses:
                    sid = s.get("station_id")
                    info_row = info_by_id.get(sid)
                    if not info_row:
                        continue
                    lat = info_row.get("lat")
                    lon = info_row.get("lon")
                    if lat is None or lon is None:
                        continue
                    bikes = int(s.get("num_bikes_available") or 0)
                    docks = int(s.get("num_docks_available") or 0)
                    stations.append({"lat": float(lat), "lon": float(lon), "bikes": bikes, "docks": docks})

                state.gbfs_stations = stations
                state.gbfs_last_updated = time.time()
                state.sources_active.add("gbfs")
                log.info("gbfs refreshed: %d stations", len(stations))
            except Exception as exc:
                log.warning("gbfs poll failed: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=GBFS_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


async def poll_weather(state: ContextState, stop: asyncio.Event) -> None:
    async with httpx.AsyncClient(timeout=20, headers={"User-Agent": HTTP_USER_AGENT}) as client:
        while not stop.is_set():
            try:
                resp = await client.get(OPEN_METEO_URL)
                resp.raise_for_status()
                data = resp.json().get("current", {})
                state.weather_precip_mm = float(data.get("precipitation") or 0.0)
                state.weather_wind_kmh = float(data.get("wind_speed_10m") or 0.0)
                temp = data.get("temperature_2m")
                state.weather_temp_c = float(temp) if temp is not None else None
                state.weather_code = int(data.get("weather_code") or 0)
                state.weather_last_updated = time.time()
                state.sources_active.add("open-meteo")
                log.info(
                    "weather refreshed: temp=%.1f°C precip=%.2fmm wind=%.1fkm/h code=%d",
                    state.weather_temp_c if state.weather_temp_c is not None else -99.0,
                    state.weather_precip_mm,
                    state.weather_wind_kmh,
                    state.weather_code,
                )
            except Exception as exc:
                log.warning("weather poll failed: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=WEATHER_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


async def poll_nyc311(state: ContextState, stop: asyncio.Event) -> None:
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": HTTP_USER_AGENT}) as client:
        while not stop.is_set():
            try:
                window_start = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 6 * 3600))
                params = {
                    "$where": f"created_date > '{window_start}' AND (complaint_type like '%Traffic%' OR complaint_type like '%Blocked%' OR complaint_type like '%Illegal Parking%')",
                    "$select": "latitude,longitude,complaint_type,created_date",
                    "$limit": "2000",
                }
                resp = await client.get(NYC311_URL, params=params)
                resp.raise_for_status()
                rows = resp.json()
                incidents: list[tuple[float, float]] = []
                for row in rows:
                    lat = row.get("latitude")
                    lon = row.get("longitude")
                    if lat is None or lon is None:
                        continue
                    try:
                        incidents.append((float(lat), float(lon)))
                    except (TypeError, ValueError):
                        continue
                state.nyc311_incidents = incidents
                state.nyc311_last_updated = time.time()
                state.sources_active.add("nyc311")
                log.info("nyc311 refreshed: %d traffic incidents in last 6h", len(incidents))
            except Exception as exc:
                log.warning("nyc311 poll failed: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=NYC311_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


async def poll_nyc_events(
    state: ContextState,
    redis_client: aioredis.Redis,
    stop: asyncio.Event,
) -> None:
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": HTTP_USER_AGENT}) as client:
        while not stop.is_set():
            now_local = datetime.now(tz=NYC_TZ)
            start_local = now_local - timedelta(minutes=max(15.0, EVENT_POST_WINDOW_MINUTES))
            end_local = now_local + timedelta(hours=max(1.0, EVENT_LOOKAHEAD_HOURS))
            params = {
                "$select": (
                    "event_id,event_name,event_type,event_borough,event_location,start_date_time,end_date_time"
                ),
                "$where": (
                    f"end_date_time >= '{start_local.strftime('%Y-%m-%dT%H:%M:%S')}' "
                    f"AND start_date_time <= '{end_local.strftime('%Y-%m-%dT%H:%M:%S')}'"
                ),
                "$limit": "5000",
            }
            error_type = ""
            events: list[dict[str, object]] = []
            try:
                resp = await client.get(NYC_EVENTS_URL, params=params)
                resp.raise_for_status()
                rows = resp.json()
                if not isinstance(rows, list):
                    rows = []

                seen_ids: set[str] = set()
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    parsed = _build_event_record(row)
                    if not parsed:
                        continue
                    event_id_raw = str(parsed.get("event_id") or "").strip()
                    dedupe_key = event_id_raw or (
                        f"{parsed.get('event_name')}|{parsed.get('start_ts')}|{parsed.get('event_borough')}"
                    )
                    if dedupe_key in seen_ids:
                        continue
                    seen_ids.add(dedupe_key)
                    events.append(parsed)

                state.events = events
                state.events_rows = len(events)
                state.events_last_updated = time.time()
                state.events_source_status = "ok"
                state.sources_active.add("nyc-events")
                log.info("nyc events refreshed: %d upcoming events", len(events))
            except Exception as exc:
                state.events = []
                state.events_rows = 0
                state.events_source_status = "degraded"
                state.sources_active.discard("nyc-events")
                error_type = type(exc).__name__
                log.warning("nyc events poll failed: %s", exc)

            now_iso = utc_now_iso()
            try:
                await redis_client.hset(
                    EVENTS_CONTEXT_KEY,
                    mapping={
                        "source": "nyc_permitted_event_information_tvpp-9vvx",
                        "status": state.events_source_status,
                        "events_upcoming_count": str(state.events_rows),
                        "events_window_start": start_local.isoformat(),
                        "events_window_end": end_local.isoformat(),
                        "error_type": error_type,
                        "updated_at": now_iso,
                    },
                )
                await redis_client.expire(EVENTS_CONTEXT_KEY, EVENTS_CONTEXT_TTL_SECONDS)
            except Exception as exc:
                log.warning("events context redis update failed: %s", exc)

            try:
                await asyncio.wait_for(stop.wait(), timeout=EVENTS_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


def _count_within(points: list[tuple[float, float]], lat: float, lon: float, radius_km: float) -> int:
    if not points:
        return 0
    # Fast bounding-box prefilter to avoid haversine for obvious outliers
    deg_lat = radius_km / 111.32
    deg_lon = radius_km / max(111.32 * math.cos(math.radians(lat)), 1e-6)
    lat_lo, lat_hi = lat - deg_lat, lat + deg_lat
    lon_lo, lon_hi = lon - deg_lon, lon + deg_lon
    c = 0
    for plat, plon in points:
        if plat < lat_lo or plat > lat_hi or plon < lon_lo or plon > lon_hi:
            continue
        if haversine_km(lat, lon, plat, plon) <= radius_km:
            c += 1
    return c


def _compute_zone_demand(
    state: ContextState,
    lat: float,
    lon: float,
    *,
    event_pressure: float = 0.0,
) -> float:
    """Demand proxy: how many Citi Bike stations near the centroid are empty/low.

    More empty stations -> more unmet micro-mobility demand -> taxi demand bump.
    """
    if not state.gbfs_stations:
        return round(min(DEMAND_INDEX_CAP, 1.0 + max(0.0, float(event_pressure))), 3)
    stations_nearby = [
        (s["lat"], s["lon"], s["bikes"])
        for s in state.gbfs_stations
        if abs(s["lat"] - lat) < 0.02 and abs(s["lon"] - lon) < 0.025
    ]
    if not stations_nearby:
        return round(min(DEMAND_INDEX_CAP, 1.0 + max(0.0, float(event_pressure))), 3)
    low = 0
    for slat, slon, bikes in stations_nearby:
        if haversine_km(lat, lon, slat, slon) <= DEMAND_RADIUS_KM and bikes < 3:
            low += 1
    base = 1.0 + min(1.5, low / 5.0)
    adjusted = min(DEMAND_INDEX_CAP, base + max(0.0, float(event_pressure)))
    return round(adjusted, 3)


def _compute_weather_factor(state: ContextState) -> float:
    wind = state.weather_wind_kmh
    precip = state.weather_precip_mm
    f = 1.0 + min(0.5, precip / 4.0) + min(0.25, wind / 45.0)
    return round(f, 3)


def _rush_hour_factor(now_ts: float | None = None) -> float:
    now_local = datetime.fromtimestamp(now_ts or time.time(), tz=NYC_TZ)
    hour = now_local.hour + (now_local.minute / 60.0)
    if 7.0 <= hour < 10.0:
        return 0.18
    if 16.0 <= hour < 20.0:
        return 0.22
    if 11.0 <= hour < 14.0:
        return 0.07
    return 0.0


def _compute_traffic_factor(state: ContextState, lat: float, lon: float, now_ts: float | None = None) -> float:
    local = _count_within(state.nyc311_incidents, lat, lon, TRAFFIC_RADIUS_KM)
    precip = state.weather_precip_mm
    f = 1.0 + min(0.45, local / 12.0) + min(0.2, precip / 6.0) + _rush_hour_factor(now_ts)
    return round(min(2.2, f), 3)


def _supply_index_from_member_count(member_count: int) -> float:
    """Map nearby couriers to supply index with low-supply visibility."""
    count = max(0, int(member_count))
    return round(max(0.2, min(5.0, count / 4.0)), 3)


async def _zone_supply_index(redis_client: aioredis.Redis, lat: float, lon: float) -> float:
    try:
        members = await redis_client.geosearch(
            COURIER_GEO_KEY,
            longitude=lon,
            latitude=lat,
            unit="km",
            radius=SUPPLY_RADIUS_KM,
        )
        return _supply_index_from_member_count(len(members))
    except Exception:
        return 1.0


async def _publish_quality_snapshot(
    redis_client: aioredis.Redis,
    *,
    supply_values: list[float],
    traffic_values: list[float],
    event_pressures: list[float],
    supply_mean_history: deque[float],
    sent_cycles: int,
    source_tag: str,
    events_source_active: bool,
    events_rows: int,
    events_status: str,
) -> None:
    if not supply_values:
        return

    supply_mean = sum(supply_values) / len(supply_values)
    supply_var = sum((value - supply_mean) ** 2 for value in supply_values) / len(supply_values)
    supply_std = math.sqrt(supply_var)
    supply_mean_history.append(round(supply_mean, 4))

    span = (max(supply_mean_history) - min(supply_mean_history)) if supply_mean_history else 0.0
    flat_alert = (
        len(supply_mean_history) >= max(2, CONTEXT_SUPPLY_FLAT_MIN_CYCLES)
        and span <= CONTEXT_SUPPLY_VARIANCE_EPSILON
    )
    traffic_nonzero_rate = (
        sum(1 for value in traffic_values if value > 1.01) / len(traffic_values) if traffic_values else 0.0
    )

    payload = {
        "updated_at": utc_now_iso(),
        "cycles_published": str(sent_cycles),
        "sources": source_tag,
        "events_source_active": "1" if events_source_active else "0",
        "events_rows": str(max(0, int(events_rows))),
        "events_status": events_status,
        "supply_key": COURIER_GEO_KEY,
        "zones_count": str(len(supply_values)),
        "supply_min": f"{min(supply_values):.3f}",
        "supply_max": f"{max(supply_values):.3f}",
        "supply_mean": f"{supply_mean:.3f}",
        "supply_std": f"{supply_std:.3f}",
        "supply_variance": f"{supply_var:.5f}",
        "supply_window_span": f"{span:.5f}",
        "supply_window_cycles": str(len(supply_mean_history)),
        "supply_flat_alert": "1" if flat_alert else "0",
        "supply_flat_threshold": f"{CONTEXT_SUPPLY_VARIANCE_EPSILON:.5f}",
        "traffic_nonzero_rate": f"{traffic_nonzero_rate:.3f}",
        "traffic_mean": f"{(sum(traffic_values) / len(traffic_values)):.3f}" if traffic_values else "1.000",
        "event_pressure_mean": (
            f"{(sum(event_pressures) / len(event_pressures)):.4f}" if event_pressures else "0.0000"
        ),
        "event_pressure_max": f"{(max(event_pressures) if event_pressures else 0.0):.4f}",
        "event_pressure_nonzero_rate": (
            f"{(sum(1 for value in event_pressures if value > 0.001) / len(event_pressures)):.4f}"
            if event_pressures
            else "0.0000"
        ),
    }
    await redis_client.hset(CONTEXT_QUALITY_KEY, mapping=payload)
    await redis_client.expire(CONTEXT_QUALITY_KEY, CONTEXT_QUALITY_TTL_SECONDS)

    if flat_alert:
        log.warning(
            "context quality alert: supply_index appears flat across %d cycles "
            "(span=%.5f <= %.5f, key=%s)",
            len(supply_mean_history),
            span,
            CONTEXT_SUPPLY_VARIANCE_EPSILON,
            COURIER_GEO_KEY,
        )


async def publisher_loop(
    state: ContextState,
    centroids: dict[int, tuple[float, float]],
    producer: AIOKafkaProducer,
    redis_client: aioredis.Redis,
    stop: asyncio.Event,
) -> None:
    log.info("publisher starting (tick=%.1fs zones=%d)", CONTEXT_TICK_SECONDS, len(centroids))
    sent_cycles = 0
    supply_mean_history: deque[float] = deque(maxlen=max(2, CONTEXT_QUALITY_WINDOW_CYCLES))
    while not stop.is_set():
        tick_started = time.time()
        if not state.sources_active:
            log.info("waiting for at least one source to populate…")
            try:
                await asyncio.wait_for(stop.wait(), timeout=CONTEXT_TICK_SECONDS)
            except asyncio.TimeoutError:
                pass
            continue

        weather = _compute_weather_factor(state)
        sources_tag = "+".join(sorted(state.sources_active)) or "none"
        ts_iso = utc_now_iso()
        sends = []
        supply_values: list[float] = []
        traffic_values: list[float] = []
        event_pressures: list[float] = []

        for loc_id, (lat, lon) in centroids.items():
            zone_id = f"nyc_{loc_id}"
            event_pressure = _compute_event_pressure(
                state.events,
                lat=lat,
                lon=lon,
                now_ts=tick_started,
            )
            demand = _compute_zone_demand(state, lat, lon, event_pressure=event_pressure)
            traffic = _compute_traffic_factor(state, lat, lon, now_ts=tick_started)
            supply = await _zone_supply_index(redis_client, lat, lon)
            supply_values.append(supply)
            traffic_values.append(traffic)
            event_pressures.append(event_pressure)
            source_value = (
                f"{sources_tag};event_pressure={event_pressure:.4f}"
                if "nyc-events" in state.sources_active
                else sources_tag
            )
            sig = ContextSignalV1(
                event_id=event_id("ctx", f"{zone_id}_{int(tick_started)}"),
                event_type="context.signal.v1",
                ts=ts_iso,
                zone_id=zone_id,
                demand_index=demand,
                supply_index=supply,
                weather_factor=weather,
                traffic_factor=traffic,
                source=source_value,
            )
            sends.append(producer.send(CONTEXT_SIGNALS_TOPIC, key=zone_id.encode(), value=sig.SerializeToString()))

        try:
            await asyncio.gather(*sends)
        except Exception as exc:
            log.error("publish batch failed: %s", exc)
        else:
            sent_cycles += 1
            elapsed = time.time() - tick_started
            await _publish_quality_snapshot(
                redis_client,
                supply_values=supply_values,
                traffic_values=traffic_values,
                event_pressures=event_pressures,
                supply_mean_history=supply_mean_history,
                sent_cycles=sent_cycles,
                source_tag=sources_tag,
                events_source_active=("nyc-events" in state.sources_active),
                events_rows=state.events_rows,
                events_status=state.events_source_status,
            )
            if sent_cycles % 5 == 1:
                log.info(
                    "published cycle=%d zones=%d sources=%s weather=%.2f supply_mean=%.2f "
                    "traffic_nonzero=%.2f event_pressure_mean=%.4f took=%.2fs",
                    sent_cycles,
                    len(centroids),
                    sources_tag,
                    weather,
                    (sum(supply_values) / len(supply_values)) if supply_values else 0.0,
                    (
                        sum(1 for value in traffic_values if value > 1.01) / len(traffic_values)
                        if traffic_values
                        else 0.0
                    ),
                    (sum(event_pressures) / len(event_pressures)) if event_pressures else 0.0,
                    elapsed,
                )

        try:
            await asyncio.wait_for(stop.wait(), timeout=CONTEXT_TICK_SECONDS)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    stop = asyncio.Event()

    def _on_signal(*_: object) -> None:
        log.info("signal received, stopping context-poller")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    centroids = load_centroids()
    log.info("loaded %d NYC zone centroids", len(centroids))

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        compression_type="lz4",
        acks=1,
        linger_ms=10,
        max_batch_size=262_144,
    )

    for attempt in range(1, 11):
        try:
            await producer.start()
            log.info("kafka ready bootstrap=%s", KAFKA_BOOTSTRAP)
            break
        except Exception as exc:
            log.warning("kafka start attempt %d/10 failed: %s", attempt, exc)
            await asyncio.sleep(3)
    else:
        log.error("unable to start kafka producer")
        return

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    state = ContextState()

    tasks = [
        asyncio.create_task(poll_gbfs(state, stop), name="gbfs"),
        asyncio.create_task(poll_weather(state, stop), name="weather"),
        asyncio.create_task(poll_nyc311(state, stop), name="nyc311"),
        asyncio.create_task(poll_nyc_events(state, redis_client, stop), name="nyc-events"),
        asyncio.create_task(publisher_loop(state, centroids, producer, redis_client, stop), name="publisher"),
    ]

    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await producer.stop()
        await redis_client.aclose()
        log.info("context-poller stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
