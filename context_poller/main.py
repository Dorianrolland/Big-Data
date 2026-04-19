"""
context_poller — streams real NYC market-context signals to Kafka.

Periodically hits public APIs (Citi Bike GBFS, Open-Meteo, NYC 311 Socrata,
NYC DOT weekly traffic advisory, NYC DOT traffic speeds feed,
NYC Permitted Event Information)
and publishes one ContextSignalV1 per NYC taxi zone every CONTEXT_TICK seconds
on the context-signals-v1 topic.

Signal fields populated from real data:
- demand_index   : 1.0 + f(Citi Bike station scarcity) + event_pressure (capped)
- supply_index   : live courier count (GEOSEARCH around the zone centroid) / scale
- weather_factor : 1.0 + f(precipitation) + f(wind)      — Open-Meteo NYC
- traffic_factor : 1.0 + f(precipitation) + f(311) + closure_pressure + speed_pressure
- source         : short tag describing which APIs fed this cycle
"""
from __future__ import annotations

import asyncio
from collections import deque
from datetime import date, datetime, timedelta
import html
import hashlib
import json
import logging
import math
import os
import re
import signal
import time
from functools import lru_cache
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
DOT_ADVISORY_POLL_SECONDS = _env_float("DOT_ADVISORY_POLL_SECONDS", 1800.0, min_value=300.0)
DOT_SPEEDS_POLL_SECONDS = _env_float("DOT_SPEEDS_POLL_SECONDS", 300.0, min_value=60.0)
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
NYC_DOT_WEEKLY_ADVISORY_URL = os.getenv(
    "NYC_DOT_WEEKLY_ADVISORY_URL",
    "https://www.nyc.gov/html/dot/html/motorist/weektraf.shtml",
)
NYC_DOT_SPEEDS_URL = os.getenv(
    "NYC_DOT_SPEEDS_URL",
    "https://linkdata.nyctmc.org/data/LinkSpeedQuery.txt",
)

HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "FleetStream/1.0 (context-poller; contact=ops@fleetstream.local)")

CENTROIDS_PATH = Path(__file__).parent / "nyc_zone_centroids.json"

DEMAND_RADIUS_KM = _env_float("DEMAND_RADIUS_KM", 0.8, min_value=0.1)
SUPPLY_RADIUS_KM = _env_float("SUPPLY_RADIUS_KM", 1.0, min_value=0.1)
TRAFFIC_RADIUS_KM = _env_float("TRAFFIC_RADIUS_KM", 1.5, min_value=0.1)
TRAFFIC_CLOSURE_RADIUS_KM = _env_float("TRAFFIC_CLOSURE_RADIUS_KM", 4.5, min_value=0.2)
TRAFFIC_SPEED_RADIUS_KM = _env_float("TRAFFIC_SPEED_RADIUS_KM", 3.5, min_value=0.2)
TRAFFIC_CLOSURE_LOOKAHEAD_HOURS = _env_float("TRAFFIC_CLOSURE_LOOKAHEAD_HOURS", 72.0, min_value=1.0)
TRAFFIC_CLOSURE_POST_WINDOW_MINUTES = _env_float("TRAFFIC_CLOSURE_POST_WINDOW_MINUTES", 240.0, min_value=0.0)
TRAFFIC_CLOSURE_PRESSURE_SCALE = _env_float("TRAFFIC_CLOSURE_PRESSURE_SCALE", 0.32, min_value=0.0)
TRAFFIC_CLOSURE_PRESSURE_CAP = _env_float("TRAFFIC_CLOSURE_PRESSURE_CAP", 0.55, min_value=0.0)
TRAFFIC_SPEED_BASELINE_KMH = _env_float("TRAFFIC_SPEED_BASELINE_KMH", 38.0, min_value=5.0)
TRAFFIC_SPEED_PRESSURE_SCALE = _env_float("TRAFFIC_SPEED_PRESSURE_SCALE", 0.55, min_value=0.0)
TRAFFIC_SPEED_PRESSURE_CAP = _env_float("TRAFFIC_SPEED_PRESSURE_CAP", 0.45, min_value=0.0)
TRAFFIC_FACTOR_CAP = _env_float("TRAFFIC_FACTOR_CAP", 2.4, min_value=1.0)
TRAFFIC_SPEED_INPUT_UNIT = str(os.getenv("TRAFFIC_SPEED_INPUT_UNIT", "mph")).strip().lower()
EVENT_RADIUS_KM = _env_float("EVENT_RADIUS_KM", 4.5, min_value=0.2)
EVENT_LOOKAHEAD_HOURS = _env_float("EVENT_LOOKAHEAD_HOURS", 6.0, min_value=0.25)
EVENT_POST_WINDOW_MINUTES = _env_float("EVENT_POST_WINDOW_MINUTES", 60.0, min_value=0.0)
EVENT_PRESSURE_SCALE = _env_float("EVENT_PRESSURE_SCALE", 0.55, min_value=0.0)
EVENT_PRESSURE_CAP = _env_float("EVENT_PRESSURE_CAP", 0.75, min_value=0.0)
DEMAND_INDEX_CAP = _env_float("DEMAND_INDEX_CAP", 3.2, min_value=1.0)
CONTEXT_FRESHNESS_POLICY = str(os.getenv("CONTEXT_FRESHNESS_POLICY", "stale_neutral_v1")).strip() or "stale_neutral_v1"
CONTEXT_FRESH_GBFS_MAX_AGE_S = _env_float(
    "CONTEXT_FRESH_GBFS_MAX_AGE_S",
    max(180.0, GBFS_POLL_SECONDS * 3.0),
    min_value=30.0,
)
CONTEXT_FRESH_WEATHER_MAX_AGE_S = _env_float(
    "CONTEXT_FRESH_WEATHER_MAX_AGE_S",
    max(300.0, WEATHER_POLL_SECONDS * 2.5),
    min_value=60.0,
)
CONTEXT_FRESH_NYC311_MAX_AGE_S = _env_float(
    "CONTEXT_FRESH_NYC311_MAX_AGE_S",
    max(240.0, NYC311_POLL_SECONDS * 2.5),
    min_value=60.0,
)
CONTEXT_FRESH_EVENTS_MAX_AGE_S = _env_float(
    "CONTEXT_FRESH_EVENTS_MAX_AGE_S",
    max(420.0, EVENTS_POLL_SECONDS * 2.5),
    min_value=120.0,
)
CONTEXT_FRESH_DOT_ADVISORY_MAX_AGE_S = _env_float(
    "CONTEXT_FRESH_DOT_ADVISORY_MAX_AGE_S",
    max(900.0, DOT_ADVISORY_POLL_SECONDS * 2.0),
    min_value=300.0,
)
CONTEXT_FRESH_DOT_SPEEDS_MAX_AGE_S = _env_float(
    "CONTEXT_FRESH_DOT_SPEEDS_MAX_AGE_S",
    max(420.0, DOT_SPEEDS_POLL_SECONDS * 2.5),
    min_value=120.0,
)
TEMPORAL_PEAK_PRESSURE_BOOST = _env_float("TEMPORAL_PEAK_PRESSURE_BOOST", 0.14, min_value=0.0)
TEMPORAL_MIDDAY_PRESSURE_BOOST = _env_float("TEMPORAL_MIDDAY_PRESSURE_BOOST", 0.05, min_value=0.0)
TEMPORAL_WEEKEND_PRESSURE_BOOST = _env_float("TEMPORAL_WEEKEND_PRESSURE_BOOST", 0.08, min_value=0.0)
TEMPORAL_HOLIDAY_PRESSURE_BOOST = _env_float("TEMPORAL_HOLIDAY_PRESSURE_BOOST", 0.12, min_value=0.0)
TEMPORAL_PRESSURE_CAP = _env_float("TEMPORAL_PRESSURE_CAP", 0.32, min_value=0.0)

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


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


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


def _infer_borough_from_text(value: object) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    normalized = text.replace("/", " ").replace("-", " ").replace("_", " ")
    for token, borough in (
        ("staten island", "staten_island"),
        ("manhattan", "manhattan"),
        ("new york", "manhattan"),
        ("brooklyn", "brooklyn"),
        ("queens", "queens"),
        ("bronx", "bronx"),
    ):
        if token in normalized:
            return borough
    return None


def _value_from(row: dict[str, object], keys: tuple[str, ...]) -> object | None:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _is_nyc_lat_lon(lat: float, lon: float) -> bool:
    return 40.3 <= lat <= 41.2 and -74.6 <= lon <= -73.2


def _coerce_lat_lon(pair_a: float, pair_b: float) -> tuple[float, float] | None:
    candidates = ((pair_a, pair_b), (pair_b, pair_a))
    for lat, lon in candidates:
        if _is_nyc_lat_lon(lat, lon):
            return lat, lon
    if abs(pair_a) <= 90.0 and abs(pair_b) <= 180.0:
        return pair_a, pair_b
    if abs(pair_b) <= 90.0 and abs(pair_a) <= 180.0:
        return pair_b, pair_a
    return None


def _extract_lat_lon_from_row(row: dict[str, object]) -> tuple[float, float] | None:
    lat_raw = _value_from(row, ("latitude", "lat", "point_latitude", "from_latitude", "start_latitude"))
    lon_raw = _value_from(row, ("longitude", "lon", "lng", "point_longitude", "from_longitude", "start_longitude"))
    lat = _coerce_float(lat_raw)
    lon = _coerce_float(lon_raw)
    if lat is not None and lon is not None:
        out = _coerce_lat_lon(lat, lon)
        if out is not None:
            return out

    # Socrata location object: {"latitude": "...", "longitude": "..."}
    for loc_key in ("location", "the_geom"):
        loc_val = row.get(loc_key)
        if isinstance(loc_val, dict):
            lat2 = _coerce_float(loc_val.get("latitude"))
            lon2 = _coerce_float(loc_val.get("longitude"))
            if lat2 is not None and lon2 is not None:
                out = _coerce_lat_lon(lat2, lon2)
                if out is not None:
                    return out
            coords = loc_val.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                a = _coerce_float(coords[0])
                b = _coerce_float(coords[1])
                if a is not None and b is not None:
                    out = _coerce_lat_lon(a, b)
                    if out is not None:
                        return out
    return None


def _parse_weekly_advisory_window(html_text: str, *, now_ts: float) -> tuple[float, float]:
    default_start = now_ts - (2.0 * 3600.0)
    default_end = now_ts + (7.0 * 24.0 * 3600.0)
    if not html_text:
        return default_start, default_end

    month_map = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    pattern = re.compile(
        r"Weekly Traffic Advisory for [A-Za-z]+\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4}),\s+to\s+[A-Za-z]+\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})",
        flags=re.IGNORECASE,
    )
    match = pattern.search(html_text)
    if not match:
        return default_start, default_end

    start_month = month_map.get(match.group(1).strip().lower())
    start_day = int(match.group(2))
    start_year = int(match.group(3))
    end_month = month_map.get(match.group(4).strip().lower())
    end_day = int(match.group(5))
    end_year = int(match.group(6))
    if start_month is None or end_month is None:
        return default_start, default_end

    try:
        start_dt = datetime(start_year, start_month, start_day, 0, 0, tzinfo=NYC_TZ)
        end_dt = datetime(end_year, end_month, end_day, 23, 59, tzinfo=NYC_TZ)
    except ValueError:
        return default_start, default_end
    return start_dt.astimezone(UTC_TZ).timestamp(), end_dt.astimezone(UTC_TZ).timestamp()


def _strip_html_to_lines(raw_html: str) -> list[str]:
    if not raw_html:
        return []
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", raw_html)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(?:p|div|li|h1|h2|h3|h4|tr|td|th)>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    lines = []
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            lines.append(line)
    return lines


def _closure_severity(text: str) -> float:
    body = text.lower()
    severity = 0.25
    if "full" in body and "close" in body:
        severity += 0.65
    if "double-lane" in body or "two out of" in body or "2 out of" in body:
        severity += 0.32
    elif "single-lane" in body or "single lane" in body:
        severity += 0.2
    if "bridge" in body or "tunnel" in body:
        severity += 0.1
    if "rolling closure" in body:
        severity += 0.12
    return max(0.1, min(1.35, severity))


def _extract_closure_records_from_weekly_advisory(raw_html: str, *, now_ts: float) -> list[dict[str, object]]:
    lines = _strip_html_to_lines(raw_html)
    if not lines:
        return []

    start_ts, end_ts = _parse_weekly_advisory_window(raw_html, now_ts=now_ts)
    closure_keywords = (" close", "closure", "closed", "lane", "detour", "bridge", "tunnel", "parkway")
    ignored_prefixes = ("weekly traffic advisory", "note:", "location:", "route:", "formation:", "dispersal:")
    records: list[dict[str, object]] = []
    active_borough: str | None = None
    seen: set[str] = set()

    for line in lines:
        lower = line.lower()
        borough_guess = _normalize_borough(line) or _infer_borough_from_text(line)
        if borough_guess and len(lower) <= 32 and not any(token in lower for token in closure_keywords):
            active_borough = borough_guess
            continue

        if any(lower.startswith(prefix) for prefix in ignored_prefixes):
            continue
        if len(line) < 24:
            continue
        if not any(token in lower for token in closure_keywords):
            continue

        merged_borough = active_borough or _normalize_borough(line) or _infer_borough_from_text(line)
        coords = BOROUGH_CENTROIDS.get(merged_borough) if merged_borough else None
        if coords is None:
            continue

        dedupe_key = f"{merged_borough}|{line}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        records.append(
            {
                "lat": coords[0],
                "lon": coords[1],
                "start_ts": start_ts,
                "end_ts": end_ts,
                "severity": _closure_severity(line),
            }
        )
    return records


def _speed_severity(speed_kmh: float, *, baseline_kmh: float = TRAFFIC_SPEED_BASELINE_KMH) -> float:
    baseline = max(5.0, baseline_kmh)
    speed = max(0.0, speed_kmh)
    if speed >= baseline:
        return 0.0
    deficit = (baseline - speed) / baseline
    if speed < 12.0:
        deficit += 0.2
    return max(0.0, min(1.25, deficit))


def _normalize_speed_to_kmh(
    speed: float,
    *,
    speed_key: str = "",
    default_unit: str = TRAFFIC_SPEED_INPUT_UNIT,
) -> float:
    key = str(speed_key or "").lower()
    unit = str(default_unit or "mph").strip().lower()
    if "kmh" in key or "kph" in key:
        return float(speed)
    if "mph" in key:
        return float(speed) * 1.60934
    if unit in {"kmh", "kph"}:
        return float(speed)
    if unit == "mph":
        return float(speed) * 1.60934
    # auto fallback for unknown configs
    return float(speed) * 1.60934 if float(speed) <= 30.0 else float(speed)


def _extract_midpoint_from_link_points(raw: object) -> tuple[float, float] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    matches = re.findall(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)", text)
    if not matches:
        return None
    coords: list[tuple[float, float]] = []
    for first, second in matches:
        a = _coerce_float(first)
        b = _coerce_float(second)
        if a is None or b is None:
            continue
        out = _coerce_lat_lon(a, b)
        if out is not None:
            coords.append(out)
    if not coords:
        return None
    lat = sum(item[0] for item in coords) / len(coords)
    lon = sum(item[1] for item in coords) / len(coords)
    return round(lat, 6), round(lon, 6)


def _build_speed_sample_from_row(row: dict[str, object]) -> dict[str, object] | None:
    if not isinstance(row, dict):
        return None

    coords = _extract_lat_lon_from_row(row)
    if coords is None:
        coords = _extract_midpoint_from_link_points(_value_from(row, ("link_points", "segment", "geometry")))
    if coords is None:
        coords = _event_borough_centroid(
            _value_from(row, ("borough", "event_borough", "boro", "borough_name", "bname"))
        )
    if coords is None:
        return None

    speed_raw = _value_from(row, ("speed", "speed_kmh", "avg_speed", "speed_average", "travel_speed"))
    speed = _coerce_float(speed_raw)
    if speed is None:
        return None
    speed_key = ""
    for candidate in ("speed", "speed_kmh", "avg_speed", "speed_average", "travel_speed"):
        if candidate in row and row.get(candidate) not in (None, ""):
            speed_key = candidate
            break

    speed_kmh = _normalize_speed_to_kmh(float(speed), speed_key=speed_key)

    if speed_kmh <= 0.1:
        return None

    return {
        "lat": coords[0],
        "lon": coords[1],
        "speed_kmh": round(speed_kmh, 3),
        "severity": _speed_severity(speed_kmh),
    }


def _sample_from_text_parts(parts: list[str]) -> dict[str, object] | None:
    if len(parts) < 3:
        return None

    # Fast path: first 3 columns are lat, lon, speed (or lon, lat, speed)
    first = _coerce_float(parts[0])
    second = _coerce_float(parts[1])
    speed0 = _coerce_float(parts[2])
    if first is not None and second is not None and speed0 is not None:
        lat_lon = _coerce_lat_lon(first, second)
        if lat_lon is not None and speed0 > 0.0:
            speed_kmh = _normalize_speed_to_kmh(speed0)
            return {
                "lat": lat_lon[0],
                "lon": lat_lon[1],
                "speed_kmh": round(max(0.0, speed_kmh), 3),
                "severity": _speed_severity(max(0.0, speed_kmh)),
            }

    # Robust path: locate first plausible coordinate pair, then next numeric as speed.
    numeric: list[tuple[int, float]] = []
    for idx, token in enumerate(parts):
        value = _coerce_float(token)
        if value is not None:
            numeric.append((idx, value))
    if len(numeric) < 3:
        return None

    for i in range(len(numeric) - 1):
        idx_a, val_a = numeric[i]
        idx_b, val_b = numeric[i + 1]
        lat_lon = _coerce_lat_lon(val_a, val_b)
        if lat_lon is None:
            continue
        speed_val: float | None = None
        for idx_c, val_c in numeric:
            if idx_c > idx_b and val_c > 0.0:
                speed_val = val_c
                break
        if speed_val is None:
            continue
        speed_kmh = _normalize_speed_to_kmh(speed_val)
        return {
            "lat": lat_lon[0],
            "lon": lat_lon[1],
            "speed_kmh": round(max(0.0, speed_kmh), 3),
            "severity": _speed_severity(max(0.0, speed_kmh)),
        }
    return None


def _extract_speed_samples_from_text(raw_text: str) -> list[dict[str, object]]:
    if not raw_text:
        return []
    rows: list[dict[str, object]] = []
    for line in raw_text.splitlines():
        if "," not in line:
            continue
        parts = [part.strip() for part in line.split(",")]
        sample = _sample_from_text_parts(parts)
        if sample:
            rows.append(sample)
    return rows

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


def _weather_intensity_score(precip_mm: float, wind_kmh: float) -> float:
    precip_component = clamp(max(0.0, precip_mm) / 6.0, 0.0, 1.0)
    wind_component = clamp(max(0.0, wind_kmh) / 50.0, 0.0, 1.0)
    score = (precip_component * 0.65) + (wind_component * 0.35)
    return round(clamp(score, 0.0, 1.0), 4)


def _nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    day = 1 + delta + ((max(1, nth) - 1) * 7)
    return date(year, month, day)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    base = date(year, month, day)
    if base.weekday() == 5:
        return base - timedelta(days=1)
    if base.weekday() == 6:
        return base + timedelta(days=1)
    return base


@lru_cache(maxsize=16)
def _us_holidays_ny(year: int) -> set[date]:
    return {
        _observed_fixed_holiday(year, 1, 1),   # New Year's Day
        _nth_weekday_of_month(year, 1, 0, 3),  # MLK Day
        _nth_weekday_of_month(year, 2, 0, 3),  # Presidents Day
        _last_weekday_of_month(year, 5, 0),    # Memorial Day
        _observed_fixed_holiday(year, 6, 19),  # Juneteenth
        _observed_fixed_holiday(year, 7, 4),   # Independence Day
        _nth_weekday_of_month(year, 9, 0, 1),  # Labor Day
        _nth_weekday_of_month(year, 10, 0, 2), # Columbus Day
        _observed_fixed_holiday(year, 11, 11), # Veterans Day
        _nth_weekday_of_month(year, 11, 3, 4), # Thanksgiving
        _observed_fixed_holiday(year, 12, 25), # Christmas
    }


def _is_holiday_local(local_dt: datetime) -> bool:
    d = local_dt.date()
    # Include adjacent years to catch observed fixed-date holidays that
    # can land on Dec 31 / Jan 2 around year boundaries.
    years = (local_dt.year - 1, local_dt.year, local_dt.year + 1)
    return any(d in _us_holidays_ny(y) for y in years)


def _is_peak_hour(local_dt: datetime) -> bool:
    hour = local_dt.hour + (local_dt.minute / 60.0)
    return (7.0 <= hour < 10.0) or (16.0 <= hour < 20.0)


def _temporal_pressure(local_dt: datetime) -> float:
    hour = local_dt.hour + (local_dt.minute / 60.0)
    pressure = 0.0
    if _is_peak_hour(local_dt):
        pressure += TEMPORAL_PEAK_PRESSURE_BOOST
    elif 11.0 <= hour < 14.0:
        pressure += TEMPORAL_MIDDAY_PRESSURE_BOOST
    if local_dt.weekday() >= 5:
        pressure += TEMPORAL_WEEKEND_PRESSURE_BOOST
    if _is_holiday_local(local_dt):
        pressure += TEMPORAL_HOLIDAY_PRESSURE_BOOST
    return round(clamp(pressure, 0.0, TEMPORAL_PRESSURE_CAP), 4)


def _temporal_context(now_ts: float) -> dict[str, object]:
    local_dt = datetime.fromtimestamp(now_ts, tz=NYC_TZ)
    hour_local = local_dt.hour + (local_dt.minute / 60.0)
    is_peak = _is_peak_hour(local_dt)
    is_weekend = local_dt.weekday() >= 5
    is_holiday = _is_holiday_local(local_dt)
    return {
        "hour_local": round(hour_local, 3),
        "is_peak_hour": is_peak,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday,
        "temporal_pressure": _temporal_pressure(local_dt),
    }


def _source_age_s(last_updated: float, *, now_ts: float) -> float:
    if last_updated <= 0.0:
        return -1.0
    return max(0.0, now_ts - last_updated)


def _is_source_stale(last_updated: float, *, now_ts: float, max_age_s: float) -> bool:
    age_s = _source_age_s(last_updated, now_ts=now_ts)
    return age_s < 0.0 or age_s > max(1.0, max_age_s)


def _freshness_snapshot(state: "ContextState", *, now_ts: float) -> dict[str, object]:
    stale_gbfs = _is_source_stale(state.gbfs_last_updated, now_ts=now_ts, max_age_s=CONTEXT_FRESH_GBFS_MAX_AGE_S)
    stale_weather = _is_source_stale(
        state.weather_last_updated, now_ts=now_ts, max_age_s=CONTEXT_FRESH_WEATHER_MAX_AGE_S
    )
    stale_nyc311 = _is_source_stale(state.nyc311_last_updated, now_ts=now_ts, max_age_s=CONTEXT_FRESH_NYC311_MAX_AGE_S)
    stale_events = _is_source_stale(state.events_last_updated, now_ts=now_ts, max_age_s=CONTEXT_FRESH_EVENTS_MAX_AGE_S)
    stale_dot_closure = _is_source_stale(
        state.dot_closure_last_updated,
        now_ts=now_ts,
        max_age_s=CONTEXT_FRESH_DOT_ADVISORY_MAX_AGE_S,
    )
    stale_dot_speeds = _is_source_stale(
        state.dot_speeds_last_updated,
        now_ts=now_ts,
        max_age_s=CONTEXT_FRESH_DOT_SPEEDS_MAX_AGE_S,
    )
    stale_count = sum(
        1
        for flag in (
            stale_gbfs,
            stale_weather,
            stale_nyc311,
            stale_events,
            stale_dot_closure,
            stale_dot_speeds,
        )
        if flag
    )
    return {
        "policy": CONTEXT_FRESHNESS_POLICY,
        "ages": {
            "gbfs_s": _source_age_s(state.gbfs_last_updated, now_ts=now_ts),
            "weather_s": _source_age_s(state.weather_last_updated, now_ts=now_ts),
            "nyc311_s": _source_age_s(state.nyc311_last_updated, now_ts=now_ts),
            "events_s": _source_age_s(state.events_last_updated, now_ts=now_ts),
            "dot_closure_s": _source_age_s(state.dot_closure_last_updated, now_ts=now_ts),
            "dot_speeds_s": _source_age_s(state.dot_speeds_last_updated, now_ts=now_ts),
        },
        "stale": {
            "gbfs": stale_gbfs,
            "weather": stale_weather,
            "nyc311": stale_nyc311,
            "events": stale_events,
            "dot_closure": stale_dot_closure,
            "dot_speeds": stale_dot_speeds,
        },
        "stale_count": int(stale_count),
        "fallback_applied": stale_count > 0,
    }


def _format_metadata_value(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value).strip()


def _build_source_value(base_tag: str, metadata: dict[str, object]) -> str:
    parts = [str(base_tag or "").strip() or "none"]
    for key in sorted(metadata):
        value = metadata.get(key)
        if value is None:
            continue
        val = _format_metadata_value(value)
        if val == "":
            continue
        parts.append(f"{key}={val}")
    return ";".join(parts)


def _count_nearby_events(
    events: list[dict[str, object]],
    *,
    lat: float,
    lon: float,
    now_ts: float,
    radius_km: float = EVENT_RADIUS_KM,
) -> int:
    if not events:
        return 0
    effective_radius = max(0.2, radius_km)
    count = 0
    for evt in events:
        evt_lat = _coerce_float(evt.get("lat"))
        evt_lon = _coerce_float(evt.get("lon"))
        evt_start = _coerce_float(evt.get("start_ts"))
        evt_end = _coerce_float(evt.get("end_ts"))
        if evt_lat is None or evt_lon is None or evt_start is None or evt_end is None:
            continue
        if _event_time_weight(
            now_ts=now_ts,
            start_ts=evt_start,
            end_ts=evt_end,
            lookahead_hours=EVENT_LOOKAHEAD_HOURS,
            post_window_minutes=EVENT_POST_WINDOW_MINUTES,
        ) <= 0.0:
            continue
        if haversine_km(lat, lon, evt_lat, evt_lon) <= effective_radius:
            count += 1
    return count


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
        self.dot_closure_records: list[dict[str, object]] = []
        self.dot_closure_last_updated: float = 0.0
        self.dot_closure_status: str = "idle"
        self.dot_closure_rows: int = 0
        self.dot_speed_samples: list[dict[str, object]] = []
        self.dot_speeds_last_updated: float = 0.0
        self.dot_speeds_status: str = "idle"
        self.dot_speeds_rows: int = 0
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


async def poll_nyc_dot_weekly_advisory(state: ContextState, stop: asyncio.Event) -> None:
    async with httpx.AsyncClient(timeout=35, headers={"User-Agent": HTTP_USER_AGENT}) as client:
        while not stop.is_set():
            try:
                resp = await client.get(NYC_DOT_WEEKLY_ADVISORY_URL)
                resp.raise_for_status()
                html_text = str(resp.text or "")
                closures = _extract_closure_records_from_weekly_advisory(html_text, now_ts=time.time())

                state.dot_closure_records = closures
                state.dot_closure_rows = len(closures)
                state.dot_closure_last_updated = time.time()
                state.dot_closure_status = "ok"
                state.sources_active.add("nyc-dot-advisory")
                log.info(
                    "nyc dot weekly advisory refreshed: rows=%d source=%s",
                    len(closures),
                    NYC_DOT_WEEKLY_ADVISORY_URL,
                )
            except Exception as exc:
                state.dot_closure_records = []
                state.dot_closure_rows = 0
                state.dot_closure_status = "degraded"
                state.sources_active.discard("nyc-dot-advisory")
                log.warning("nyc dot weekly advisory poll failed: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=DOT_ADVISORY_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass


async def poll_nyc_dot_speeds(state: ContextState, stop: asyncio.Event) -> None:
    async with httpx.AsyncClient(timeout=35, headers={"User-Agent": HTTP_USER_AGENT}) as client:
        while not stop.is_set():
            try:
                resp = await client.get(NYC_DOT_SPEEDS_URL)
                resp.raise_for_status()
                raw_body = str(resp.text or "")
                samples: list[dict[str, object]] = []

                content_type = str(resp.headers.get("content-type") or "").lower()
                body_trimmed = raw_body.strip()
                if "application/json" in content_type or body_trimmed.startswith("[") or body_trimmed.startswith("{"):
                    parsed = resp.json()
                    rows = parsed if isinstance(parsed, list) else []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        sample = _build_speed_sample_from_row(row)
                        if sample:
                            samples.append(sample)
                else:
                    samples = _extract_speed_samples_from_text(raw_body)

                state.dot_speed_samples = samples
                state.dot_speeds_rows = len(samples)
                state.dot_speeds_last_updated = time.time()
                state.dot_speeds_status = "ok"
                state.sources_active.add("nyc-dot-speeds")
                log.info(
                    "nyc dot speeds refreshed: rows=%d source=%s",
                    len(samples),
                    NYC_DOT_SPEEDS_URL,
                )
            except Exception as exc:
                state.dot_speed_samples = []
                state.dot_speeds_rows = 0
                state.dot_speeds_status = "degraded"
                state.sources_active.discard("nyc-dot-speeds")
                log.warning("nyc dot speeds poll failed: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=DOT_SPEEDS_POLL_SECONDS)
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
    temporal_pressure: float = 0.0,
    stations_override: list[dict] | None = None,
) -> float:
    """Demand proxy: how many Citi Bike stations near the centroid are empty/low.

    More empty stations -> more unmet micro-mobility demand -> taxi demand bump.
    """
    stations = state.gbfs_stations if stations_override is None else stations_override
    contextual_boost = max(0.0, float(event_pressure)) + max(0.0, float(temporal_pressure))
    if not stations:
        return round(min(DEMAND_INDEX_CAP, 1.0 + contextual_boost), 3)
    stations_nearby = [
        (s["lat"], s["lon"], s["bikes"])
        for s in stations
        if abs(s["lat"] - lat) < 0.02 and abs(s["lon"] - lon) < 0.025
    ]
    if not stations_nearby:
        return round(min(DEMAND_INDEX_CAP, 1.0 + contextual_boost), 3)
    low = 0
    for slat, slon, bikes in stations_nearby:
        if haversine_km(lat, lon, slat, slon) <= DEMAND_RADIUS_KM and bikes < 3:
            low += 1
    base = 1.0 + min(1.5, low / 5.0)
    adjusted = min(DEMAND_INDEX_CAP, base + contextual_boost)
    return round(adjusted, 3)


def _compute_weather_factor(
    state: ContextState,
    *,
    precip_mm: float | None = None,
    wind_kmh: float | None = None,
) -> float:
    wind = max(0.0, float(wind_kmh) if wind_kmh is not None else state.weather_wind_kmh)
    precip = max(0.0, float(precip_mm) if precip_mm is not None else state.weather_precip_mm)
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


def _compute_closure_pressure(
    closures: list[dict[str, object]],
    *,
    lat: float,
    lon: float,
    now_ts: float,
    radius_km: float = TRAFFIC_CLOSURE_RADIUS_KM,
    lookahead_hours: float = TRAFFIC_CLOSURE_LOOKAHEAD_HOURS,
    post_window_minutes: float = TRAFFIC_CLOSURE_POST_WINDOW_MINUTES,
    scale: float = TRAFFIC_CLOSURE_PRESSURE_SCALE,
    cap: float = TRAFFIC_CLOSURE_PRESSURE_CAP,
) -> float:
    if not closures:
        return 0.0

    score = 0.0
    effective_radius = max(0.2, radius_km)
    for row in closures:
        row_lat = _coerce_float(row.get("lat"))
        row_lon = _coerce_float(row.get("lon"))
        row_start = _coerce_float(row.get("start_ts"))
        row_end = _coerce_float(row.get("end_ts"))
        severity = _coerce_float(row.get("severity")) or 0.3
        if row_lat is None or row_lon is None or row_start is None or row_end is None:
            continue

        time_w = _event_time_weight(
            now_ts=now_ts,
            start_ts=row_start,
            end_ts=row_end,
            lookahead_hours=lookahead_hours,
            post_window_minutes=post_window_minutes,
        )
        if time_w <= 0.0:
            continue
        distance_km = haversine_km(lat, lon, row_lat, row_lon)
        if distance_km > effective_radius:
            continue
        distance_w = max(0.0, 1.0 - (distance_km / effective_radius)) ** 2
        score += max(0.05, severity) * time_w * distance_w

    return round(max(0.0, min(float(cap), float(scale) * score)), 4)


def _compute_speed_pressure(
    speeds: list[dict[str, object]],
    *,
    lat: float,
    lon: float,
    radius_km: float = TRAFFIC_SPEED_RADIUS_KM,
    scale: float = TRAFFIC_SPEED_PRESSURE_SCALE,
    cap: float = TRAFFIC_SPEED_PRESSURE_CAP,
) -> float:
    if not speeds:
        return 0.0

    effective_radius = max(0.2, radius_km)
    weighted_score = 0.0
    weighted_total = 0.0
    for row in speeds:
        row_lat = _coerce_float(row.get("lat"))
        row_lon = _coerce_float(row.get("lon"))
        severity = _coerce_float(row.get("severity"))
        if row_lat is None or row_lon is None:
            continue
        if severity is None:
            speed_kmh = _coerce_float(row.get("speed_kmh"))
            severity = _speed_severity(speed_kmh) if speed_kmh is not None else None
        if severity is None or severity <= 0.0:
            continue

        distance_km = haversine_km(lat, lon, row_lat, row_lon)
        if distance_km > effective_radius:
            continue
        weight = max(0.0, 1.0 - (distance_km / effective_radius))
        weighted_score += severity * weight
        weighted_total += weight

    if weighted_total <= 0.0:
        return 0.0
    normalized = weighted_score / weighted_total
    return round(max(0.0, min(float(cap), float(scale) * normalized)), 4)


def _compute_traffic_factor(
    state: ContextState,
    lat: float,
    lon: float,
    now_ts: float | None = None,
    *,
    closure_pressure: float | None = None,
    speed_pressure: float | None = None,
    precip_mm: float | None = None,
    incidents: list[tuple[float, float]] | None = None,
) -> float:
    incident_points = state.nyc311_incidents if incidents is None else incidents
    local = _count_within(incident_points, lat, lon, TRAFFIC_RADIUS_KM)
    precip = max(0.0, float(precip_mm) if precip_mm is not None else state.weather_precip_mm)
    now_ref = float(now_ts or time.time())
    closure_component = (
        float(closure_pressure)
        if closure_pressure is not None
        else _compute_closure_pressure(
            state.dot_closure_records,
            lat=lat,
            lon=lon,
            now_ts=now_ref,
        )
    )
    speed_component = (
        float(speed_pressure)
        if speed_pressure is not None
        else _compute_speed_pressure(
            state.dot_speed_samples,
            lat=lat,
            lon=lon,
        )
    )
    f = (
        1.0
        + min(0.45, local / 12.0)
        + min(0.2, precip / 6.0)
        + _rush_hour_factor(now_ts)
        + max(0.0, closure_component)
        + max(0.0, speed_component)
    )
    return round(min(TRAFFIC_FACTOR_CAP, f), 3)


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
    closure_pressures: list[float],
    speed_pressures: list[float],
    supply_mean_history: deque[float],
    sent_cycles: int,
    source_tag: str,
    events_source_active: bool,
    events_rows: int,
    events_status: str,
    dot_advisory_status: str,
    dot_advisory_rows: int,
    dot_speeds_status: str,
    dot_speeds_rows: int,
    freshness_snapshot: dict[str, object] | None = None,
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

    freshness = freshness_snapshot or {}
    freshness_ages = freshness.get("ages", {}) if isinstance(freshness, dict) else {}
    freshness_stale = freshness.get("stale", {}) if isinstance(freshness, dict) else {}
    age_gbfs_s = _coerce_float(freshness_ages.get("gbfs_s"))
    age_weather_s = _coerce_float(freshness_ages.get("weather_s"))
    age_nyc311_s = _coerce_float(freshness_ages.get("nyc311_s"))
    age_events_s = _coerce_float(freshness_ages.get("events_s"))
    age_dot_closure_s = _coerce_float(freshness_ages.get("dot_closure_s"))
    age_dot_speeds_s = _coerce_float(freshness_ages.get("dot_speeds_s"))

    payload = {
        "updated_at": utc_now_iso(),
        "cycles_published": str(sent_cycles),
        "sources": source_tag,
        "events_source_active": "1" if events_source_active else "0",
        "events_rows": str(max(0, int(events_rows))),
        "events_status": events_status,
        "dot_advisory_status": dot_advisory_status,
        "dot_advisory_rows": str(max(0, int(dot_advisory_rows))),
        "dot_speeds_status": dot_speeds_status,
        "dot_speeds_rows": str(max(0, int(dot_speeds_rows))),
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
        "closure_pressure_mean": (
            f"{(sum(closure_pressures) / len(closure_pressures)):.4f}" if closure_pressures else "0.0000"
        ),
        "closure_pressure_max": f"{(max(closure_pressures) if closure_pressures else 0.0):.4f}",
        "speed_pressure_mean": (
            f"{(sum(speed_pressures) / len(speed_pressures)):.4f}" if speed_pressures else "0.0000"
        ),
        "speed_pressure_max": f"{(max(speed_pressures) if speed_pressures else 0.0):.4f}",
        "freshness_policy": str(freshness.get("policy") or CONTEXT_FRESHNESS_POLICY),
        "context_fallback_applied": "1" if bool(freshness.get("fallback_applied")) else "0",
        "stale_sources_count": str(int(freshness.get("stale_count") or 0)),
        "age_gbfs_s": f"{(age_gbfs_s if age_gbfs_s is not None else -1.0):.1f}",
        "age_weather_s": f"{(age_weather_s if age_weather_s is not None else -1.0):.1f}",
        "age_nyc311_s": f"{(age_nyc311_s if age_nyc311_s is not None else -1.0):.1f}",
        "age_events_s": f"{(age_events_s if age_events_s is not None else -1.0):.1f}",
        "age_dot_closure_s": f"{(age_dot_closure_s if age_dot_closure_s is not None else -1.0):.1f}",
        "age_dot_speeds_s": f"{(age_dot_speeds_s if age_dot_speeds_s is not None else -1.0):.1f}",
        "stale_gbfs": "1" if bool(freshness_stale.get("gbfs")) else "0",
        "stale_weather": "1" if bool(freshness_stale.get("weather")) else "0",
        "stale_nyc311": "1" if bool(freshness_stale.get("nyc311")) else "0",
        "stale_events": "1" if bool(freshness_stale.get("events")) else "0",
        "stale_dot_closure": "1" if bool(freshness_stale.get("dot_closure")) else "0",
        "stale_dot_speeds": "1" if bool(freshness_stale.get("dot_speeds")) else "0",
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

        freshness = _freshness_snapshot(state, now_ts=tick_started)
        freshness_ages = freshness.get("ages", {}) if isinstance(freshness, dict) else {}
        freshness_stale = freshness.get("stale", {}) if isinstance(freshness, dict) else {}
        age_gbfs_s = _coerce_float(freshness_ages.get("gbfs_s"))
        age_weather_s = _coerce_float(freshness_ages.get("weather_s"))
        age_nyc311_s = _coerce_float(freshness_ages.get("nyc311_s"))
        age_events_s = _coerce_float(freshness_ages.get("events_s"))
        age_dot_closure_s = _coerce_float(freshness_ages.get("dot_closure_s"))
        age_dot_speeds_s = _coerce_float(freshness_ages.get("dot_speeds_s"))

        effective_gbfs_stations = state.gbfs_stations if not bool(freshness_stale.get("gbfs")) else []
        effective_events = state.events if not bool(freshness_stale.get("events")) else []
        effective_closure_records = state.dot_closure_records if not bool(freshness_stale.get("dot_closure")) else []
        effective_speed_samples = state.dot_speed_samples if not bool(freshness_stale.get("dot_speeds")) else []
        effective_nyc311_incidents = state.nyc311_incidents if not bool(freshness_stale.get("nyc311")) else []
        effective_weather_precip = state.weather_precip_mm if not bool(freshness_stale.get("weather")) else 0.0
        effective_weather_wind = state.weather_wind_kmh if not bool(freshness_stale.get("weather")) else 0.0
        weather_intensity = _weather_intensity_score(effective_weather_precip, effective_weather_wind)
        temporal = _temporal_context(tick_started)
        temporal_pressure = float(temporal["temporal_pressure"])
        weather = _compute_weather_factor(
            state,
            precip_mm=effective_weather_precip,
            wind_kmh=effective_weather_wind,
        )
        sources_tag = "+".join(sorted(state.sources_active)) or "none"
        ts_iso = utc_now_iso()
        sends = []
        supply_values: list[float] = []
        traffic_values: list[float] = []
        event_pressures: list[float] = []
        closure_pressures: list[float] = []
        speed_pressures: list[float] = []

        for loc_id, (lat, lon) in centroids.items():
            zone_id = f"nyc_{loc_id}"
            event_pressure = _compute_event_pressure(
                effective_events,
                lat=lat,
                lon=lon,
                now_ts=tick_started,
            )
            event_count_nearby = _count_nearby_events(
                effective_events,
                lat=lat,
                lon=lon,
                now_ts=tick_started,
            )
            demand = _compute_zone_demand(
                state,
                lat,
                lon,
                event_pressure=event_pressure,
                temporal_pressure=temporal_pressure,
                stations_override=effective_gbfs_stations,
            )
            closure_pressure = _compute_closure_pressure(
                effective_closure_records,
                lat=lat,
                lon=lon,
                now_ts=tick_started,
            )
            speed_pressure = _compute_speed_pressure(
                effective_speed_samples,
                lat=lat,
                lon=lon,
            )
            traffic = _compute_traffic_factor(
                state,
                lat,
                lon,
                now_ts=tick_started,
                closure_pressure=closure_pressure,
                speed_pressure=speed_pressure,
                precip_mm=effective_weather_precip,
                incidents=effective_nyc311_incidents,
            )
            supply = await _zone_supply_index(redis_client, lat, lon)
            supply_values.append(supply)
            traffic_values.append(traffic)
            event_pressures.append(event_pressure)
            closure_pressures.append(closure_pressure)
            speed_pressures.append(speed_pressure)
            source_value = _build_source_value(
                sources_tag,
                {
                    "event_pressure": event_pressure,
                    "event_count_nearby": event_count_nearby,
                    "weather_precip_mm": effective_weather_precip,
                    "weather_wind_kmh": effective_weather_wind,
                    "weather_intensity": weather_intensity,
                    "temporal_hour_local": temporal["hour_local"],
                    "temporal_is_peak": temporal["is_peak_hour"],
                    "temporal_is_weekend": temporal["is_weekend"],
                    "temporal_is_holiday": temporal["is_holiday"],
                    "temporal_pressure": temporal_pressure,
                    "freshness_policy": freshness.get("policy"),
                    "freshness_fallback_applied": bool(freshness.get("fallback_applied")),
                    "freshness_stale_sources": int(freshness.get("stale_count") or 0),
                    "age_gbfs_s": (age_gbfs_s if age_gbfs_s is not None else -1.0),
                    "age_weather_s": (age_weather_s if age_weather_s is not None else -1.0),
                    "age_nyc311_s": (age_nyc311_s if age_nyc311_s is not None else -1.0),
                    "age_events_s": (age_events_s if age_events_s is not None else -1.0),
                    "age_dot_closure_s": (age_dot_closure_s if age_dot_closure_s is not None else -1.0),
                    "age_dot_speeds_s": (age_dot_speeds_s if age_dot_speeds_s is not None else -1.0),
                    "stale_gbfs": bool(freshness_stale.get("gbfs")),
                    "stale_weather": bool(freshness_stale.get("weather")),
                    "stale_nyc311": bool(freshness_stale.get("nyc311")),
                    "stale_events": bool(freshness_stale.get("events")),
                    "stale_dot_closure": bool(freshness_stale.get("dot_closure")),
                    "stale_dot_speeds": bool(freshness_stale.get("dot_speeds")),
                },
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
                closure_pressures=closure_pressures,
                speed_pressures=speed_pressures,
                supply_mean_history=supply_mean_history,
                sent_cycles=sent_cycles,
                source_tag=sources_tag,
                events_source_active=("nyc-events" in state.sources_active),
                events_rows=state.events_rows,
                events_status=state.events_source_status,
                dot_advisory_status=state.dot_closure_status,
                dot_advisory_rows=state.dot_closure_rows,
                dot_speeds_status=state.dot_speeds_status,
                dot_speeds_rows=state.dot_speeds_rows,
                freshness_snapshot=freshness,
            )
            if sent_cycles % 5 == 1:
                log.info(
                    "published cycle=%d zones=%d sources=%s weather=%.2f supply_mean=%.2f "
                    "traffic_nonzero=%.2f closure_pressure_mean=%.4f speed_pressure_mean=%.4f "
                    "event_pressure_mean=%.4f stale_sources=%d took=%.2fs",
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
                    (sum(closure_pressures) / len(closure_pressures)) if closure_pressures else 0.0,
                    (sum(speed_pressures) / len(speed_pressures)) if speed_pressures else 0.0,
                    (sum(event_pressures) / len(event_pressures)) if event_pressures else 0.0,
                    int(freshness.get("stale_count") or 0),
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
        asyncio.create_task(poll_nyc_dot_weekly_advisory(state, stop), name="nyc-dot-advisory"),
        asyncio.create_task(poll_nyc_dot_speeds(state, stop), name="nyc-dot-speeds"),
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
