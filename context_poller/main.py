"""
context_poller — streams real NYC market-context signals to Kafka.

Periodically hits public APIs (Citi Bike GBFS, Open-Meteo, NYC 311 Socrata)
and publishes one ContextSignalV1 per NYC taxi zone every CONTEXT_TICK seconds
on the context-signals-v1 topic.

Signal fields populated from real data:
- demand_index   : 1.0 + f(Citi Bike station scarcity near the zone centroid)
- supply_index   : live courier count (GEOSEARCH around the zone centroid) / scale
- weather_factor : 1.0 + f(precipitation) + f(wind)      — Open-Meteo NYC
- traffic_factor : 1.0 + f(precipitation) + f(311 traffic complaints near zone)
- source         : short tag describing which APIs fed this cycle
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import signal
import time
from pathlib import Path

import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

from copilot_events_pb2 import ContextSignalV1

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("context-poller")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
CONTEXT_SIGNALS_TOPIC = os.getenv("CONTEXT_SIGNALS_TOPIC", "context-signals-v1")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
COURIER_GEO_KEY = os.getenv("COURIER_GEO_KEY", "fleet:livreurs")

CONTEXT_TICK_SECONDS = float(os.getenv("CONTEXT_TICK_SECONDS", "30"))
GBFS_POLL_SECONDS = float(os.getenv("GBFS_POLL_SECONDS", "60"))
WEATHER_POLL_SECONDS = float(os.getenv("WEATHER_POLL_SECONDS", "600"))
NYC311_POLL_SECONDS = float(os.getenv("NYC311_POLL_SECONDS", "300"))

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

HTTP_USER_AGENT = os.getenv("HTTP_USER_AGENT", "FleetStream/1.0 (context-poller; contact=ops@fleetstream.local)")

CENTROIDS_PATH = Path(__file__).parent / "nyc_zone_centroids.json"

DEMAND_RADIUS_KM = float(os.getenv("DEMAND_RADIUS_KM", "0.8"))
SUPPLY_RADIUS_KM = float(os.getenv("SUPPLY_RADIUS_KM", "1.0"))
TRAFFIC_RADIUS_KM = float(os.getenv("TRAFFIC_RADIUS_KM", "1.5"))

EARTH_KM = 6371.0


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


def _compute_zone_demand(state: ContextState, lat: float, lon: float) -> float:
    """Demand proxy: how many Citi Bike stations near the centroid are empty/low.

    More empty stations -> more unmet micro-mobility demand -> taxi demand bump.
    """
    if not state.gbfs_stations:
        return 1.0
    stations_nearby = [
        (s["lat"], s["lon"], s["bikes"])
        for s in state.gbfs_stations
        if abs(s["lat"] - lat) < 0.02 and abs(s["lon"] - lon) < 0.025
    ]
    if not stations_nearby:
        return 1.0
    low = 0
    for slat, slon, bikes in stations_nearby:
        if haversine_km(lat, lon, slat, slon) <= DEMAND_RADIUS_KM and bikes < 3:
            low += 1
    return round(1.0 + min(1.5, low / 5.0), 3)


def _compute_weather_factor(state: ContextState) -> float:
    wind = state.weather_wind_kmh
    precip = state.weather_precip_mm
    f = 1.0 + min(0.5, precip / 4.0) + min(0.25, wind / 45.0)
    return round(f, 3)


def _compute_traffic_factor(state: ContextState, lat: float, lon: float) -> float:
    local = _count_within(state.nyc311_incidents, lat, lon, TRAFFIC_RADIUS_KM)
    precip = state.weather_precip_mm
    f = 1.0 + min(0.4, local / 15.0) + min(0.2, precip / 6.0)
    return round(f, 3)


async def _zone_supply_index(redis_client: aioredis.Redis, lat: float, lon: float) -> float:
    try:
        members = await redis_client.geosearch(
            COURIER_GEO_KEY,
            longitude=lon,
            latitude=lat,
            unit="km",
            radius=SUPPLY_RADIUS_KM,
        )
        return round(max(1.0, len(members) / 4.0), 3)
    except Exception:
        return 1.0


async def publisher_loop(
    state: ContextState,
    centroids: dict[int, tuple[float, float]],
    producer: AIOKafkaProducer,
    redis_client: aioredis.Redis,
    stop: asyncio.Event,
) -> None:
    log.info("publisher starting (tick=%.1fs zones=%d)", CONTEXT_TICK_SECONDS, len(centroids))
    sent_cycles = 0
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

        for loc_id, (lat, lon) in centroids.items():
            zone_id = f"nyc_{loc_id}"
            demand = _compute_zone_demand(state, lat, lon)
            traffic = _compute_traffic_factor(state, lat, lon)
            supply = await _zone_supply_index(redis_client, lat, lon)
            sig = ContextSignalV1(
                event_id=event_id("ctx", f"{zone_id}_{int(tick_started)}"),
                event_type="context.signal.v1",
                ts=ts_iso,
                zone_id=zone_id,
                demand_index=demand,
                supply_index=supply,
                weather_factor=weather,
                traffic_factor=traffic,
                source=sources_tag,
            )
            sends.append(producer.send(CONTEXT_SIGNALS_TOPIC, key=zone_id.encode(), value=sig.SerializeToString()))

        try:
            await asyncio.gather(*sends)
        except Exception as exc:
            log.error("publish batch failed: %s", exc)
        else:
            sent_cycles += 1
            elapsed = time.time() - tick_started
            if sent_cycles % 5 == 1:
                log.info(
                    "published cycle=%d zones=%d sources=%s weather=%.2f took=%.2fs",
                    sent_cycles,
                    len(centroids),
                    sources_tag,
                    weather,
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
