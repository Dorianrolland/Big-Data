"""
Routing provider chain for the TLC replay service.

Purpose
-------
Replace the straight-line fallback used by the legacy replay with a real
road-network route so that a single driver's position always slides along
actual streets (no buildings traversed).

Design
------
- Provider chain (`ROUTING_PROVIDERS` env, comma-separated, ordered):
    * "public" -> HTTP call to a public routing API
      (openrouteservice by default, any "/v2/directions/driving-car"
      compatible endpoint that returns GeoJSON works).
    * "osrm" -> HTTP call to a local OSRM instance (`/route/v1/driving`)
    * "osrm_public" -> HTTP call to the public OSRM demo endpoint
      (`https://router.project-osrm.org`)
- Each provider either returns a `Route` with a decoded polyline (list of
  (lat, lon) pairs) and total distance / duration in km / min, or raises.
- `RoutingClient.route_segment()` tries providers in order and returns the
  first non-empty route. If every provider fails (or no provider is
  configured) it raises `RoutingUnavailableError`; the caller is expected
  to hold the driver at its last routed point (explicit degraded state).
- No straight-line fallback is ever produced here; a caller that wants the
  legacy behaviour must handle the exception itself.

This module has no hard Kafka/Redis dependency; it only needs httpx. It is
trivially testable (see `tests/test_single_driver.py`).
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
from dataclasses import dataclass
from typing import Iterable

import httpx

log = logging.getLogger("tlc-replay.routing")

EARTH_RADIUS_KM = 6371.0


class RoutingUnavailableError(RuntimeError):
    """Raised when every configured routing provider has failed."""


@dataclass
class Route:
    """A routed segment as returned by a provider.

    `geometry` is a list of `(lat, lon)` pairs, ordered from origin to
    destination. The first point is the start and the last point is the
    end. `distance_km` and `duration_min` are the totals reported by the
    provider (we use them for speed / ETA but do not recompute).
    """

    geometry: list[tuple[float, float]]
    distance_km: float
    duration_min: float
    source: str

    def is_valid(self) -> bool:
        return len(self.geometry) >= 2 and self.distance_km > 0 and self.duration_min > 0


@dataclass
class RoutingConfig:
    providers: tuple[str, ...] = ("osrm", "osrm_public")
    public_provider: str = "openrouteservice"
    public_base_url: str = "https://api.openrouteservice.org"
    public_api_key: str | None = None
    osrm_url: str = "http://osrm:5000"
    osrm_public_url: str = "https://router.project-osrm.org"
    request_timeout_s: float = 6.0
    retries_per_provider: int = 1

    @classmethod
    def from_env(cls) -> "RoutingConfig":
        raw = os.getenv("ROUTING_PROVIDERS", "osrm,osrm_public").strip()
        providers = tuple(p.strip() for p in raw.split(",") if p.strip())
        if not providers:
            providers = ("osrm", "osrm_public")
        return cls(
            providers=providers,
            public_provider=os.getenv("ROUTING_PUBLIC_PROVIDER", "openrouteservice").strip() or "openrouteservice",
            public_base_url=os.getenv("ROUTING_PUBLIC_BASE_URL", "https://api.openrouteservice.org").rstrip("/"),
            public_api_key=(os.getenv("ROUTING_PUBLIC_API_KEY", "").strip() or None),
            osrm_url=os.getenv("ROUTING_OSRM_URL", os.getenv("OSRM_URL", "http://osrm:5000")).rstrip("/"),
            osrm_public_url=os.getenv("ROUTING_OSRM_PUBLIC_URL", "https://router.project-osrm.org").rstrip("/"),
            request_timeout_s=float(os.getenv("ROUTING_TIMEOUT_SECONDS", "6.0")),
            retries_per_provider=int(os.getenv("ROUTING_RETRIES", "1")),
        )


# ── polyline decoding (Google / ORS encoded format) ────────────────────────────
def _decode_polyline(encoded: str, precision: int = 5) -> list[tuple[float, float]]:
    if not encoded:
        return []
    coords: list[tuple[float, float]] = []
    index = 0
    lat = 0
    lng = 0
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
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        coords.append((lat / factor, lng / factor))
    return coords


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return EARTH_RADIUS_KM * 2 * math.asin(math.sqrt(a))


def cumulative_distances_km(geometry: Iterable[tuple[float, float]]) -> list[float]:
    """Return cumulative distance (km) at each vertex; first entry is 0."""
    out: list[float] = []
    total = 0.0
    prev: tuple[float, float] | None = None
    for pt in geometry:
        if prev is None:
            out.append(0.0)
        else:
            total += haversine_km(prev[0], prev[1], pt[0], pt[1])
            out.append(total)
        prev = pt
    return out


def interpolate_on_route(
    route: Route,
    cumulative_km: list[float],
    progress: float,
) -> tuple[tuple[float, float], float]:
    """Return (lat, lon) at a given progress [0..1] along `route.geometry`.

    `cumulative_km` must be the list returned by `cumulative_distances_km`
    for this route's geometry (cached by the caller to avoid recomputing on
    every tick). Also returns the bearing at that point (degrees).
    """
    progress = max(0.0, min(1.0, float(progress)))
    geom = route.geometry
    if not geom:
        raise ValueError("cannot interpolate on an empty route geometry")
    if len(geom) == 1 or cumulative_km[-1] <= 0:
        return geom[0], 0.0

    target = progress * cumulative_km[-1]
    # binary-search would be fine, but linear scan is plenty fast for our sizes
    for i in range(1, len(cumulative_km)):
        if cumulative_km[i] >= target:
            seg_start = cumulative_km[i - 1]
            seg_end = cumulative_km[i]
            span = seg_end - seg_start
            t = 0.0 if span <= 0 else (target - seg_start) / span
            (lat1, lon1) = geom[i - 1]
            (lat2, lon2) = geom[i]
            lat = lat1 + (lat2 - lat1) * t
            lon = lon1 + (lon2 - lon1) * t
            bearing = _bearing(lat1, lon1, lat2, lon2)
            return (lat, lon), bearing
    last = geom[-1]
    before = geom[-2]
    return last, _bearing(before[0], before[1], last[0], last[1])


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlon) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


# ── providers ──────────────────────────────────────────────────────────────────
class _Provider:
    name: str = "base"

    async def route(
        self,
        client: httpx.AsyncClient,
        origin: tuple[float, float],
        dest: tuple[float, float],
    ) -> Route:
        raise NotImplementedError

    async def healthcheck(self, client: httpx.AsyncClient) -> tuple[str, str]:
        """Probe the provider. Returns `(status, detail)` where status is one
        of `healthy`, `unhealthy`, `not_probed`. Default: not_probed so that
        providers we don't want to burn quota on (e.g. ORS) are treated as
        opaque at startup. Override in a subclass for cheap probes."""
        return ("not_probed", "")


class _OsrmProvider(_Provider):
    name = "osrm"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def route(
        self,
        client: httpx.AsyncClient,
        origin: tuple[float, float],
        dest: tuple[float, float],
    ) -> Route:
        # OSRM wants lon,lat order.
        coords = f"{origin[1]:.6f},{origin[0]:.6f};{dest[1]:.6f},{dest[0]:.6f}"
        url = f"{self.base_url}/route/v1/driving/{coords}"
        params = {
            "overview": "full",
            "geometries": "polyline",
            "steps": "false",
            "annotations": "false",
        }
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != "Ok" or not payload.get("routes"):
            raise RuntimeError(f"osrm_no_route:{payload.get('code')}")
        r0 = payload["routes"][0]
        geometry = _decode_polyline(r0.get("geometry", ""), precision=5)
        if len(geometry) < 2:
            raise RuntimeError("osrm_empty_geometry")
        return Route(
            geometry=geometry,
            distance_km=float(r0["distance"]) / 1000.0,
            duration_min=max(float(r0["duration"]) / 60.0, 0.5),
            source="osrm",
        )

    async def healthcheck(self, client: httpx.AsyncClient) -> tuple[str, str]:
        # Two nearby points in Manhattan — cheap and quota-free.
        probe = "-74.0060,40.7128;-74.0100,40.7200"
        url = f"{self.base_url}/route/v1/driving/{probe}"
        try:
            resp = await client.get(url, params={"overview": "false"}, timeout=5.0)
        except Exception as exc:
            return ("unhealthy", type(exc).__name__)
        if resp.status_code != 200:
            return ("unhealthy", f"http_{resp.status_code}")
        try:
            data = resp.json()
        except Exception:
            return ("unhealthy", "bad_json")
        if data.get("code") != "Ok":
            return ("unhealthy", f"code_{data.get('code')}")
        return ("healthy", "")


class _OpenRouteServiceProvider(_Provider):
    name = "openrouteservice"

    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    async def route(
        self,
        client: httpx.AsyncClient,
        origin: tuple[float, float],
        dest: tuple[float, float],
    ) -> Route:
        url = f"{self.base_url}/v2/directions/driving-car"
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json, application/geo+json",
        }
        body = {
            "coordinates": [
                [origin[1], origin[0]],
                [dest[1], dest[0]],
            ],
            "instructions": False,
            "geometry_simplify": False,
        }
        resp = await client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        payload = resp.json()
        routes = payload.get("routes") or []
        if not routes:
            raise RuntimeError("ors_no_routes")
        r0 = routes[0]
        geometry = _decode_polyline(r0.get("geometry", ""), precision=5)
        if len(geometry) < 2:
            raise RuntimeError("ors_empty_geometry")
        summary = r0.get("summary") or {}
        return Route(
            geometry=geometry,
            distance_km=float(summary.get("distance", 0.0)) / 1000.0,
            duration_min=max(float(summary.get("duration", 0.0)) / 60.0, 0.5),
            source="openrouteservice",
        )


class _PublicOsrmProvider(_OsrmProvider):
    """Public OSRM endpoint fallback.

    This provider is quota/throughput-limited and should remain a fallback
    behind a local OSRM instance for production-like runs.
    """

    name = "osrm_public"


# ── client ─────────────────────────────────────────────────────────────────────
class RoutingClient:
    """Async routing client that walks the configured provider chain.

    The client owns a shared `httpx.AsyncClient` so connections are reused.
    It also memoises per-segment routes with a tiny LRU-ish dict so that a
    repositioning chain that revisits the same pickup/dropoff does not hit
    the network again.
    """

    def __init__(self, config: RoutingConfig | None = None) -> None:
        self.config = config or RoutingConfig.from_env()
        self._client: httpx.AsyncClient | None = None
        self._providers: list[_Provider] = self._build_providers()
        self._cache: dict[tuple[int, int, int, int], Route] = {}
        self._cache_order: list[tuple[int, int, int, int]] = []
        self._cache_max = 512
        self._last_error: str | None = None

    def _build_providers(self) -> list[_Provider]:
        out: list[_Provider] = []
        for name in self.config.providers:
            key = name.lower()
            if key == "public":
                if not self.config.public_api_key:
                    log.info("routing: 'public' provider skipped (no ROUTING_PUBLIC_API_KEY)")
                    continue
                if self.config.public_provider.lower() == "openrouteservice":
                    out.append(_OpenRouteServiceProvider(self.config.public_base_url, self.config.public_api_key))
                else:
                    log.warning("routing: unknown public provider '%s'", self.config.public_provider)
            elif key == "osrm":
                out.append(_OsrmProvider(self.config.osrm_url))
            elif key == "osrm_public":
                out.append(_PublicOsrmProvider(self.config.osrm_public_url))
            else:
                log.warning("routing: unknown provider '%s'", name)
        return out

    @property
    def active_providers(self) -> list[str]:
        return [p.name for p in self._providers]

    @property
    def last_error(self) -> str | None:
        return self._last_error

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.config.request_timeout_s)

    async def healthcheck(self) -> dict[str, str]:
        """Ping each active provider and return `{name: status}`.

        `status` is either `healthy`, `not_probed`, or `unhealthy:<detail>`.
        This is called once at scenario startup as a fail-informative probe
        (we don't abort, but we want a clear log line when OSRM isn't up).
        """
        if self._client is None:
            await self.start()
        out: dict[str, str] = {}
        for p in self._providers:
            try:
                status, detail = await p.healthcheck(self._client)  # type: ignore[arg-type]
            except Exception as exc:
                status, detail = ("unhealthy", type(exc).__name__)
            out[p.name] = status if not detail else f"{status}:{detail}"
        return out

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            finally:
                self._client = None

    def _cache_key(self, origin: tuple[float, float], dest: tuple[float, float]) -> tuple[int, int, int, int]:
        # round to ~11m to dedupe centroid-to-centroid queries cheaply
        return (
            int(round(origin[0] * 1e4)),
            int(round(origin[1] * 1e4)),
            int(round(dest[0] * 1e4)),
            int(round(dest[1] * 1e4)),
        )

    def _cache_put(self, key: tuple[int, int, int, int], route: Route) -> None:
        if key in self._cache:
            return
        self._cache[key] = route
        self._cache_order.append(key)
        if len(self._cache_order) > self._cache_max:
            old = self._cache_order.pop(0)
            self._cache.pop(old, None)

    async def route_segment(
        self,
        origin: tuple[float, float],
        dest: tuple[float, float],
    ) -> Route:
        """Route a single origin->dest segment.

        Walks the configured provider chain in order. If all providers fail
        (or none are configured), raises `RoutingUnavailableError`. Never
        falls back to a straight-line geometry; the single-driver caller is
        expected to hold the driver at its last known routed point.
        """
        if haversine_km(origin[0], origin[1], dest[0], dest[1]) < 0.02:
            # endpoints are the same (<20 m) -> synthetic micro-route, no network
            return Route(
                geometry=[origin, dest],
                distance_km=0.02,
                duration_min=0.1,
                source="identity",
            )

        key = self._cache_key(origin, dest)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        if not self._providers:
            self._last_error = "no_providers_configured"
            raise RoutingUnavailableError("no routing provider configured (ROUTING_PROVIDERS empty)")
        if self._client is None:
            await self.start()
        assert self._client is not None

        errors: list[str] = []
        for provider in self._providers:
            for attempt in range(1, max(1, self.config.retries_per_provider) + 1):
                try:
                    route = await provider.route(self._client, origin, dest)
                    if route.is_valid():
                        self._cache_put(key, route)
                        self._last_error = None
                        return route
                    errors.append(f"{provider.name}:invalid_route")
                except Exception as exc:
                    errors.append(f"{provider.name}:{type(exc).__name__}")
                    if attempt < self.config.retries_per_provider:
                        await asyncio.sleep(0.15 * attempt)
        self._last_error = ", ".join(errors)
        raise RoutingUnavailableError(f"all routing providers failed: {self._last_error}")
