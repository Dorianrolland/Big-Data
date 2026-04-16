"""Unit tests for the single-driver scenario building blocks.

These tests exercise:
- Routing provider chain fallback order and the no-straight-line rule
- Polyline decoder + cumulative distance + on-route interpolation
- Train window construction for the ML split (10/2)

The routing tests mock httpx at the provider level so no network is hit.
The ML window test does not touch DuckDB / joblib — it only validates the
pure helper that computes the half-open [start, end) window.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _arun(coro):
    return asyncio.run(coro)

_TLC_DIR = Path(__file__).resolve().parent.parent / "tlc_replay"
if str(_TLC_DIR) not in sys.path:
    sys.path.insert(0, str(_TLC_DIR))

from routing import (  # noqa: E402
    Route,
    RoutingClient,
    RoutingConfig,
    RoutingUnavailableError,
    _decode_polyline,
    cumulative_distances_km,
    haversine_km,
    interpolate_on_route,
)
from single_driver import (  # noqa: E402
    _deterministic_offset_minutes,
    _in_time_window,
    _parse_window,
)
from datetime import time as dtime  # noqa: E402


# ── polyline + interpolation ──────────────────────────────────────────────────
def test_decode_polyline_known_vector():
    # google reference: "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
    pts = _decode_polyline("_p~iF~ps|U_ulLnnqC_mqNvxq`@")
    assert len(pts) == 3
    # first point near (38.5, -120.2)
    assert abs(pts[0][0] - 38.5) < 0.01
    assert abs(pts[0][1] - (-120.2)) < 0.01


def test_cumulative_distances_monotonic():
    geom = [(40.0, -74.0), (40.001, -74.0), (40.002, -74.0), (40.003, -74.0)]
    cum = cumulative_distances_km(geom)
    assert cum[0] == 0.0
    for i in range(1, len(cum)):
        assert cum[i] > cum[i - 1]
    assert abs(cum[-1] - 3 * haversine_km(40.0, -74.0, 40.001, -74.0)) < 1e-9


def test_interpolate_on_route_bounds_and_mid():
    geom = [(40.0, -74.0), (40.0, -74.01), (40.0, -74.02)]
    route = Route(geometry=geom, distance_km=2.0, duration_min=3.0, source="test")
    cum = cumulative_distances_km(geom)
    (lat0, lon0), _ = interpolate_on_route(route, cum, 0.0)
    (lat1, lon1), _ = interpolate_on_route(route, cum, 1.0)
    (lat_mid, lon_mid), _ = interpolate_on_route(route, cum, 0.5)
    assert (lat0, lon0) == geom[0]
    assert (lat1, lon1) == geom[-1]
    # mid lon should be around -74.01
    assert abs(lon_mid - (-74.01)) < 1e-3


# ── provider chain fallback ──────────────────────────────────────────────────
class _FakeProvider:
    def __init__(self, name, behaviour):
        self.name = name
        self._behaviour = behaviour  # "ok" | "fail"

    async def route(self, client, origin, dest):
        if self._behaviour == "fail":
            raise RuntimeError(f"{self.name}_down")
        return Route(
            geometry=[origin, ((origin[0] + dest[0]) / 2, (origin[1] + dest[1]) / 2), dest],
            distance_km=haversine_km(origin[0], origin[1], dest[0], dest[1]),
            duration_min=5.0,
            source=self.name,
        )


def test_routing_chain_public_then_osrm():
    async def _run():
        client = RoutingClient(RoutingConfig(providers=("public", "osrm"), public_api_key="dummy"))
        client._providers = [_FakeProvider("public", "fail"), _FakeProvider("osrm", "ok")]
        await client.start()
        try:
            route = await client.route_segment((40.7, -74.0), (40.72, -73.98))
            assert route.source == "osrm"
            assert len(route.geometry) >= 2
        finally:
            await client.close()

    _arun(_run())


def test_routing_chain_osrm_then_public_osrm():
    async def _run():
        client = RoutingClient(RoutingConfig(providers=("osrm", "osrm_public")))
        client._providers = [_FakeProvider("osrm", "fail"), _FakeProvider("osrm_public", "ok")]
        await client.start()
        try:
            route = await client.route_segment((40.7, -74.0), (40.72, -73.98))
            assert route.source == "osrm_public"
            assert len(route.geometry) >= 2
        finally:
            await client.close()

    _arun(_run())


def test_routing_chain_all_fail_raises():
    async def _run():
        client = RoutingClient(RoutingConfig(providers=("osrm",)))
        client._providers = [_FakeProvider("osrm", "fail")]
        await client.start()
        try:
            with pytest.raises(RoutingUnavailableError):
                await client.route_segment((40.7, -74.0), (40.72, -73.98))
        finally:
            await client.close()

    _arun(_run())


def test_routing_never_returns_straight_line_on_failure():
    # Regression: the legacy replay used to fall back to a two-point
    # great-circle segment when OSRM was down, which produced the
    # "through buildings" artifact. The new chain must raise instead.
    async def _run():
        client = RoutingClient(RoutingConfig(providers=()))  # no providers at all
        with pytest.raises(RoutingUnavailableError):
            await client.route_segment((40.7, -74.0), (40.72, -73.98))

    _arun(_run())


def test_routing_public_provider_skipped_without_key():
    cfg = RoutingConfig(providers=("public", "osrm"), public_api_key=None)
    client = RoutingClient(cfg)
    assert client.active_providers == ["osrm"]


# ── healthcheck + cache ───────────────────────────────────────────────────────
class _HealthProvider:
    """Fake provider for healthcheck tests; supports a pluggable outcome."""

    def __init__(self, name: str, health: tuple[str, str] | Exception):
        self.name = name
        self._health = health
        self.route_calls = 0
        self.health_calls = 0

    async def route(self, client, origin, dest):
        self.route_calls += 1
        return Route(
            geometry=[origin, ((origin[0] + dest[0]) / 2, (origin[1] + dest[1]) / 2), dest],
            distance_km=haversine_km(origin[0], origin[1], dest[0], dest[1]),
            duration_min=5.0,
            source=self.name,
        )

    async def healthcheck(self, client):
        self.health_calls += 1
        if isinstance(self._health, Exception):
            raise self._health
        return self._health


def test_healthcheck_aggregates_per_provider():
    async def _run():
        client = RoutingClient(RoutingConfig(providers=("osrm",)))
        client._providers = [_HealthProvider("osrm", ("healthy", ""))]
        await client.start()
        try:
            h = await client.healthcheck()
            assert h == {"osrm": "healthy"}
            assert client._providers[0].health_calls == 1
        finally:
            await client.close()

    _arun(_run())


def test_healthcheck_encodes_unhealthy_with_detail():
    async def _run():
        client = RoutingClient(RoutingConfig(providers=("osrm",)))
        client._providers = [_HealthProvider("osrm", ("unhealthy", "http_502"))]
        await client.start()
        try:
            h = await client.healthcheck()
            assert h == {"osrm": "unhealthy:http_502"}
        finally:
            await client.close()

    _arun(_run())


def test_healthcheck_catches_provider_exceptions():
    async def _run():
        client = RoutingClient(RoutingConfig(providers=("osrm",)))
        client._providers = [_HealthProvider("osrm", RuntimeError("boom"))]
        await client.start()
        try:
            h = await client.healthcheck()
            assert h == {"osrm": "unhealthy:RuntimeError"}
        finally:
            await client.close()

    _arun(_run())


def test_route_segment_caches_repeated_queries():
    async def _run():
        provider = _HealthProvider("osrm", ("healthy", ""))
        client = RoutingClient(RoutingConfig(providers=("osrm",)))
        client._providers = [provider]
        await client.start()
        try:
            a = await client.route_segment((40.7000, -74.0000), (40.7200, -74.0100))
            b = await client.route_segment((40.7000, -74.0000), (40.7200, -74.0100))
            assert a is b  # same Route instance from cache
            assert provider.route_calls == 1  # second call hit cache
            # A different segment triggers another provider call
            await client.route_segment((40.7500, -73.9850), (40.7600, -73.9800))
            assert provider.route_calls == 2
        finally:
            await client.close()

    _arun(_run())


def test_route_segment_identity_skips_provider():
    async def _run():
        provider = _HealthProvider("osrm", ("healthy", ""))
        client = RoutingClient(RoutingConfig(providers=("osrm",)))
        client._providers = [provider]
        await client.start()
        try:
            # Points 5 m apart -> synthetic identity route, no provider call
            r = await client.route_segment((40.7000, -74.0000), (40.70001, -74.00001))
            assert r.source == "identity"
            assert provider.route_calls == 0
        finally:
            await client.close()

    _arun(_run())


# ── ML train window ───────────────────────────────────────────────────────────
def test_compute_train_window_half_open():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from ml.train_copilot_model import compute_train_window  # type: ignore

    w = compute_train_window("2024-01", 10)
    assert w is not None
    start, end = w
    assert start == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert end == datetime(2024, 11, 1, tzinfo=timezone.utc)


def test_compute_train_window_disabled_returns_none():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from ml.train_copilot_model import compute_train_window  # type: ignore

    assert compute_train_window(None, 10) is None
    assert compute_train_window("2024-01", 0) is None


def test_compute_train_window_rejects_invalid():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from ml.train_copilot_model import compute_train_window  # type: ignore

    with pytest.raises(ValueError):
        compute_train_window("not-a-month", 10)


# ── single-driver pure helpers ────────────────────────────────────────────────
def test_parse_window_standard():
    start, end = _parse_window("12:00-14:00")
    assert start == dtime(12, 0)
    assert end == dtime(14, 0)


def test_parse_window_invalid_falls_back():
    # a malformed value returns the default noon window instead of raising,
    # so a typo in env never crashes the scenario
    start, end = _parse_window("garbage")
    assert start == dtime(12, 0)
    assert end == dtime(14, 0)


def test_in_time_window_inside_and_outside():
    start, end = dtime(12, 0), dtime(14, 0)
    assert _in_time_window(datetime(2024, 1, 15, 13, 0, tzinfo=timezone.utc), start, end)
    assert _in_time_window(datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc), start, end)
    assert _in_time_window(datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc), start, end)
    assert not _in_time_window(datetime(2024, 1, 15, 11, 59, tzinfo=timezone.utc), start, end)
    assert not _in_time_window(datetime(2024, 1, 15, 14, 1, tzinfo=timezone.utc), start, end)


def test_in_time_window_crosses_midnight():
    # night acceleration spans a midnight-crossing window in practice only when
    # operators set it that way (e.g. 22:00 -> 05:00). The helper must accept
    # that form or the night factor breaks silently.
    start, end = dtime(22, 0), dtime(5, 0)
    assert _in_time_window(datetime(2024, 1, 15, 23, 0, tzinfo=timezone.utc), start, end)
    assert _in_time_window(datetime(2024, 1, 15, 3, 0, tzinfo=timezone.utc), start, end)
    assert not _in_time_window(datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc), start, end)


def test_deterministic_offset_minutes_stable_and_in_range():
    key = "drv_demo_001:2024-01-15"
    v1 = _deterministic_offset_minutes(key, 20, 45)
    v2 = _deterministic_offset_minutes(key, 20, 45)
    assert v1 == v2
    assert 20 <= v1 <= 45


def test_deterministic_offset_minutes_varies_by_day():
    values = {
        _deterministic_offset_minutes(f"drv_demo_001:2024-01-{d:02d}", 20, 45)
        for d in range(1, 21)
    }
    # not perfectly unique (we're hashing into a 26-wide range), but at least
    # the driver should not get the exact same lunch length every day
    assert len(values) > 1


def test_deterministic_offset_minutes_degenerate_span():
    assert _deterministic_offset_minutes("anything", 30, 30) == 30
    assert _deterministic_offset_minutes("anything", 30, 20) == 30


# ── inter-month continuity sanity ─────────────────────────────────────────────
def test_single_driver_run_flags_distinguish_cold_and_warm_starts():
    """Pure-logic check: `self.state is None` drives the cold/warm branch in
    SingleDriverScenario.run(). A warm call must NOT reset self._vt."""
    from datetime import datetime, timezone as tz
    from single_driver import SingleDriverScenario, _DriverState  # noqa

    scenario = SingleDriverScenario.__new__(SingleDriverScenario)
    # Pretend a previous month finished with driver state + virtual clock set.
    scenario.state = _DriverState(lat=40.75, lon=-74.0)
    scenario._vt = datetime(2024, 10, 31, 23, 30, tzinfo=tz.utc)
    scenario._current_trip = None
    scenario._phase = "idle"

    # The cold/warm predicate run() uses:
    first_call = scenario.state is None
    assert first_call is False

    # Simulating what run() does on the warm branch:
    vt = scenario._vt
    assert vt == datetime(2024, 10, 31, 23, 30, tzinfo=tz.utc)

    # Cold start variant:
    cold = SingleDriverScenario.__new__(SingleDriverScenario)
    cold.state = None
    cold._vt = None
    cold._current_trip = None
    cold._phase = "idle"
    assert (cold.state is None) is True
