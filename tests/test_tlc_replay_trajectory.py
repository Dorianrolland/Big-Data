"""Unit tests for route-aware trajectory logic in tlc_replay/main.py."""
from __future__ import annotations

import asyncio
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path


_TLC_MAIN_PATH = Path(__file__).resolve().parent.parent / "tlc_replay" / "main.py"
_SPEC = importlib.util.spec_from_file_location("tlc_replay_main", _TLC_MAIN_PATH)
assert _SPEC and _SPEC.loader
replay_main = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(replay_main)


def _make_trip(*, trip_key: str, pu_loc: int = 100, do_loc: int = 200) -> object:
    request_ts = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    pickup_ts = request_ts + timedelta(minutes=1)
    dropoff_ts = pickup_ts + timedelta(minutes=12)
    return replay_main.Trip(
        trip_key=trip_key,
        courier_id="drv_demo_001",
        offer_id=f"offer_{trip_key}",
        order_id=f"order_{trip_key}",
        request_ts=request_ts,
        pickup_ts=pickup_ts,
        dropoff_ts=dropoff_ts,
        pu_loc=pu_loc,
        do_loc=do_loc,
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7420,
        dropoff_lon=-73.9730,
        trip_km=3.2,
        trip_min=12.0,
        fare_usd=15.0,
    )


def test_interpolate_on_geometry_follows_route_shape():
    geometry = [
        (0.0, 0.0),
        (0.0, 1.0),
        (1.0, 1.0),
    ]
    cumulative = replay_main.cumulative_route_distances_km(geometry)
    lat, lon, heading = replay_main.interpolate_on_geometry(geometry, cumulative, 0.5)

    # Halfway on this L-shaped route should be near the corner (0, 1),
    # not near the straight-line midpoint (0.5, 0.5).
    assert abs(lat - 0.0) < 0.01
    assert abs(lon - 1.0) < 0.01
    assert abs(lat - 0.5) > 0.2
    assert 0.0 <= heading <= 360.0


def test_decode_polyline_known_shape():
    # Encodes [(38.5,-120.2),(40.7,-120.95),(43.252,-126.453)] from Google polyline docs.
    encoded = "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
    points = replay_main.decode_polyline(encoded)

    assert len(points) == 3
    assert points[0] == (38.5, -120.2)
    assert points[-1] == (43.252, -126.453)


def test_source_platform_for_route_replaces_existing_route_suffix():
    tagged = replay_main.source_platform_for_route("tlc_hvfhv|route=linear", "osrm")

    assert tagged == "tlc_hvfhv|route=osrm"


def test_prepare_trip_route_falls_back_to_linear_when_osrm_fails(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="fallback_case")

    async def _raise_osrm(*_args, **_kwargs):
        raise RuntimeError("osrm_down")

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _raise_osrm)

    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "linear"
    assert len(trip.route_geometry) == 2
    assert replay.stats_route_linear_fallback == 1
    assert replay.stats_route_osrm_errors == 1


def test_prepare_trip_route_uses_zone_pair_cache(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip_a = _make_trip(trip_key="cache_a", pu_loc=10, do_loc=20)
    trip_b = _make_trip(trip_key="cache_b", pu_loc=10, do_loc=20)

    calls: dict[str, int] = {"count": 0}

    async def _fake_osrm(*_args, **_kwargs):
        calls["count"] += 1
        return [
            (40.7580, -73.9855),
            (40.7500, -73.9790),
            (40.7420, -73.9730),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_osrm)

    asyncio.run(replay._prepare_trip_route(trip_a))
    asyncio.run(replay._prepare_trip_route(trip_b))

    assert calls["count"] == 1
    assert replay.stats_route_cache_hits == 1
    assert replay.stats_route_cache_misses == 1
    assert trip_a.route_source == "osrm"
    assert trip_b.route_source == "osrm"


def test_prepare_trip_route_respects_cache_max(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "linear")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_CACHE_MAX", 1)

    trip_a = _make_trip(trip_key="evict_a", pu_loc=10, do_loc=20)
    trip_b = _make_trip(trip_key="evict_b", pu_loc=11, do_loc=21)
    trip_c = _make_trip(trip_key="evict_c", pu_loc=10, do_loc=20)

    asyncio.run(replay._prepare_trip_route(trip_a))
    asyncio.run(replay._prepare_trip_route(trip_b))
    asyncio.run(replay._prepare_trip_route(trip_c))

    # cache max=1 so key(10,20) is evicted by key(11,21), then misses again on trip_c
    assert replay.stats_route_cache_hits == 0
    assert replay.stats_route_cache_misses == 3
