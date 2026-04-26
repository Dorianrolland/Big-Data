"""Unit tests for route-aware trajectory logic in tlc_replay/main.py."""
from __future__ import annotations

import asyncio
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


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
    tagged_public = replay_main.source_platform_for_route("tlc_hvfhv", "osrm_public")
    tagged_valhalla = replay_main.source_platform_for_route("tlc_hvfhv", "valhalla_public")

    assert tagged == "tlc_hvfhv|route=osrm"
    assert tagged_public == "tlc_hvfhv|route=osrm"
    assert tagged_valhalla == "tlc_hvfhv|route=osrm"


def test_prepare_trip_route_holds_when_osrm_fails_under_strict_road_geometry(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="fallback_case")

    async def _raise_osrm(*_args, **_kwargs):
        raise RuntimeError("osrm_down")

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _raise_osrm)

    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "hold"
    assert trip.route_geometry == [(trip.pickup_lat, trip.pickup_lon)]
    assert replay.stats_route_hold_fallback == 1
    assert replay.stats_route_osrm_errors == 1


def test_prepare_trip_route_holds_position_when_linear_fallback_disabled(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="hold_case")

    async def _raise_osrm(*_args, **_kwargs):
        raise RuntimeError("osrm_down")

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_LINEAR_FALLBACK_ALLOWED", False)
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _raise_osrm)

    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "hold"
    assert trip.route_geometry == [(trip.pickup_lat, trip.pickup_lon)]
    assert trip.route_cumulative_km == [0.0]
    assert replay.stats_route_hold_fallback == 1


def test_prepare_trip_route_can_still_use_linear_when_strict_mode_disabled(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="legacy_linear_case")

    async def _raise_osrm(*_args, **_kwargs):
        raise RuntimeError("osrm_down")

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_STRICT_ROAD_GEOMETRY", False)
    monkeypatch.setattr(replay_main, "TLC_ROUTE_LINEAR_FALLBACK_ALLOWED", True)
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _raise_osrm)

    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "linear"
    assert len(trip.route_geometry) >= 2
    assert trip.route_geometry[0] == (trip.pickup_lat, trip.pickup_lon)
    assert trip.route_geometry[-1] == (trip.dropoff_lat, trip.dropoff_lon)
    assert replay.stats_route_linear_fallback == 1


def test_synthetic_grid_geometry_uses_turn_for_long_segments():
    geometry = replay_main.synthetic_grid_geometry(
        origin_lat=40.7580,
        origin_lon=-73.9855,
        dest_lat=40.7420,
        dest_lon=-73.9730,
    )

    assert len(geometry) >= 4
    assert geometry[0] == (40.7580, -73.9855)
    assert geometry[-1] == (40.7420, -73.9730)


def test_synthetic_grid_geometry_east_river_crossing_uses_bridge_waypoints():
    geometry = replay_main.synthetic_grid_geometry(
        origin_lat=40.7180,
        origin_lon=-74.0080,   # Lower Manhattan side
        dest_lat=40.7120,
        dest_lon=-73.9530,     # Brooklyn/Queens side
    )

    assert len(geometry) >= 6
    assert any(point[1] < -73.99 for point in geometry)  # west side present
    assert any(point[1] > -73.98 for point in geometry)  # east side present


def test_densify_geometry_inserts_intermediate_points():
    geometry = [(40.7580, -73.9855), (40.7580, -73.9655)]  # ~1.7 km east-west
    dense = replay_main.densify_geometry(geometry, max_step_km=0.2)

    assert len(dense) > len(geometry)
    assert dense[0] == geometry[0]
    assert dense[-1] == geometry[-1]


def test_route_geometry_for_replay_respects_disable_switch(monkeypatch):
    geometry = [(40.7580, -73.9855), (40.7420, -73.9730)]
    monkeypatch.setattr(replay_main, "TLC_ROUTE_DENSIFY_MAX_STEP_KM", 0.0)

    out = replay_main.route_geometry_for_replay(geometry)
    assert out == geometry


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


def test_init_fleet_driver_pool_prefers_road_safe_zone_anchors():
    replay = replay_main.TLCReplay("2024-01")
    replay.fleet_pool_enabled = True
    replay.centroids = {
        10: (40.7001, -73.9001),
        20: (40.7002, -73.9002),
    }
    replay.zone_road_anchors = {
        10: (40.7101, -73.9101),
        20: (40.7102, -73.9102),
    }

    replay._init_fleet_driver_pool()

    first_state = replay.fleet_driver_state[replay.fleet_driver_ids[0]]
    assert first_state[1] == pytest.approx(40.7101, abs=1e-6)
    assert first_state[2] == pytest.approx(-73.9101, abs=1e-6)


def test_fetch_osrm_geometry_falls_back_to_public_provider():
    replay = replay_main.TLCReplay("2024-01")
    replay.route_osrm_targets = [
        ("osrm", "http://local-osrm"),
        ("osrm_public", "https://router.project-osrm.org"),
    ]

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "code": "Ok",
                "routes": [{"geometry": "_p~iF~ps|U_ulLnnqC_mqNvxq`@"}],
            }

    class _FakeClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def get(self, url: str, params=None):  # noqa: ANN001
            _ = params
            self.calls.append(url)
            if "local-osrm" in url:
                raise RuntimeError("local_osrm_down")
            return _FakeResponse()

    replay.osrm_client = _FakeClient()
    source, geometry, snap_meta = asyncio.run(
        replay._fetch_osrm_geometry(
            origin_lat=40.7580,
            origin_lon=-73.9855,
            dest_lat=40.7420,
            dest_lon=-73.9730,
        )
    )

    assert source == "osrm_public"
    assert len(geometry) >= 2
    assert replay._last_osrm_provider == "osrm_public"
    assert snap_meta["origin_snap_km"] >= 0
    assert snap_meta["dest_snap_km"] >= 0


def test_fetch_osrm_geometry_supports_valhalla_public_provider():
    replay = replay_main.TLCReplay("2024-01")
    replay.route_osrm_targets = [("valhalla_public", "https://valhalla1.openstreetmap.de")]

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "trip": {
                    "legs": [
                        {
                            "shape": "_izlhA~rlgdF_{geC~ywl@_kwzCn`{nI",
                        }
                    ]
                }
            }

    class _FakeClient:
        async def get(self, *_args, **_kwargs):  # noqa: ANN001
            return _FakeResponse()

    replay.osrm_client = _FakeClient()
    source, geometry, snap_meta = asyncio.run(
        replay._fetch_osrm_geometry(
            origin_lat=40.7580,
            origin_lon=-73.9855,
            dest_lat=40.7420,
            dest_lon=-73.9730,
        )
    )

    assert source == "valhalla_public"
    assert len(geometry) >= 2
    assert replay._last_osrm_provider == "valhalla_public"
    assert snap_meta["origin_snap_km"] >= 0
    assert snap_meta["dest_snap_km"] >= 0


def test_prepare_trip_route_tracks_public_osrm_source(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="public_osrm_case")

    async def _fake_osrm(*_args, **_kwargs):
        replay._last_osrm_provider = "osrm_public"
        return [
            (40.7580, -73.9855),
            (40.7500, -73.9790),
            (40.7420, -73.9730),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_osrm)
    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "osrm_public"
    assert replay.stats_route_osrm_public_success == 1
    assert replay.stats_route_osrm_success == 0


def test_prepare_trip_route_timeout_triggers_prefetch_schedule(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="prefetch_timeout_case")
    called: dict[str, int] = {"count": 0}

    async def _slow_fetch(*_args, **_kwargs):
        await asyncio.sleep(0.35)
        return "osrm_public", [
            (40.7580, -73.9855),
            (40.7500, -73.9790),
            (40.7420, -73.9730),
        ]

    def _fake_schedule(_trip, _key):
        called["count"] += 1

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_FETCH_TIMEOUT_S", 0.001)
    monkeypatch.setattr(replay_main, "TLC_ROUTE_OSRM_TIMEOUT_S", 0.1)
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _slow_fetch)
    monkeypatch.setattr(replay, "_schedule_route_prefetch", _fake_schedule)

    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "hold"
    assert replay.stats_route_hold_fallback == 1
    assert called["count"] == 1


def test_schedule_route_prefetch_populates_cache(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="prefetch_cache_case", pu_loc=33, do_loc=44)
    key = (int(trip.pu_loc), int(trip.do_loc))

    async def _fake_fetch(*_args, **_kwargs):
        return "osrm_public", [
            (40.7580, -73.9855),
            (40.7500, -73.9790),
            (40.7420, -73.9730),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_PREFETCH_ENABLED", True)
    replay.osrm_client = object()
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_fetch)

    async def _run_prefetch():
        replay._schedule_route_prefetch(trip, key)
        while replay.route_prefetch_tasks:
            await asyncio.sleep(0.01)

    asyncio.run(_run_prefetch())

    assert key in replay.route_cache
    source, geometry, cumulative = replay.route_cache[key]
    assert source == "osrm_public"
    assert len(geometry) >= 2
    assert cumulative[-1] > 0
    assert replay.stats_route_prefetch_started == 1
    assert replay.stats_route_prefetch_success == 1


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


def test_prefetch_can_upgrade_linear_cache_to_osrm(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="prefetch_upgrade_case", pu_loc=70, do_loc=71)
    key = (int(trip.pu_loc), int(trip.do_loc))

    # Existing fallback cache entry from a previous timeout.
    replay.route_cache[key] = (
        "linear",
        [(trip.pickup_lat, trip.pickup_lon), (trip.dropoff_lat, trip.dropoff_lon)],
        replay_main.cumulative_route_distances_km([(trip.pickup_lat, trip.pickup_lon), (trip.dropoff_lat, trip.dropoff_lon)]),
    )
    replay.route_cache_order.append(key)

    async def _fake_fetch(*_args, **_kwargs):
        return "osrm_public", [
            (40.7580, -73.9855),
            (40.7525, -73.9810),
            (40.7460, -73.9765),
            (40.7420, -73.9730),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_PREFETCH_ENABLED", True)
    replay.osrm_client = object()
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_fetch)

    async def _run_prefetch():
        replay._schedule_route_prefetch(trip, key)
        while replay.route_prefetch_tasks:
            await asyncio.sleep(0.01)

    asyncio.run(_run_prefetch())

    source, geometry, cumulative = replay.route_cache[key]
    assert source == "osrm_public"
    assert len(geometry) >= 3
    assert cumulative[-1] > 0


def test_row_to_trip_infers_missing_zone_centroid_without_times_square_collapse():
    replay = replay_main.TLCReplay("2024-01")
    request_ts = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    pickup_ts = request_ts + timedelta(minutes=2)
    dropoff_ts = pickup_ts + timedelta(minutes=10)
    row = {
        "dispatching_base_num": "B02764",
        "request_datetime": request_ts,
        "pickup_datetime": pickup_ts,
        "dropoff_datetime": dropoff_ts,
        "PULocationID": 161,  # known centroid
        "DOLocationID": 265,  # unknown in local centroid file
        "trip_miles": 2.4,
        "trip_time": 600,
        "base_passenger_fare": 12.0,
        "tolls": 0.0,
        "bcf": 0.0,
        "sales_tax": 0.0,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "tips": 0.0,
        "driver_pay": 0.0,
    }

    trip = replay._row_to_trip(row)
    assert trip is not None
    # Should not collapse to the legacy hardcoded fallback center.
    assert (round(trip.dropoff_lat, 6), round(trip.dropoff_lon, 6)) != (40.758000, -73.985500)
    # Keep a plausible local leg.
    inferred_km = replay_main.haversine_km(trip.pickup_lat, trip.pickup_lon, trip.dropoff_lat, trip.dropoff_lon)
    assert inferred_km > 0.3


def test_row_to_trip_prefers_road_safe_zone_anchor_when_available():
    replay = replay_main.TLCReplay("2024-01")
    replay.centroids = {
        161: (40.7580, -73.9855),
        238: (40.7420, -73.9730),
    }
    replay.zone_road_anchors = {
        161: (40.7571, -73.9844),
        238: (40.7415, -73.9722),
    }
    request_ts = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    pickup_ts = request_ts + timedelta(minutes=2)
    dropoff_ts = pickup_ts + timedelta(minutes=10)
    row = {
        "dispatching_base_num": "B02764",
        "request_datetime": request_ts,
        "pickup_datetime": pickup_ts,
        "dropoff_datetime": dropoff_ts,
        "PULocationID": 161,
        "DOLocationID": 238,
        "trip_miles": 2.4,
        "trip_time": 600,
        "base_passenger_fare": 12.0,
        "tolls": 0.0,
        "bcf": 0.0,
        "sales_tax": 0.0,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "tips": 0.0,
        "driver_pay": 0.0,
    }

    trip = replay._row_to_trip(row)
    assert trip is not None
    assert trip.pickup_lat == pytest.approx(40.7571, abs=1e-6)
    assert trip.pickup_lon == pytest.approx(-73.9844, abs=1e-6)
    assert trip.dropoff_lat == pytest.approx(40.7415, abs=1e-6)
    assert trip.dropoff_lon == pytest.approx(-73.9722, abs=1e-6)


def test_build_position_available_has_zero_speed():
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="available_speed_zero_case")
    pos = replay._build_position(
        trip,
        lat=trip.dropoff_lat,
        lon=trip.dropoff_lon,
        ts=trip.dropoff_ts,
        status="available",
    )

    assert pos.speed_kmh == 0.0


def test_emit_trip_start_and_finish_use_routed_endpoints(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="routed_anchor_case")

    async def _fake_osrm(*_args, **_kwargs):
        return "osrm_public", [
            (40.7571, -73.9844),
            (40.7500, -73.9790),
            (40.7415, -73.9722),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_osrm)
    asyncio.run(replay._prepare_trip_route(trip))

    sent: list[tuple[str, bytes]] = []

    async def _capture(topic, _key, value):  # noqa: ANN001
        sent.append((topic, value))

    monkeypatch.setattr(replay, "_send", _capture)
    asyncio.run(replay._emit_trip_start(trip))
    asyncio.run(replay._emit_trip_finish(trip))

    offer = replay_main.OrderOfferV1()
    offer.ParseFromString(sent[0][1])

    courier_payloads = [value for topic, value in sent if topic == replay_main.COURIER_TOPIC]
    pickup_pos = replay_main.CourierPositionV1()
    pickup_pos.ParseFromString(courier_payloads[0])
    available_pos = replay_main.CourierPositionV1()
    available_pos.ParseFromString(courier_payloads[1])

    assert pickup_pos.status == "pickup_arrived"
    assert pickup_pos.lat == pytest.approx(40.7571, abs=1e-6)
    assert pickup_pos.lon == pytest.approx(-73.9844, abs=1e-6)
    assert available_pos.status == "available"
    assert available_pos.lat == pytest.approx(40.7415, abs=1e-6)
    assert available_pos.lon == pytest.approx(-73.9722, abs=1e-6)
    assert offer.pickup_lat == pytest.approx(40.7571, abs=1e-6)
    assert offer.dropoff_lat == pytest.approx(40.7415, abs=1e-6)


def test_prepare_trip_route_rejects_waypoint_snaps_that_are_too_far(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="bad_waypoint_snap")

    async def _fake_osrm(*_args, **_kwargs):
        return "osrm", [
            (40.7000, -73.9500),
            (40.6900, -73.9400),
        ], {
            "snapped_origin": (40.7000, -73.9500),
            "snapped_dest": (40.6900, -73.9400),
            "origin_snap_km": 2.4,
            "dest_snap_km": 2.1,
        }

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_ROUTE_LINEAR_FALLBACK_ALLOWED", False)
    monkeypatch.setattr(replay_main, "TLC_ROUTE_WAYPOINT_MAX_SNAP_KM", 0.4)
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_osrm)

    asyncio.run(replay._prepare_trip_route(trip))

    assert trip.route_source == "hold"
    assert trip.route_geometry == [(trip.pickup_lat, trip.pickup_lon)]
    assert replay.stats_route_hold_fallback == 1


def test_mark_fleet_driver_available_uses_route_endpoint():
    replay = replay_main.TLCReplay("2024-01")
    replay.fleet_pool_enabled = True
    trip = _make_trip(trip_key="fleet_available_anchor")
    trip.route_geometry = [
        (40.7571, -73.9844),
        (40.7500, -73.9790),
        (40.7415, -73.9722),
    ]
    trip.route_cumulative_km = replay_main.cumulative_route_distances_km(trip.route_geometry)

    replay._mark_fleet_driver_available(trip)

    state = replay.fleet_driver_state[str(trip.courier_id)]
    assert state[1] == pytest.approx(40.7415, abs=1e-6)
    assert state[2] == pytest.approx(-73.9722, abs=1e-6)


def test_prepare_fleet_reposition_uses_previous_driver_position(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    replay.fleet_pool_enabled = True
    trip = _make_trip(trip_key="fleet_reposition_prepare")
    trip.courier_id = "drv_demo_001"
    replay.fleet_driver_state = {
        "drv_demo_001": (
            trip.request_ts - timedelta(minutes=5),
            40.7485,
            -74.0020,
            161,
        )
    }

    async def _fake_osrm(*_args, **_kwargs):
        return "osrm_public", [
            (40.7485, -74.0020),
            (40.7520, -73.9940),
            (trip.pickup_lat, trip.pickup_lon),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    replay.osrm_client = object()
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_osrm)

    asyncio.run(replay._prepare_fleet_reposition(trip))

    assert trip.reposition_start_lat == 40.7485
    assert trip.reposition_start_lon == -74.0020
    assert trip.reposition_source == "osrm_public"
    assert trip.reposition_geometry[0] == (40.7485, -74.0020)
    assert trip.reposition_geometry[-1] == (trip.pickup_lat, trip.pickup_lon)


def test_prepare_fleet_reposition_uses_snapped_route_origin(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    replay.fleet_pool_enabled = True
    trip = _make_trip(trip_key="fleet_reposition_snap_origin")
    trip.courier_id = "drv_demo_001"
    replay.fleet_driver_state = {
        "drv_demo_001": (
            trip.request_ts - timedelta(minutes=5),
            40.7485,
            -74.0020,
            161,
        )
    }

    async def _fake_osrm(*_args, **_kwargs):
        return "osrm_public", [
            (40.7489, -74.0016),
            (40.7520, -73.9940),
            (trip.pickup_lat, trip.pickup_lon),
        ]

    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    replay.osrm_client = object()
    monkeypatch.setattr(replay, "_fetch_osrm_geometry", _fake_osrm)

    asyncio.run(replay._prepare_fleet_reposition(trip))

    assert trip.reposition_start_lat == pytest.approx(40.7489, abs=1e-6)
    assert trip.reposition_start_lon == pytest.approx(-74.0016, abs=1e-6)


def test_emit_trip_start_uses_reposition_origin_for_fleet_driver(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="fleet_reposition_start")
    trip.courier_id = "drv_demo_001"
    trip.reposition_start_ts = trip.request_ts
    trip.reposition_start_lat = 40.7485
    trip.reposition_start_lon = -74.0020
    trip.reposition_source = "osrm_public"
    trip.reposition_geometry = [
        (40.7485, -74.0020),
        (40.7520, -73.9940),
        (trip.pickup_lat, trip.pickup_lon),
    ]
    trip.reposition_cumulative_km = replay_main.cumulative_route_distances_km(trip.reposition_geometry)

    sent: list[bytes] = []

    async def _capture(_topic, _key, value):  # noqa: ANN001
        sent.append(value)

    monkeypatch.setattr(replay, "_send", _capture)
    asyncio.run(replay._emit_trip_start(trip))

    pos = replay_main.CourierPositionV1()
    pos.ParseFromString(sent[-1])

    assert pos.status == "repositioning"
    assert pos.lat == round(trip.reposition_start_lat, 6)
    assert pos.lon == round(trip.reposition_start_lon, 6)


def test_emit_trip_progress_uses_reposition_route_before_pickup(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="fleet_reposition_progress")
    trip.reposition_start_ts = trip.request_ts
    trip.reposition_start_lat = 40.7485
    trip.reposition_start_lon = -74.0020
    trip.reposition_source = "osrm_public"
    trip.reposition_geometry = [
        (40.7485, -74.0020),
        (40.7520, -73.9940),
        (trip.pickup_lat, trip.pickup_lon),
    ]
    trip.reposition_cumulative_km = replay_main.cumulative_route_distances_km(trip.reposition_geometry)

    sent: list[bytes] = []

    async def _capture(_topic, _key, value):  # noqa: ANN001
        sent.append(value)

    monkeypatch.setattr(replay, "_send", _capture)
    mid = trip.request_ts + (trip.pickup_ts - trip.request_ts) / 2
    asyncio.run(replay._emit_trip_progress(trip, mid))

    pos = replay_main.CourierPositionV1()
    pos.ParseFromString(sent[-1])

    assert pos.status == "repositioning"
    assert round(pos.lat, 4) != round(trip.pickup_lat, 4)
    assert round(pos.lon, 4) != round(trip.pickup_lon, 4)
    assert round(pos.lat, 4) != round(trip.dropoff_lat, 4)


def test_hold_reposition_is_not_reanchored_to_pickup():
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="blocked_reposition_alignment")
    origin = (40.7060, -74.0090)
    trip.reposition_start_lat = origin[0]
    trip.reposition_start_lon = origin[1]
    trip.reposition_start_zone_id = 231
    trip.reposition_source = "hold"
    trip.reposition_geometry = [origin]
    trip.reposition_cumulative_km = [0.0]
    trip_route = [
        (trip.pickup_lat, trip.pickup_lon),
        (40.7500, -73.9790),
        (trip.dropoff_lat, trip.dropoff_lon),
    ]

    replay._apply_trip_route(
        trip,
        "osrm_public",
        trip_route,
        replay_main.cumulative_route_distances_km(trip_route),
    )

    assert trip.reposition_blocked is True
    assert trip.reposition_start_lat == pytest.approx(origin[0], abs=1e-6)
    assert trip.reposition_start_lon == pytest.approx(origin[1], abs=1e-6)
    assert trip.reposition_geometry == [origin]
    assert replay._trip_needs_reposition(trip) is True


def test_blocked_reposition_keeps_driver_at_last_known_point(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="blocked_reposition_motion")
    origin = (40.7060, -74.0090)
    trip.reposition_start_lat = origin[0]
    trip.reposition_start_lon = origin[1]
    trip.reposition_start_zone_id = 231
    trip.reposition_source = "hold"
    trip.reposition_geometry = [origin]
    trip.reposition_cumulative_km = [0.0]
    trip_route = [
        (trip.pickup_lat, trip.pickup_lon),
        (40.7500, -73.9790),
        (trip.dropoff_lat, trip.dropoff_lon),
    ]
    replay._apply_trip_route(
        trip,
        "osrm_public",
        trip_route,
        replay_main.cumulative_route_distances_km(trip_route),
    )

    sent: list[tuple[str, bytes]] = []

    async def _capture(topic, _key, value):  # noqa: ANN001
        sent.append((topic, value))

    monkeypatch.setattr(replay, "_send", _capture)
    asyncio.run(replay._emit_trip_start(trip))
    asyncio.run(replay._emit_trip_progress(trip, trip.pickup_ts + timedelta(minutes=4)))
    asyncio.run(replay._emit_trip_finish(trip))

    courier_positions = []
    for topic, raw in sent:
        if topic != replay_main.COURIER_TOPIC:
            continue
        pos = replay_main.CourierPositionV1()
        pos.ParseFromString(raw)
        courier_positions.append(pos)

    assert [pos.status for pos in courier_positions] == [
        "repositioning",
        "repositioning",
        "available",
    ]
    assert all(pos.lat == pytest.approx(origin[0], abs=1e-6) for pos in courier_positions)
    assert all(pos.lon == pytest.approx(origin[1], abs=1e-6) for pos in courier_positions)
    assert all(pos.speed_kmh == 0.0 for pos in courier_positions)
    assert all(pos.source_platform.endswith("|route=hold") for pos in courier_positions)


def test_blocked_reposition_keeps_fleet_state_at_origin():
    replay = replay_main.TLCReplay("2024-01")
    replay.fleet_pool_enabled = True
    trip = _make_trip(trip_key="blocked_reposition_state")
    trip.courier_id = "drv_demo_001"
    trip.reposition_start_lat = 40.7060
    trip.reposition_start_lon = -74.0090
    trip.reposition_start_zone_id = 231
    trip.reposition_source = "hold"
    trip.reposition_blocked = True

    replay._mark_fleet_driver_available(trip)

    state = replay.fleet_driver_state[str(trip.courier_id)]
    assert state[1] == pytest.approx(40.7060, abs=1e-6)
    assert state[2] == pytest.approx(-74.0090, abs=1e-6)
    assert state[3] == 231


def test_emit_trip_progress_emits_dense_samples_along_geometry(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="dense_progress_case")
    trip.route_geometry = [
        (40.7580, -73.9855),
        (40.7580, -73.9820),
        (40.7550, -73.9820),
    ]
    trip.route_cumulative_km = replay_main.cumulative_route_distances_km(trip.route_geometry)
    trip.last_position_ts = trip.pickup_ts

    sent: list[bytes] = []

    async def _capture(_topic, _key, value):  # noqa: ANN001
        sent.append(value)

    monkeypatch.setattr(replay_main, "TLC_LIVE_ROUTE_MAX_STEP_KM", 0.05)
    monkeypatch.setattr(replay_main, "TLC_LIVE_ROUTE_MAX_POINTS_PER_TICK", 16)
    monkeypatch.setattr(replay, "_send", _capture)

    now = trip.pickup_ts + timedelta(minutes=8)
    asyncio.run(replay._emit_trip_progress(trip, now))

    decoded = []
    for raw in sent:
        pos = replay_main.CourierPositionV1()
        pos.ParseFromString(raw)
        decoded.append(pos)

    assert len(decoded) >= 3
    assert all(pos.status == "delivering" for pos in decoded)
    assert any(abs(pos.lon - (-73.9820)) < 0.0008 for pos in decoded)
    assert any(abs(pos.lat - 40.7580) < 0.0008 for pos in decoded)
    assert decoded[-1].ts == replay_main.iso_from_dt(now)


def test_emit_progress_phase_zeroes_speed_when_route_holds_position(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="held_progress_case")
    trip.route_geometry = [
        (trip.pickup_lat, trip.pickup_lon),
        (trip.pickup_lat, trip.pickup_lon),
    ]
    trip.route_cumulative_km = replay_main.cumulative_route_distances_km(trip.route_geometry)
    trip.last_position_ts = trip.pickup_ts
    trip.last_position_lat = trip.pickup_lat
    trip.last_position_lon = trip.pickup_lon

    sent: list[bytes] = []

    async def _capture(_topic, _key, value):  # noqa: ANN001
        sent.append(value)

    monkeypatch.setattr(replay, "_send", _capture)

    asyncio.run(
        replay._emit_progress_phase(
            trip,
            phase_start_ts=trip.pickup_ts,
            phase_end_ts=trip.dropoff_ts,
            segment_start_ts=trip.pickup_ts,
            segment_end_ts=trip.pickup_ts + timedelta(minutes=2),
            geometry=trip.route_geometry,
            cumulative_km=trip.route_cumulative_km,
            status="delivering",
        )
    )

    assert sent
    pos = replay_main.CourierPositionV1()
    pos.ParseFromString(sent[-1])
    assert pos.status == "delivering"
    assert pos.speed_kmh == 0.0


def test_emit_progress_phase_inserts_stoplight_pause_without_large_jumps(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    trip = _make_trip(trip_key="urban_pause_case")
    trip.route_geometry = [
        (40.7580, -73.9855),
        (40.7580, -73.9810),
        (40.7525, -73.9810),
        (40.7525, -73.9765),
    ]
    trip.route_cumulative_km = replay_main.cumulative_route_distances_km(trip.route_geometry)
    trip.last_position_ts = trip.pickup_ts
    trip.last_position_lat = trip.pickup_lat
    trip.last_position_lon = trip.pickup_lon

    sent: list[bytes] = []

    async def _capture(_topic, _key, value):  # noqa: ANN001
        sent.append(value)

    monkeypatch.setattr(replay_main, "TLC_COURIER_SIGNAL_BASE_STOP_SECONDS", 24.0)
    monkeypatch.setattr(replay_main, "TLC_COURIER_SIGNAL_MIN_SPACING_KM", 0.05)
    monkeypatch.setattr(replay_main, "TLC_COURIER_SIGNAL_MAX_PAUSE_RATIO", 0.4)
    monkeypatch.setattr(replay_main, "TLC_LIVE_ROUTE_MAX_POINTS_PER_TICK", 24)
    monkeypatch.setattr(replay, "_send", _capture)

    asyncio.run(
        replay._emit_progress_phase(
            trip,
            phase_start_ts=trip.pickup_ts,
            phase_end_ts=trip.pickup_ts + timedelta(minutes=4),
            segment_start_ts=trip.pickup_ts,
            segment_end_ts=trip.pickup_ts + timedelta(minutes=4),
            geometry=trip.route_geometry,
            cumulative_km=trip.route_cumulative_km,
            status="delivering",
        )
    )

    decoded = []
    for raw in sent:
        pos = replay_main.CourierPositionV1()
        pos.ParseFromString(raw)
        decoded.append(pos)

    assert decoded
    assert any(pos.speed_kmh == 0.0 for pos in decoded)
    max_step_km = 0.0
    for previous, current in zip(decoded, decoded[1:]):
        max_step_km = max(
            max_step_km,
            replay_main.haversine_km(previous.lat, previous.lon, current.lat, current.lon),
        )
    assert max_step_km < 0.12


def test_prefer_working_route_provider_moves_public_first_when_local_down():
    replay = replay_main.TLCReplay("2024-01")
    replay.route_osrm_targets = [
        ("osrm", "http://osrm:5000"),
        ("osrm_public", "https://router.project-osrm.org"),
    ]

    class _MixedClient:
        async def get(self, url: str, *_args, **_kwargs):  # noqa: ANN001
            if "osrm:5000" in url:
                raise RuntimeError("local_osrm_down")

            class _OkResponse:
                status_code = 200

                def json(self):  # noqa: ANN201
                    return {"code": "Ok", "waypoints": [{"location": [-73.9855, 40.758]}]}

            return _OkResponse()

    replay.osrm_client = _MixedClient()
    asyncio.run(replay._prefer_working_route_provider())
    assert replay.route_osrm_targets[0][0] == "osrm_public"


def test_routing_preflight_requires_local_osrm_dataset(monkeypatch, tmp_path):
    replay = replay_main.TLCReplay("2024-01")
    missing = tmp_path / "missing.osrm"
    monkeypatch.setattr(replay_main, "TLC_ROUTE_MODE", "osrm")
    monkeypatch.setattr(replay_main, "TLC_REQUIRE_LOCAL_OSRM_DATA", True)
    monkeypatch.setattr(replay_main, "TLC_ROUTE_SYNC_ON_CACHE_MISS", True)
    monkeypatch.setattr(replay_main, "TLC_LOCAL_OSRM_DATA_FILE", missing)

    with pytest.raises(RuntimeError):
        replay._routing_preflight()
