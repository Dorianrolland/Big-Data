"""Non-regression tests for synthetic courier identity generation."""
from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path


_TLC_MAIN_PATH = Path(__file__).resolve().parent.parent / "tlc_replay" / "main.py"
_SPEC = importlib.util.spec_from_file_location("tlc_replay_main_identity", _TLC_MAIN_PATH)
assert _SPEC and _SPEC.loader
replay_main = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(replay_main)


def _trip_row(*, req: datetime, pickup: datetime, dropoff: datetime, pu: int, do: int) -> dict:
    return {
        "dispatching_base_num": "B02764",
        "request_datetime": req,
        "pickup_datetime": pickup,
        "dropoff_datetime": dropoff,
        "PULocationID": pu,
        "DOLocationID": do,
        "trip_miles": 2.5,
        "trip_time": 780,
        "base_passenger_fare": 12.5,
        "tolls": 0.0,
        "bcf": 0.0,
        "sales_tax": 0.0,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "tips": 0.0,
        "driver_pay": 0.0,
    }


def test_synth_courier_id_trip_mode_varies_per_trip_instance():
    req_a = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    req_b = req_a + timedelta(minutes=7)
    pickup = req_a + timedelta(minutes=1)
    dropoff = pickup + timedelta(minutes=11)

    cid_a = replay_main.synth_courier_id(
        "B02764",
        132,
        238,
        req_a,
        pickup,
        dropoff,
        mode="trip",
    )
    cid_b = replay_main.synth_courier_id(
        "B02764",
        132,
        238,
        req_b,
        req_b + timedelta(minutes=1),
        req_b + timedelta(minutes=12),
        mode="trip",
    )
    assert cid_a != cid_b


def test_synth_courier_id_legacy_zone_day_keeps_historical_collision_behavior():
    req_a = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    req_b = req_a + timedelta(minutes=45)

    cid_a = replay_main.synth_courier_id(
        "B02764",
        132,
        238,
        req_a,
        req_a + timedelta(minutes=1),
        req_a + timedelta(minutes=12),
        mode="legacy_zone_day",
    )
    cid_b = replay_main.synth_courier_id(
        "B02764",
        132,
        116,
        req_b,
        req_b + timedelta(minutes=1),
        req_b + timedelta(minutes=18),
        mode="legacy_zone_day",
    )
    assert cid_a == cid_b


def test_synth_courier_id_fleet_pool_returns_stable_demo_driver_id(monkeypatch):
    monkeypatch.setattr(replay_main, "TLC_FLEET_DEMO_N_DRIVERS", 360)
    req_a = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    req_b = req_a + timedelta(minutes=15)

    cid_a = replay_main.synth_courier_id(
        "B02764",
        132,
        238,
        req_a,
        req_a + timedelta(minutes=1),
        req_a + timedelta(minutes=12),
        mode="fleet_pool",
    )
    cid_b = replay_main.synth_courier_id(
        "B02764",
        132,
        238,
        req_b,
        req_b + timedelta(minutes=1),
        req_b + timedelta(minutes=12),
        mode="fleet_pool",
    )

    assert cid_a.startswith("drv_demo_")
    assert cid_b.startswith("drv_demo_")
    assert cid_a == cid_b


def test_row_to_trip_uses_trip_mode_to_avoid_overlapping_identity_collisions(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    monkeypatch.setattr(replay_main, "TLC_COURIER_ID_MODE", "trip")

    req = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    row_a = _trip_row(
        req=req,
        pickup=req + timedelta(minutes=1),
        dropoff=req + timedelta(minutes=14),
        pu=132,
        do=238,
    )
    row_b = _trip_row(
        req=req + timedelta(minutes=2),
        pickup=req + timedelta(minutes=3),
        dropoff=req + timedelta(minutes=16),
        pu=132,
        do=238,
    )

    trip_a = replay._row_to_trip(row_a)
    trip_b = replay._row_to_trip(row_b)

    assert trip_a is not None
    assert trip_b is not None
    assert trip_a.courier_id != trip_b.courier_id


def test_pick_fleet_driver_prefers_nearby_available_driver(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    monkeypatch.setattr(replay_main, "TLC_COURIER_ID_MODE", "fleet_pool")
    monkeypatch.setattr(replay_main, "TLC_FLEET_DEMO_N_DRIVERS", 2)
    monkeypatch.setattr(replay_main, "TLC_FLEET_REPOSITION_KMH", 22.0)
    monkeypatch.setattr(replay_main, "TLC_FLEET_MAX_REPOSITION_MIN", 30.0)
    monkeypatch.setattr(replay_main, "TLC_FLEET_PICKUP_GRACE_MIN", 2.0)
    replay.fleet_pool_enabled = True
    replay.fleet_driver_ids = ["drv_demo_001", "drv_demo_002"]
    base_ts = datetime(2024, 1, 2, 11, 55, tzinfo=timezone.utc)
    replay.fleet_driver_state = {
        "drv_demo_001": (base_ts, 40.7579, -73.9854),  # near pickup
        "drv_demo_002": (base_ts, 40.6892, -74.0445),  # far away
    }

    trip = replay_main.Trip(
        trip_key="fleet_pick",
        courier_id="drv_demo_002",
        offer_id="offer_fleet_pick",
        order_id="order_fleet_pick",
        request_ts=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
        pickup_ts=datetime(2024, 1, 2, 12, 1, tzinfo=timezone.utc),
        dropoff_ts=datetime(2024, 1, 2, 12, 12, tzinfo=timezone.utc),
        pu_loc=132,
        do_loc=238,
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7420,
        dropoff_lon=-73.9730,
        trip_km=3.2,
        trip_min=12.0,
        fare_usd=12.0,
    )

    picked = replay._pick_fleet_driver(trip)
    assert picked == "drv_demo_001"


def test_pick_fleet_driver_returns_none_when_pickup_is_unreachable(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    monkeypatch.setattr(replay_main, "TLC_COURIER_ID_MODE", "fleet_pool")
    monkeypatch.setattr(replay_main, "TLC_FLEET_DEMO_N_DRIVERS", 1)
    monkeypatch.setattr(replay_main, "TLC_FLEET_REPOSITION_KMH", 18.0)
    monkeypatch.setattr(replay_main, "TLC_FLEET_MAX_REPOSITION_MIN", 20.0)
    monkeypatch.setattr(replay_main, "TLC_FLEET_PICKUP_GRACE_MIN", 1.0)
    replay.fleet_pool_enabled = True
    replay.fleet_driver_ids = ["drv_demo_001"]
    base_ts = datetime(2024, 1, 2, 11, 59, tzinfo=timezone.utc)
    replay.fleet_driver_state = {
        "drv_demo_001": (base_ts, 40.6413, -73.7781),  # JFK -> too far for a 1 min pickup window
    }

    trip = replay_main.Trip(
        trip_key="fleet_unreachable",
        courier_id="drv_demo_001",
        offer_id="offer_fleet_unreachable",
        order_id="order_fleet_unreachable",
        request_ts=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
        pickup_ts=datetime(2024, 1, 2, 12, 1, tzinfo=timezone.utc),
        dropoff_ts=datetime(2024, 1, 2, 12, 16, tzinfo=timezone.utc),
        pu_loc=132,
        do_loc=238,
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7420,
        dropoff_lon=-73.9730,
        trip_km=3.2,
        trip_min=12.0,
        fare_usd=12.0,
    )

    assert replay._pick_fleet_driver(trip) is None


def test_pick_fleet_driver_prefers_route_aware_same_side_candidate_over_cross_river_shortcut(monkeypatch):
    replay = replay_main.TLCReplay("2024-01")
    monkeypatch.setattr(replay_main, "TLC_COURIER_ID_MODE", "fleet_pool")
    monkeypatch.setattr(replay_main, "TLC_FLEET_DEMO_N_DRIVERS", 2)
    monkeypatch.setattr(replay_main, "TLC_FLEET_REPOSITION_KMH", 20.0)
    monkeypatch.setattr(replay_main, "TLC_FLEET_MAX_REPOSITION_MIN", 30.0)
    monkeypatch.setattr(replay_main, "TLC_FLEET_PICKUP_GRACE_MIN", 5.0)
    replay.fleet_pool_enabled = True
    replay.fleet_driver_ids = ["drv_demo_001", "drv_demo_002"]
    base_ts = datetime(2024, 1, 2, 11, 55, tzinfo=timezone.utc)
    replay.zone_road_anchors = {
        10: (40.7130, -73.9620),   # east of river
        11: (40.7200, -73.9820),   # same Manhattan side
        12: (40.7160, -73.9720),   # pickup zone
    }
    replay.fleet_driver_state = {
        "drv_demo_001": (base_ts, 40.7130, -73.9620, 10),
        "drv_demo_002": (base_ts, 40.7200, -73.9820, 11),
    }

    trip = replay_main.Trip(
        trip_key="fleet_route_aware_cross_river",
        courier_id="drv_demo_999",
        offer_id="offer_fleet_route_aware_cross_river",
        order_id="order_fleet_route_aware_cross_river",
        request_ts=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
        pickup_ts=datetime(2024, 1, 2, 12, 8, tzinfo=timezone.utc),
        dropoff_ts=datetime(2024, 1, 2, 12, 20, tzinfo=timezone.utc),
        pu_loc=12,
        do_loc=238,
        pickup_lat=40.7160,
        pickup_lon=-73.9720,
        dropoff_lat=40.7420,
        dropoff_lon=-73.9730,
        trip_km=3.2,
        trip_min=12.0,
        fare_usd=12.0,
    )

    picked = replay._pick_fleet_driver(trip)
    assert picked == "drv_demo_002"
