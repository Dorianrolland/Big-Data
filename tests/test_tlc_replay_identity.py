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
