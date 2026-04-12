import json
from datetime import datetime

import pytest
from schemas.gen.copilot_events_pb2 import ContextSignalV1, OrderEventV1, OrderOfferV1

cold = pytest.importorskip("cold_path.main")


def test_parse_offer_event_contract():
    msg = OrderOfferV1(
        event_id="offer_1",
        event_type="order.offer.v1",
        ts="2026-04-11T10:00:00Z",
        offer_id="ofr_1",
        courier_id="L001",
        zone_id="nyc_142",
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7484,
        dropoff_lon=-73.9857,
        estimated_fare_eur=12.5,
        estimated_distance_km=3.2,
        estimated_duration_min=18,
        demand_index=1.2,
        weather_factor=1.0,
        traffic_factor=1.1,
    )
    rec = cold.parse_offer(msg.SerializeToString(), "order-offers-v1")

    assert rec["topic"] == "order-offers-v1"
    assert rec["event_type"] == "order.offer.v1"
    assert rec["offer_id"] == "ofr_1"
    assert rec["status"] == "offered"


def test_parse_order_event_contract():
    msg = OrderEventV1(
        event_id="evt_1",
        event_type="order.event.v1",
        ts="2026-04-11T10:00:10Z",
        offer_id="ofr_1",
        order_id="ord_1",
        courier_id="L001",
        status="accepted",
        zone_id="nyc_142",
        actual_fare_eur=10.5,
        actual_distance_km=2.9,
        actual_duration_min=16,
    )
    rec = cold.parse_order_event(msg.SerializeToString(), "order-events-v1")

    assert rec["topic"] == "order-events-v1"
    assert rec["event_type"] == "order.event.v1"
    assert rec["order_id"] == "ord_1"
    assert rec["status"] == "accepted"


def test_parse_context_signal_contract():
    msg = ContextSignalV1(
        event_id="ctx_1",
        event_type="context.signal.v1",
        ts="2026-04-11T10:00:05Z",
        zone_id="nyc_142",
        demand_index=1.7,
        supply_index=0.9,
        weather_factor=1.0,
        traffic_factor=1.2,
        source="sim",
    )
    rec = cold.parse_context(msg.SerializeToString(), "context-signals-v1")

    assert rec["event_type"] == "context.signal.v1"
    assert rec["zone_id"] == "nyc_142"
    assert rec["source"] == "sim"


def test_write_dlq_jsonl(tmp_path):
    original = cold.DLQ_PATH
    cold.DLQ_PATH = tmp_path
    try:
        cold.write_dlq_jsonl([("order-offers-v1", b"\\x00\\x01", "parse_error")])
    finally:
        cold.DLQ_PATH = original

    files = list(tmp_path.glob("cold-dlq-*.jsonl"))
    assert files

    payload = json.loads(files[0].read_text(encoding="utf-8").strip())
    assert payload["topic"] == "order-offers-v1"
    assert payload["reason"] == "parse_error"
    assert payload["raw_hex"]
    datetime.fromisoformat(payload["ts_rejected"])
    assert payload["ts_rejected"].endswith("+00:00")
