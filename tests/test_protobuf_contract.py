from schemas.gen.copilot_events_pb2 import ContextSignalV1, CourierPositionV1, OrderEventV1, OrderOfferV1


def test_protobuf_roundtrip_position():
    msg = CourierPositionV1(
        event_id="pos_1",
        event_type="courier.position.v1",
        ts="2026-04-11T10:00:00Z",
        courier_id="L001",
        lat=40.7580,
        lon=-73.9855,
        speed_kmh=21.5,
        heading_deg=70.0,
        status="available",
        accuracy_m=4.0,
        battery_pct=86.0,
    )
    encoded = msg.SerializeToString()

    decoded = CourierPositionV1()
    decoded.ParseFromString(encoded)

    assert decoded.courier_id == "L001"
    assert decoded.event_type == "courier.position.v1"


def test_protobuf_roundtrip_offer():
    msg = OrderOfferV1(
        event_id="offer_1",
        event_type="order.offer.v1",
        ts="2026-04-11T10:00:01Z",
        offer_id="OFR01",
        courier_id="L001",
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7484,
        dropoff_lon=-73.9857,
        estimated_fare_eur=12.2,
        estimated_distance_km=3.4,
        estimated_duration_min=18.0,
        demand_index=1.2,
        weather_factor=1.0,
        traffic_factor=1.1,
        zone_id="nyc_142",
    )

    encoded = msg.SerializeToString()
    decoded = OrderOfferV1()
    decoded.ParseFromString(encoded)

    assert decoded.offer_id == "OFR01"
    assert decoded.estimated_fare_eur > 0


def test_protobuf_roundtrip_order_event():
    msg = OrderEventV1(
        event_id="evt_1",
        event_type="order.event.v1",
        ts="2026-04-11T10:00:02Z",
        offer_id="OFR01",
        order_id="ORD01",
        courier_id="L001",
        status="accepted",
        zone_id="nyc_142",
        actual_fare_eur=12.2,
        actual_distance_km=3.4,
        actual_duration_min=17.0,
    )

    encoded = msg.SerializeToString()
    decoded = OrderEventV1()
    decoded.ParseFromString(encoded)

    assert decoded.order_id == "ORD01"
    assert decoded.status == "accepted"


def test_protobuf_roundtrip_context_signal():
    msg = ContextSignalV1(
        event_id="ctx_1",
        event_type="context.signal.v1",
        ts="2026-04-11T10:00:03Z",
        zone_id="nyc_142",
        demand_index=1.4,
        supply_index=0.8,
        weather_factor=1.0,
        traffic_factor=1.1,
        source="simulator",
    )

    encoded = msg.SerializeToString()
    decoded = ContextSignalV1()
    decoded.ParseFromString(encoded)

    assert decoded.zone_id == "nyc_142"
    assert decoded.source == "simulator"
