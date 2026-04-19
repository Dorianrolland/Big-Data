from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import sys

_FEATURES_DIR = Path(__file__).resolve().parent.parent / "copilot_features"
if str(_FEATURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FEATURES_DIR))
_FEATURES_MAIN = _FEATURES_DIR / "main.py"
_FEATURES_SPEC = importlib.util.spec_from_file_location("copilot_features_main", _FEATURES_MAIN)
assert _FEATURES_SPEC and _FEATURES_SPEC.loader
features = importlib.util.module_from_spec(_FEATURES_SPEC)
sys.modules.setdefault("copilot_features_main", features)
_FEATURES_SPEC.loader.exec_module(features)  # type: ignore[attr-defined]  # noqa: E402


class _FakeRedis:
    def __init__(self) -> None:
        self.hmget_store: dict[str, dict[str, str]] = {}
        self.hset_calls: list[tuple[str, dict[str, object]]] = []
        self.expire_calls: list[tuple[str, int]] = []

    async def hmget(self, key: str, fields: list[str]):  # noqa: ANN001
        raw = self.hmget_store.get(key, {})
        return [raw.get(field) for field in fields]

    async def hset(self, key: str, mapping: dict[str, object]):  # noqa: ANN001
        self.hset_calls.append((key, dict(mapping)))
        current = self.hmget_store.setdefault(key, {})
        for mk, mv in mapping.items():
            current[str(mk)] = str(mv)

    async def expire(self, key: str, seconds: int):  # noqa: ANN001
        self.expire_calls.append((key, seconds))


class _FakeRedisEnrich:
    def __init__(self, rows: dict[str, dict[str, str]]):
        self.rows = rows

    async def hgetall(self, key: str):  # noqa: ANN001
        return dict(self.rows.get(key, {}))


def test_parse_source_event_pressure_defaults_to_zero():
    source, pressure = features._parse_source_event_pressure("gbfs+open-meteo")
    assert source == "gbfs+open-meteo"
    assert pressure == 0.0


def test_parse_source_event_pressure_extracts_value():
    source, pressure = features._parse_source_event_pressure("gbfs+nyc-events;event_pressure=0.2841")
    assert source == "gbfs+nyc-events"
    assert pressure == 0.2841


def test_parse_source_event_pressure_handles_extra_metadata_and_clamps():
    source, pressure = features._parse_source_event_pressure(
        "gbfs+nyc-events;event_pressure=9.7;events_rows=48;status=ok"
    )
    assert source == "gbfs+nyc-events"
    assert pressure == 2.0


def test_parse_source_event_pressure_invalid_number_falls_back_to_zero():
    source, pressure = features._parse_source_event_pressure("gbfs+nyc-events;event_pressure=not-a-number;foo=bar")
    assert source == "gbfs+nyc-events"
    assert pressure == 0.0


def test_parse_source_metadata_extracts_temporal_and_freshness_fields():
    source, metadata = features._parse_source_metadata(
        "gbfs+open-meteo;event_pressure=0.18;temporal_is_peak=1;freshness_policy=stale_neutral_v1"
    )
    assert source == "gbfs+open-meteo"
    assert metadata["event_pressure"] == "0.18"
    assert metadata["temporal_is_peak"] == "1"
    assert metadata["freshness_policy"] == "stale_neutral_v1"


def test_parse_source_metadata_legacy_source_without_metadata():
    source, metadata = features._parse_source_metadata("gbfs+open-meteo+nyc311")
    assert source == "gbfs+open-meteo+nyc311"
    assert metadata == {}


def test_upsert_context_persists_event_pressure():
    signal = features.ContextSignalV1(
        event_id="ctx_1",
        event_type="context.signal.v1",
        ts="2026-04-18T10:00:00Z",
        zone_id="nyc_142",
        demand_index=1.55,
        supply_index=0.95,
        weather_factor=1.02,
        traffic_factor=1.1,
        source=(
            "gbfs+open-meteo+nyc311+nyc-events;"
            "event_pressure=0.2712;"
            "event_count_nearby=3;"
            "weather_precip_mm=2.3;"
            "weather_wind_kmh=17.4;"
            "weather_intensity=0.38;"
            "temporal_hour_local=8.5;"
            "temporal_is_peak=1;"
            "temporal_is_weekend=0;"
            "temporal_is_holiday=0;"
            "temporal_pressure=0.14;"
            "freshness_policy=stale_neutral_v1;"
            "freshness_fallback_applied=1;"
            "freshness_stale_sources=2;"
            "age_weather_s=52.0"
        ),
    )
    redis = _FakeRedis()

    asyncio.run(features.upsert_context(redis, signal))

    assert redis.hset_calls
    key, payload = redis.hset_calls[-1]
    assert key.endswith("nyc_142")
    assert payload["source"] == "gbfs+open-meteo+nyc311+nyc-events"
    assert float(payload["event_pressure"]) == 0.2712
    assert int(payload["event_count_nearby"]) == 3
    assert float(payload["weather_precip_mm"]) == 2.3
    assert int(payload["is_peak_hour"]) == 1
    assert payload["freshness_policy"] == "stale_neutral_v1"
    assert int(payload["context_fallback_applied"]) == 1
    assert int(payload["context_stale_sources"]) == 2


def test_enrich_offer_surfaces_event_pressure():
    redis = _FakeRedisEnrich(
        {
            f"{features.COURIER_HASH_PREFIX}drv_demo_001": {
                "lat": "40.751",
                "lon": "-73.990",
                "speed_kmh": "18.0",
                "status": "available",
            },
            f"{features.ZONE_CONTEXT_PREFIX}nyc_142": {
                "demand_index": "1.40",
                "supply_index": "1.00",
                "weather_factor": "1.05",
                "traffic_factor": "1.10",
                "event_pressure": "0.3375",
                "event_count_nearby": "4",
                "weather_precip_mm": "1.7",
                "weather_wind_kmh": "21.0",
                "weather_intensity": "0.41",
                "temporal_hour_local": "17.2",
                "is_peak_hour": "1",
                "is_weekend": "0",
                "is_holiday": "0",
                "temporal_pressure": "0.14",
                "context_fallback_applied": "0",
                "context_stale_sources": "1",
                "freshness_policy": "stale_neutral_v1",
                "gbfs_demand_boost": "0.20",
            },
        }
    )
    offer = features.OrderOfferV1(
        event_id="off_evt_1",
        event_type="order.offer.v1",
        ts="2026-04-18T12:00:00Z",
        offer_id="off_1",
        courier_id="drv_demo_001",
        zone_id="nyc_142",
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7440,
        dropoff_lon=-73.9900,
        estimated_fare_eur=22.0,
        estimated_distance_km=5.1,
        estimated_duration_min=18.0,
        demand_index=1.0,
        weather_factor=1.0,
        traffic_factor=1.0,
    )

    enriched = asyncio.run(features.enrich_offer(redis, offer))

    assert enriched["event_pressure"] == 0.3375
    assert enriched["event_count_nearby"] == 4
    assert enriched["weather_precip_mm"] == 1.7
    assert enriched["is_peak_hour"] == 1
    assert enriched["freshness_policy"] == "stale_neutral_v1"
    assert enriched["demand_index"] == 1.6


def test_enrich_offer_handles_invalid_zone_numeric_values():
    redis = _FakeRedisEnrich(
        {
            f"{features.COURIER_HASH_PREFIX}drv_demo_002": {
                "lat": "40.751",
                "lon": "-73.990",
                "speed_kmh": "18.0",
                "status": "available",
            },
            f"{features.ZONE_CONTEXT_PREFIX}nyc_144": {
                "demand_index": "invalid",
                "supply_index": "bad",
                "weather_factor": "NaN",
                "traffic_factor": "NaN",
                "event_pressure": "not-a-number",
                "gbfs_demand_boost": "-5",
            },
        }
    )
    offer = features.OrderOfferV1(
        event_id="off_evt_2",
        event_type="order.offer.v1",
        ts="2026-04-18T12:15:00Z",
        offer_id="off_2",
        courier_id="drv_demo_002",
        zone_id="nyc_144",
        pickup_lat=40.7580,
        pickup_lon=-73.9855,
        dropoff_lat=40.7440,
        dropoff_lon=-73.9900,
        estimated_fare_eur=18.0,
        estimated_distance_km=4.5,
        estimated_duration_min=16.0,
        demand_index=1.2,
        weather_factor=1.0,
        traffic_factor=1.0,
    )

    enriched = asyncio.run(features.enrich_offer(redis, offer))

    assert enriched["event_pressure"] == 0.0
    assert enriched["demand_index"] == 1.2
    assert enriched["supply_index"] == 1.0
