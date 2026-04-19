from __future__ import annotations

import sys
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from api.copilot_router import _zone_context_payload  # noqa: E402


def test_zone_context_payload_exposes_context_v2_fields():
    payload = _zone_context_payload(
        {
            "zone_id": "nyc_142",
            "demand_index": "1.42",
            "supply_index": "0.93",
            "weather_factor": "1.08",
            "traffic_factor": "1.14",
            "event_pressure": "0.23",
            "event_count_nearby": "3",
            "weather_precip_mm": "2.5",
            "weather_wind_kmh": "16.0",
            "weather_intensity": "0.37",
            "temporal_hour_local": "17.5",
            "is_peak_hour": "1",
            "is_weekend": "0",
            "is_holiday": "0",
            "temporal_pressure": "0.14",
            "context_fallback_applied": "1",
            "context_stale_sources": "2",
            "freshness_policy": "stale_neutral_v1",
            "age_weather_s": "52.0",
            "stale_weather": "1",
        }
    )
    assert payload["zone_id"] == "nyc_142"
    assert payload["event_count_nearby"] == 3
    assert payload["weather_precip_mm"] == 2.5
    assert payload["is_peak_hour"] is True
    assert payload["temporal_pressure"] == 0.14
    assert payload["context_fallback_applied"] is True
    assert payload["context_stale_sources"] == 2
    assert payload["freshness_policy"] == "stale_neutral_v1"
    assert payload["age_weather_s"] == 52.0
    assert payload["stale_weather"] is True


def test_zone_context_payload_uses_defaults_and_hint():
    payload = _zone_context_payload({}, zone_id_hint="nyc_hint")
    assert payload["zone_id"] == "nyc_hint"
    assert payload["demand_index"] == 1.0
    assert payload["supply_index"] == 1.0
    assert payload["event_count_nearby"] == 0
    assert payload["context_fallback_applied"] is False
    assert payload["context_stale_sources"] == 0
