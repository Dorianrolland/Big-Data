from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
import sys

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import copilot_router as router  # noqa: E402


class _FakeRedis:
    def __init__(self, payloads: dict[str, dict[str, str]]):
        self._payloads = payloads

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._payloads.get(key, {}))


def test_health_exposes_events_source_status():
    redis_payloads = {
        router.WEATHER_KEY: {},
        router.GBFS_KEY: {},
        router.IRVE_KEY: {},
        router.EVENTS_CONTEXT_KEY: {
            "source": "nyc_permitted_event_information_tvpp-9vvx",
            "status": "ok",
            "events_upcoming_count": "18",
            "updated_at": "2026-04-18T10:00:00+00:00",
        },
        router.FUEL_CONTEXT_KEY: {},
        router.CONTEXT_QUALITY_KEY: {
            "events_source_active": "1",
            "events_status": "ok",
            "events_rows": "18",
            "event_pressure_mean": "0.1423",
            "event_pressure_max": "0.6312",
            "event_pressure_nonzero_rate": "0.2738",
            "dot_advisory_status": "ok",
            "dot_advisory_rows": "44",
            "dot_speeds_status": "ok",
            "dot_speeds_rows": "321",
            "closure_pressure_mean": "0.1142",
            "closure_pressure_max": "0.3711",
            "speed_pressure_mean": "0.0834",
            "speed_pressure_max": "0.2942",
            "freshness_policy": "stale_neutral_v1",
            "context_fallback_applied": "1",
            "stale_sources_count": "2",
            "age_weather_s": "52.0",
            "stale_weather": "1",
        },
        router.TLC_REPLAY_KEY: {},
        router.SINGLE_REPLAY_STATUS_KEY: {},
    }
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=_FakeRedis(redis_payloads),
                copilot_model=None,
                copilot_model_quality_gate={},
            )
        )
    )

    payload = asyncio.run(router.copilot_health(request))
    assert payload["events_context"]["source"] == "nyc_permitted_event_information_tvpp-9vvx"
    assert payload["events_context"]["status"] == "ok"
    assert payload["events_context"]["events_upcoming_count"] == 18
    assert payload["data_quality"]["events_source_active"] is True
    assert payload["data_quality"]["events_status"] == "ok"
    assert payload["data_quality"]["events_rows"] == 18
    assert payload["data_quality"]["event_pressure_max"] == 0.6312
    assert payload["data_quality"]["dot_advisory_status"] == "ok"
    assert payload["data_quality"]["dot_advisory_rows"] == 44
    assert payload["data_quality"]["dot_speeds_status"] == "ok"
    assert payload["data_quality"]["dot_speeds_rows"] == 321
    assert payload["data_quality"]["closure_pressure_max"] == 0.3711
    assert payload["data_quality"]["speed_pressure_max"] == 0.2942
    assert payload["data_quality"]["freshness_policy"] == "stale_neutral_v1"
    assert payload["data_quality"]["context_fallback_applied"] is True
    assert payload["data_quality"]["stale_sources_count"] == 2
    assert payload["data_quality"]["age_weather_s"] == 52.0
    assert payload["data_quality"]["stale_weather"] is True


def test_health_exposes_supply_flat_alert():
    redis_payloads = {
        router.WEATHER_KEY: {},
        router.GBFS_KEY: {},
        router.IRVE_KEY: {},
        router.EVENTS_CONTEXT_KEY: {},
        router.FUEL_CONTEXT_KEY: {},
        router.CONTEXT_QUALITY_KEY: {
            "supply_variance": "0.0001",
            "supply_flat_alert": "1",
            "traffic_nonzero_rate": "0.15",
        },
        router.TLC_REPLAY_KEY: {},
        router.SINGLE_REPLAY_STATUS_KEY: {},
    }
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=_FakeRedis(redis_payloads),
                copilot_model=None,
                copilot_model_quality_gate={},
            )
        )
    )
    payload = asyncio.run(router.copilot_health(request))
    assert payload["data_quality"]["supply_flat_alert"] is True
    assert payload["data_quality"]["supply_variance"] < router.QUALITY_ALERT_THRESHOLDS["supply_variance_min"]
    assert payload["data_quality"]["traffic_nonzero_rate"] < router.QUALITY_ALERT_THRESHOLDS["traffic_nonzero_rate_min"]
    assert "quality_alert_thresholds" in payload
    assert payload["quality_alert_thresholds"]["routing_success_rate_min"] == 0.80


def test_health_exposes_routing_quality_metrics():
    redis_payloads = {
        router.WEATHER_KEY: {},
        router.GBFS_KEY: {},
        router.IRVE_KEY: {},
        router.EVENTS_CONTEXT_KEY: {},
        router.FUEL_CONTEXT_KEY: {},
        router.CONTEXT_QUALITY_KEY: {},
        router.TLC_REPLAY_KEY: {},
        router.SINGLE_REPLAY_STATUS_KEY: {
            "route_requests": "100",
            "route_successes": "72",
            "hold_ticks": "40",
            "positions": "100",
            "routing_degraded": "1",
            "routing_last_error": "connection refused",
            "driver_id": "drv_demo_001",
            "state": "moving",
        },
    }
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=_FakeRedis(redis_payloads),
                copilot_model=None,
                copilot_model_quality_gate={},
            )
        )
    )
    payload = asyncio.run(router.copilot_health(request))
    rq = payload["routing_quality"]
    assert rq["routing_degraded"] is True
    assert rq["routing_last_error"] == "connection refused"
    assert rq["routing_success_rate"] == round(72 / 100, 4)
    assert rq["hold_rate"] == round(40 / 100, 4)
    assert rq["routing_success_rate"] < router.QUALITY_ALERT_THRESHOLDS["routing_success_rate_min"]
    assert rq["hold_rate"] > router.QUALITY_ALERT_THRESHOLDS["hold_rate_max"]
