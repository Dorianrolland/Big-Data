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


def test_health_marks_single_driver_flat_supply_and_optional_dot_feed_as_demo_ready():
    redis_payloads = {
        router.WEATHER_KEY: {},
        router.GBFS_KEY: {},
        router.IRVE_KEY: {},
        router.EVENTS_CONTEXT_KEY: {},
        router.FUEL_CONTEXT_KEY: {},
        router.CONTEXT_QUALITY_KEY: {
            "supply_variance": "0.0001",
            "supply_flat_alert": "1",
            "context_fallback_applied": "1",
            "stale_sources_count": "1",
            "dot_speeds_status": "degraded",
            "dot_speeds_rows": "0",
            "stale_dot_speeds": "1",
        },
        router.TLC_REPLAY_KEY: {},
        router.SINGLE_REPLAY_STATUS_KEY: {
            "driver_id": "drv_demo_001",
            "state": "repositioning",
            "mode": "single_driver",
            "route_requests": "12",
            "route_successes": "12",
            "positions": "12",
        },
    }
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=_FakeRedis(redis_payloads),
                copilot_model={"feature_columns": []},
                copilot_model_quality_gate={"accepted": True, "reason": "ok"},
            )
        )
    )

    payload = asyncio.run(router.copilot_health(request))

    dq = payload["data_quality"]
    assert dq["supply_signal_mode"] == "single_driver_demo"
    assert dq["supply_flat_alert"] is True
    assert dq["supply_flat_effective"] is False
    assert dq["effective_stale_sources_count"] == 0
    assert dq["status"] == "ok"
    assert "nyc_dot_speeds" in dq["optional_sources"]
    assert payload["demo_readiness"]["ready"] is True
    assert payload["demo_readiness"]["status"] == "ok"


def test_health_marks_routing_warmup_as_non_blocking_monitoring():
    redis_payloads = {
        router.WEATHER_KEY: {},
        router.GBFS_KEY: {},
        router.IRVE_KEY: {},
        router.EVENTS_CONTEXT_KEY: {},
        router.FUEL_CONTEXT_KEY: {},
        router.CONTEXT_QUALITY_KEY: {},
        router.TLC_REPLAY_KEY: {},
        router.SINGLE_REPLAY_STATUS_KEY: {
            "driver_id": "drv_demo_001",
            "state": "repositioning",
            "positions": "4",
        },
    }
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=_FakeRedis(redis_payloads),
                copilot_model={"feature_columns": []},
                copilot_model_quality_gate={"accepted": True, "reason": "ok"},
            )
        )
    )

    payload = asyncio.run(router.copilot_health(request))

    rq = payload["routing_quality"]
    assert rq["status"] == "warming_up"
    assert rq["ready"] is True
    assert "first route samples" in rq["note"].lower()
    assert payload["demo_readiness"]["ready"] is True
    assert payload["demo_readiness"]["status"] == "monitoring"
    assert payload["demo_readiness"]["summary"].startswith("Demo ready")


def test_normalize_nyc_ev_station_maps_open_data_row():
    station = router._normalize_nyc_ev_station(
        {
            "station_name": "Broadway Hub",
            "street": "350 Broadway",
            "borough": "Manhattan",
            "type_of_charger": "Level 2",
            "latitude": "40.7163",
            "longitude": "-74.0055",
        },
        fallback_index=7,
    )

    assert station is not None
    assert station["source"] == "nyc_open_data_ev_stations"
    assert station["station_id"].startswith("broadway_hub")
    assert station["puissance_kw"] == 7.2
    assert station["zone_id"]


def test_health_falls_back_to_single_driver_replay_status():
    redis_payloads = {
        router.WEATHER_KEY: {},
        router.GBFS_KEY: {},
        router.IRVE_KEY: {},
        router.EVENTS_CONTEXT_KEY: {},
        router.FUEL_CONTEXT_KEY: {},
        router.CONTEXT_QUALITY_KEY: {},
        router.TLC_REPLAY_KEY: {},
        router.SINGLE_REPLAY_STATUS_KEY: {
            "driver_id": "drv_demo_001",
            "state": "repositioning",
            "positions": "23",
            "trips": "1",
            "updated_at": "2026-04-25T00:15:39.679856+00:00",
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

    assert payload["tlc_replay"]["status"] == "ok"
    assert payload["tlc_replay"]["mode"] == "single_driver"
    assert payload["tlc_replay"]["state"] == "repositioning"
    assert payload["tlc_replay"]["events"] == "23"
