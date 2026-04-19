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
