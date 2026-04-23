from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))


class _FakeGauge:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def set(self, *_args, **_kwargs) -> None:
        return None


class _FakeInstrumentator:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def instrument(self, app):  # noqa: ANN001
        return self

    def expose(self, *_args, **_kwargs):  # noqa: ANN001
        return None


sys.modules.setdefault("prometheus_client", types.SimpleNamespace(Gauge=_FakeGauge))
sys.modules.setdefault(
    "prometheus_fastapi_instrumentator",
    types.SimpleNamespace(Instrumentator=_FakeInstrumentator),
)

_API_MAIN_SPEC = importlib.util.spec_from_file_location(
    "api_main_focus_under_test",
    _API_DIR / "main.py",
)
assert _API_MAIN_SPEC is not None and _API_MAIN_SPEC.loader is not None
api_main = importlib.util.module_from_spec(_API_MAIN_SPEC)
sys.modules.setdefault("api_main_focus_under_test", api_main)
_API_MAIN_SPEC.loader.exec_module(api_main)


class _FakeRedis:
    def __init__(self, hashes: dict[str, dict[str, str]], tracks: dict[str, list[str]]) -> None:
        self.hashes = {key: dict(value) for key, value in hashes.items()}
        self.tracks = {key: list(value) for key, value in tracks.items()}

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self.tracks.get(key, [])
        if end == -1:
            return list(items[start:])
        return list(items[start : end + 1])


def _track_point(ts: str, lat: float, lon: float, speed_kmh: float, status: str) -> str:
    return json.dumps(
        {
            "lat": lat,
            "lon": lon,
            "ts": ts,
            "speed_kmh": speed_kmh,
            "status": status,
            "route_source": "osrm",
            "anomaly_state": "ok",
        },
        separators=(",", ":"),
    )


def test_merge_focus_trail_points_keeps_order_and_dedupes_live_position():
    cold = [
        {"ts": "2024-01-02T12:00:00+00:00", "lat": 40.75, "lon": -73.99, "speed_kmh": 8.0, "status": "repositioning"},
    ]
    hot = [
        {"ts": "2024-01-02T12:01:00+00:00", "lat": 40.751, "lon": -73.988, "speed_kmh": 12.0, "status": "delivering"},
    ]
    position = {
        "ts": "2024-01-02T12:01:00+00:00",
        "lat": 40.751,
        "lon": -73.988,
        "speed_kmh": 12.0,
        "status": "delivering",
    }

    merged = api_main._merge_focus_trail_points(cold, hot, position=position, max_points=10)

    assert len(merged) == 2
    assert merged[0]["status"] == "repositioning"
    assert merged[-1]["status"] == "delivering"


def test_livreur_focus_uses_hot_track_when_cold_trail_fails(monkeypatch):
    driver_id = "drv_demo_focus"
    api_main._focus_trail_cache.clear()
    api_main.app.state.redis = _FakeRedis(
        hashes={
            f"{api_main.HASH_PREFIX}{driver_id}": {
                "lat": "40.7520",
                "lon": "-73.9810",
                "speed_kmh": "18.0",
                "heading_deg": "90.0",
                "status": "delivering",
                "accuracy_m": "6.0",
                "battery_pct": "88.0",
                "ts": "2024-01-02T12:02:00+00:00",
                "route_source": "osrm",
                "anomaly_state": "ok",
                "stale_reason": "",
            },
            "copilot:replay:tlc:single:status": {
                "state": "running",
            },
        },
        tracks={
            f"{api_main.TRACK_KEY_PREFIX}{driver_id}": [
                _track_point("2024-01-02T12:02:00+00:00", 40.7520, -73.9810, 18.0, "delivering"),
                _track_point("2024-01-02T12:01:30+00:00", 40.7512, -73.9822, 17.5, "delivering"),
                _track_point("2024-01-02T12:01:00+00:00", 40.7505, -73.9831, 16.0, "pickup_arrived"),
            ],
        },
    )

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise RuntimeError("DuckDB unavailable")

    monkeypatch.setattr(api_main, "_dq", _failing_dq)

    payload = asyncio.run(
        api_main.livreur_focus(
            driver_id=driver_id,
            trail_points=5,
            stale_after_s=15.0,
        )
    )

    assert payload["position"]["driver_id"] == driver_id
    assert len(payload["trail"]) >= 3
    assert payload["trail"][-1]["lat"] == payload["position"]["lat"]
    assert payload["trail"][-1]["lon"] == payload["position"]["lon"]
    assert payload["trail"][0]["status"] == "pickup_arrived"
