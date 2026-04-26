from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException

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
    "api_main_fallback_under_test",
    _API_DIR / "main.py",
)
assert _API_MAIN_SPEC is not None and _API_MAIN_SPEC.loader is not None
api_main = importlib.util.module_from_spec(_API_MAIN_SPEC)
sys.modules.setdefault("api_main_fallback_under_test", api_main)
_API_MAIN_SPEC.loader.exec_module(api_main)


class _FakePipeline:
    def __init__(self, redis: "_FakeRedis") -> None:
        self.redis = redis
        self.ops: list[tuple] = []

    def hmget(self, key: str, *fields: str):  # noqa: ANN001
        self.ops.append(("hmget", key, list(fields)))
        return self

    def lrange(self, key: str, start: int, end: int):  # noqa: ANN001
        self.ops.append(("lrange", key, start, end))
        return self

    async def execute(self) -> list[object]:
        out: list[object] = []
        for op in self.ops:
            if op[0] == "hmget":
                _, key, fields = op
                bucket = self.redis.hashes.get(key, {})
                out.append([bucket.get(field) for field in fields])
            elif op[0] == "lrange":
                _, key, start, end = op
                items = self.redis.tracks.get(key, [])
                if end == -1:
                    out.append(list(items[start:]))
                else:
                    out.append(list(items[start : end + 1]))
            else:
                raise AssertionError(f"Unsupported pipeline operation: {op[0]}")
        return out


class _FakeRedis:
    def __init__(
        self,
        *,
        ids: list[str] | None = None,
        hashes: dict[str, dict[str, str]] | None = None,
        tracks: dict[str, list[str]] | None = None,
        geosearch_rows: list[tuple[str, tuple[float, float]]] | None = None,
    ) -> None:
        self.ids = list(ids or [])
        self.hashes = {key: dict(value) for key, value in (hashes or {}).items()}
        self.tracks = {key: list(value) for key, value in (tracks or {}).items()}
        self.geosearch_rows = list(geosearch_rows or [])

    async def ping(self) -> bool:
        return True

    async def zcard(self, _key: str) -> int:
        return len(self.ids)

    async def zrange(self, _key: str, start: int, end: int) -> list[str]:
        if end == -1:
            return list(self.ids[start:])
        return list(self.ids[start : end + 1])

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self.tracks.get(key, [])
        if end == -1:
            return list(items[start:])
        return list(items[start : end + 1])

    async def geosearch(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        return list(self.geosearch_rows)

    def pipeline(self, transaction: bool = False) -> _FakePipeline:  # noqa: ARG002
        return _FakePipeline(self)


def _track_point(
    ts: str,
    speed_kmh: float,
    status: str,
    *,
    route_source: str = "osrm",
    anomaly_state: str = "ok",
) -> str:
    return json.dumps(
        {
            "lat": 40.758,
            "lon": -73.9855,
            "ts": ts,
            "speed_kmh": speed_kmh,
            "status": status,
            "route_source": route_source,
            "anomaly_state": anomaly_state,
        },
        separators=(",", ":"),
    )


@pytest.fixture(autouse=True)
def _clear_analytics_cache():
    api_main._analytics_result_cache.clear()
    yield
    api_main._analytics_result_cache.clear()


def test_detect_anomalies_uses_live_tracks_and_ignores_clean_motion(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _unexpected_dq(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("detect_anomalies should not query DuckDB anymore")

    redis = _FakeRedis(
        ids=["drv_demo_001"],
        tracks={
            "fleet:track:drv_demo_001": [
                _track_point("2024-01-02T12:05:00+00:00", 12.0, "delivering"),
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:03:00+00:00", 11.0, "delivering"),
                _track_point("2024-01-02T12:02:00+00:00", 13.0, "delivering"),
            ]
        },
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _unexpected_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert payload["analytics_status"]["degraded"] is False
    assert payload["anomalies"] == []


def test_detect_anomalies_reuses_stale_cache_when_redis_is_unavailable(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _unexpected_dq(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("detect_anomalies should not query DuckDB anymore")

    class _UnavailableRedis:
        async def ping(self):
            raise OSError("redis offline")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _unexpected_dq)
    api_main.app.state.redis = _FakeRedis(
        ids=["drv_demo_001"],
        tracks={
            "fleet:track:drv_demo_001": [
                _track_point("2024-01-02T12:05:00+00:00", 12.0, "delivering"),
                _track_point("2024-01-02T12:04:00+00:00", 11.0, "delivering"),
                _track_point("2024-01-02T12:03:00+00:00", 12.0, "delivering"),
            ]
        },
    )

    first = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )
    api_main.app.state.redis = _UnavailableRedis()
    second = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )

    assert first["analytics_status"]["source"] == "hot_path_live"
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache"
    assert second["analytics_status"]["degraded"] is False
    assert second["resume"]["livreurs_scannes"] == 1
    assert second["analytics_status"]["snapshot_age_s"] >= 0


def test_zone_coverage_uses_live_zone_context_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    redis = _FakeRedis(
        hashes={
            "copilot:context:zone:40.76_-73.98": {
                "zone_id": "40.76_-73.98",
                "demand_index": "1.8",
                "supply_index": "0.9",
                "traffic_factor": "1.1",
                "forecast_demand_index_15m": "2.4",
            }
        },
        geosearch_rows=[
            ("drv_demo_001", (-73.98, 40.76)),
        ]
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(api_main.zone_coverage(resolution=0.02, heures=1, reference_ts=None))

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "live_zone_context"
    assert payload["analytics_status"]["degraded"] is False
    assert payload["nb_zones_analysees"] == 1
    assert payload["toutes_zones"][0]["livreurs_actifs"] == 1


def test_fleet_insights_reuses_stale_historical_snapshot_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)
    call_state = {"should_fail": False}

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(*_args, **_kwargs):  # noqa: ANN001
        if call_state["should_fail"]:
            raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")
        return [(1, 12.0, 33.3, 1, 2)]

    redis = _FakeRedis(
        ids=["drv_demo_001"],
        hashes={
            "fleet:livreur:drv_demo_001": {
                "status": "delivering",
                "speed_kmh": "0.0",
                "ts": "2024-01-02T12:04:00+00:00",
                "lat": "40.7600",
                "lon": "-73.9800",
                "battery_pct": "80",
            }
        },
        tracks={
            "fleet:track:drv_demo_001": [
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:03:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:02:00+00:00", 0.0, "delivering"),
            ]
        },
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)
    api_main.app.state.redis = redis

    first = asyncio.run(api_main.fleet_insights(heures=1, reference_ts=None))
    call_state["should_fail"] = True
    second = asyncio.run(api_main.fleet_insights(heures=1, reference_ts=None))

    assert first["cold_path_available"] is True
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache_blended"
    assert second["analytics_status"]["degraded"] is False
    assert second["productivite_historique"]["vitesse_moyenne_kmh"] == 12.0
    assert second["flotte_temps_reel"]["total_actifs"] == 1


def test_history_returns_structured_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)
    api_main.app.state.redis = _FakeRedis()

    payload = asyncio.run(
        api_main.history(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "cold_path_fallback"
    assert payload["resume"]["nb_points"] == 0
    assert payload["resume"]["distance_totale_km"] == 0.0
    assert payload["trajectory"] == []
    assert payload["livreur_id"] == "drv_demo_001"
    assert "DuckDB timeout" in payload["detail"]


def test_history_uses_live_track_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"mode": "explicit", "source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    redis = _FakeRedis(
        tracks={
            "fleet:track:drv_demo_001": [
                json.dumps(
                    {
                        "lat": 40.7580,
                        "lon": -73.9855,
                        "ts": "2024-01-02T12:05:00+00:00",
                        "speed_kmh": 12.0,
                        "status": "delivering",
                        "route_source": "osrm",
                        "anomaly_state": "ok",
                    },
                    separators=(",", ":"),
                ),
                json.dumps(
                    {
                        "lat": 40.7586,
                        "lon": -73.9848,
                        "ts": "2024-01-02T12:04:00+00:00",
                        "speed_kmh": 11.0,
                        "status": "delivering",
                        "route_source": "osrm",
                        "anomaly_state": "ok",
                    },
                    separators=(",", ":"),
                ),
            ]
        }
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.history(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert payload["analytics_status"]["degraded"] is False
    assert payload["resume"]["nb_points"] == 2
    assert len(payload["trajectory"]) == 2
    assert "detail" not in payload


def test_history_uses_live_track_fallback_when_cold_path_has_no_rows(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"mode": "explicit", "source": "test"}

    async def _empty_dq(*_args, **_kwargs):  # noqa: ANN001
        return []

    redis = _FakeRedis(
        tracks={
            "fleet:track:drv_demo_001": [
                json.dumps(
                    {
                        "lat": 40.7580,
                        "lon": -73.9855,
                        "ts": "2024-01-02T12:05:00+00:00",
                        "speed_kmh": 12.0,
                        "status": "delivering",
                        "route_source": "osrm",
                        "anomaly_state": "ok",
                    },
                    separators=(",", ":"),
                ),
                json.dumps(
                    {
                        "lat": 40.7586,
                        "lon": -73.9848,
                        "ts": "2024-01-02T12:04:00+00:00",
                        "speed_kmh": 11.0,
                        "status": "delivering",
                        "route_source": "osrm",
                        "anomaly_state": "ok",
                    },
                    separators=(",", ":"),
                ),
            ]
        }
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _empty_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.history(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert payload["resume"]["nb_points"] == 2
    assert len(payload["trajectory"]) == 2


def test_history_reuses_stale_cache_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)
    call_state = {"should_fail": False}

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(sql, *_args, **_kwargs):  # noqa: ANN001
        if call_state["should_fail"]:
            raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")
        if "SELECT lat, lon, speed_kmh, heading_deg, status, ts" in sql:
            return [
                (40.76, -73.98, 12.0, 90.0, "delivering", ref_ts),
                (40.761, -73.979, 14.0, 92.0, "delivering", ref_ts),
            ]
        if "AS total_km" in sql:
            return [(1.23,)]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)

    first = asyncio.run(api_main.history(livreur_id="drv_demo_001", heures=1, reference_ts=None))
    for key, (_expires_at, cached_at, payload) in list(api_main._analytics_result_cache.items()):
        api_main._analytics_result_cache[key] = (0.0, cached_at, payload)
    call_state["should_fail"] = True
    second = asyncio.run(api_main.history(livreur_id="drv_demo_001", heures=1, reference_ts=None))

    assert first["cold_path_available"] is True
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache"
    assert second["resume"]["distance_totale_km"] == 1.23
    assert len(second["trajectory"]) == 2
    assert "detail" not in second


def test_driver_score_returns_structured_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)
    api_main.app.state.redis = _FakeRedis()

    payload = asyncio.run(
        api_main.driver_score(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "cold_path_fallback"
    assert payload["livreur_id"] == "drv_demo_001"
    assert "score_global" not in payload
    assert "DuckDB timeout" in payload["detail"]


def test_driver_score_uses_live_track_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"mode": "explicit", "source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    redis = _FakeRedis(
        tracks={
            "fleet:track:drv_demo_001": [
                json.dumps(
                    {
                        "lat": 40.7580,
                        "lon": -73.9855,
                        "ts": "2024-01-02T12:05:00+00:00",
                        "speed_kmh": 17.0,
                        "status": "delivering",
                    },
                    separators=(",", ":"),
                ),
                json.dumps(
                    {
                        "lat": 40.7586,
                        "lon": -73.9848,
                        "ts": "2024-01-02T12:04:00+00:00",
                        "speed_kmh": 14.0,
                        "status": "available",
                    },
                    separators=(",", ":"),
                ),
            ]
        }
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.driver_score(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert payload["analytics_status"]["degraded"] is False
    assert payload["score_global"] >= 0
    assert payload["metriques"]["vitesse_moyenne_kmh"] == 15.5
    assert "detail" not in payload


def test_driver_score_uses_live_track_fallback_when_cold_path_has_no_rows(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"mode": "explicit", "source": "test"}

    async def _empty_dq(*_args, **_kwargs):  # noqa: ANN001
        return []

    redis = _FakeRedis(
        tracks={
            "fleet:track:drv_demo_001": [
                json.dumps(
                    {
                        "lat": 40.7580,
                        "lon": -73.9855,
                        "ts": "2024-01-02T12:05:00+00:00",
                        "speed_kmh": 12.0,
                        "status": "delivering",
                    },
                    separators=(",", ":"),
                ),
                json.dumps(
                    {
                        "lat": 40.7586,
                        "lon": -73.9848,
                        "ts": "2024-01-02T12:04:00+00:00",
                        "speed_kmh": 10.0,
                        "status": "delivering",
                    },
                    separators=(",", ":"),
                ),
            ]
        }
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _empty_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.driver_score(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert payload["score_global"] >= 0
    assert payload["metriques"]["taux_livraison_pct"] == 100.0


def test_analytics_status_payload_clears_stale_detail():
    payload = api_main._analytics_status_payload(
        {"resume": {"ok": True}, "detail": "old warning"},
        cold_path_available=False,
        degraded=False,
        source="stale_cache",
        detail="",
    )

    assert payload["analytics_status"]["source"] == "stale_cache"
    assert "detail" not in payload


def test_detect_gps_fraud_uses_live_validated_tracks_and_only_surfaces_frozen_positions(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _unexpected_dq(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("detect_gps_fraud should no longer query DuckDB")

    redis = _FakeRedis(
        ids=["drv_demo_001", "drv_demo_002"],
        tracks={
            "fleet:track:drv_demo_001": [
                _track_point("2024-01-02T12:05:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:04:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:03:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:02:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:01:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T12:00:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:59:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:58:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:57:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:56:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:55:00+00:00", 0.0, "delivering"),
                _track_point("2024-01-02T11:54:00+00:00", 0.0, "delivering"),
            ],
            "fleet:track:drv_demo_002": [
                _track_point("2024-01-02T12:05:00+00:00", 12.0, "delivering"),
                _track_point("2024-01-02T12:04:00+00:00", 10.0, "delivering"),
            ],
        },
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _unexpected_dq)
    api_main.app.state.redis = redis

    payload = asyncio.run(
        api_main.detect_gps_fraud(
            fenetre_minutes=15,
            seuil_teleport_km=2.0,
            vitesse_max_physique_kmh=90.0,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "hot_path_live"
    assert payload["analytics_status"]["degraded"] is False
    assert payload["resume"]["teleportations"] == 0
    assert payload["resume"]["positions_figees"] == 1
    assert payload["resume"]["total_fraudes"] == 1
    assert payload["positions_figees"][0]["livreur_id"] == "drv_demo_001"
    assert payload["teleportations"] == []


def test_predict_demand_uses_live_zone_context_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)
    api_main.app.state.redis = _FakeRedis(
        hashes={
            "copilot:context:zone:40.76_-73.98": {
                "zone_id": "40.76_-73.98",
                "demand_index": "1.4",
                "supply_index": "0.8",
                "traffic_factor": "1.1",
                "forecast_demand_index_15m": "2.0",
                "demand_trend": "0.22",
            }
        }
    )

    payload = asyncio.run(
        api_main.predict_demand(
            horizon_minutes=30,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "live_zone_context"
    assert payload["analytics_status"]["degraded"] is False
    assert payload["nb_zones_analysees"] == 1
    assert payload["dispatch_prioritaire"]
