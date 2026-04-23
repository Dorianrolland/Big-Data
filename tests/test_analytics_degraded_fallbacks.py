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

    async def geosearch(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        return list(self.geosearch_rows)

    def pipeline(self, transaction: bool = False) -> _FakePipeline:  # noqa: ARG002
        return _FakePipeline(self)


def _track_point(ts: str, speed_kmh: float, status: str) -> str:
    return json.dumps(
        {
            "lat": 40.758,
            "lon": -73.9855,
            "ts": ts,
            "speed_kmh": speed_kmh,
            "status": status,
            "route_source": "osrm",
            "anomaly_state": "ok",
        },
        separators=(",", ":"),
    )


@pytest.fixture(autouse=True)
def _clear_analytics_cache():
    api_main._analytics_result_cache.clear()
    yield
    api_main._analytics_result_cache.clear()


def test_detect_anomalies_returns_structured_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)

    payload = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "cold_path_fallback"
    assert payload["analytics_status"]["degraded"] is True
    assert payload["anomalies"] == []
    assert "DuckDB timeout" in payload["detail"]


def test_detect_anomalies_reuses_stale_cache_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)
    call_state = {"should_fail": False}

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(sql, *_args, **_kwargs):  # noqa: ANN001
        if call_state["should_fail"]:
            raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")
        if "COUNT(DISTINCT livreur_id)" in sql:
            return [(7,)]
        return []

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)

    first = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )
    call_state["should_fail"] = True
    second = asyncio.run(
        api_main.detect_anomalies(
            fenetre_minutes=10,
            seuil_vitesse=50.0,
            seuil_immobile=2.0,
            reference_ts=None,
        )
    )

    assert first["cold_path_available"] is True
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache"
    assert second["resume"]["livreurs_scannes"] == 7
    assert second["analytics_status"]["snapshot_age_s"] >= 0


def test_zone_coverage_reuses_stale_cache_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)
    call_state = {"should_fail": False}

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(*_args, **_kwargs):  # noqa: ANN001
        if call_state["should_fail"]:
            raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")
        return [(40.76, -73.98, 120)]

    redis = _FakeRedis(
        geosearch_rows=[
            ("drv_demo_001", (-73.98, 40.76)),
        ]
    )

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)
    api_main.app.state.redis = redis

    first = asyncio.run(api_main.zone_coverage(resolution=0.02, heures=1, reference_ts=None))
    call_state["should_fail"] = True
    second = asyncio.run(api_main.zone_coverage(resolution=0.02, heures=1, reference_ts=None))

    assert first["cold_path_available"] is True
    assert first["nb_zones_analysees"] == 1
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache"
    assert second["nb_zones_analysees"] == 1
    assert second["toutes_zones"][0]["passages_historiques"] == 120


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
    assert second["analytics_status"]["source"] == "stale_cache"
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

    payload = asyncio.run(
        api_main.history(
            livreur_id="drv_demo_001",
            heures=1,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "cold_path_fallback"
    assert payload["trajectory"] == []
    assert payload["livreur_id"] == "drv_demo_001"
    assert "DuckDB timeout" in payload["detail"]


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
    call_state["should_fail"] = True
    second = asyncio.run(api_main.history(livreur_id="drv_demo_001", heures=1, reference_ts=None))

    assert first["cold_path_available"] is True
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache"
    assert second["resume"]["distance_totale_km"] == 1.23
    assert len(second["trajectory"]) == 2


def test_driver_score_returns_structured_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)

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


def test_detect_gps_fraud_reuses_stale_cache_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)
    call_state = {"should_fail": False}

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _fake_dq(sql, *_args, **_kwargs):  # noqa: ANN001
        if call_state["should_fail"]:
            raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")
        if "SELECT * FROM fraud" in sql:
            return [
                (
                    "drv_demo_001",
                    40.76,
                    -73.98,
                    40.75,
                    -73.97,
                    ref_ts,
                    ref_ts,
                    22.0,
                    2.8,
                    168.0,
                )
            ]
        if "HAVING COUNT(*) >= 5" in sql:
            return [("drv_demo_002", 40.76, -73.98, 6, ref_ts, ref_ts)]
        if "COUNT(DISTINCT livreur_id)" in sql:
            return [(4,)]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _fake_dq)

    first = asyncio.run(
        api_main.detect_gps_fraud(
            fenetre_minutes=15,
            seuil_teleport_km=2.0,
            vitesse_max_physique_kmh=90.0,
            reference_ts=None,
        )
    )
    call_state["should_fail"] = True
    second = asyncio.run(
        api_main.detect_gps_fraud(
            fenetre_minutes=15,
            seuil_teleport_km=2.0,
            vitesse_max_physique_kmh=90.0,
            reference_ts=None,
        )
    )

    assert first["cold_path_available"] is True
    assert second["cold_path_available"] is False
    assert second["analytics_status"]["source"] == "stale_cache"
    assert second["resume"]["total_fraudes"] == 2


def test_predict_demand_returns_structured_fallback_when_duckdb_fails(monkeypatch):
    ref_ts = datetime(2024, 1, 2, 12, 5, tzinfo=timezone.utc)

    async def _fake_resolve_reference_ts(_value=None):  # noqa: ANN001
        return ref_ts, {"source": "test"}

    async def _failing_dq(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="DuckDB timeout (4.0s).")

    monkeypatch.setattr(api_main, "_resolve_reference_ts", _fake_resolve_reference_ts)
    monkeypatch.setattr(api_main, "_dq", _failing_dq)

    payload = asyncio.run(
        api_main.predict_demand(
            horizon_minutes=30,
            reference_ts=None,
        )
    )

    assert payload["cold_path_available"] is False
    assert payload["analytics_status"]["source"] == "cold_path_fallback"
    assert payload["nb_zones_analysees"] == 0
    assert payload["dispatch_prioritaire"] == []
    assert "DuckDB timeout" in payload["detail"]
