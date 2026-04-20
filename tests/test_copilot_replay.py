"""Unit tests for replay parquet glob selection helpers."""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import api.copilot_router as copilot_router_module  # noqa: E402
from api.copilot_router import _existing_parquet_globs  # noqa: E402


def test_existing_parquet_globs_keeps_only_patterns_with_files(tmp_path: Path):
    present_dir = tmp_path / "topic=offer" / "year=2026" / "month=04" / "day=17" / "hour=18"
    present_dir.mkdir(parents=True)
    (present_dir / "chunk.parquet").write_bytes(b"PAR1")

    present_pattern = str(present_dir / "*.parquet")
    missing_pattern = str(tmp_path / "topic=score" / "year=2026" / "month=04" / "day=17" / "hour=18" / "*.parquet")

    out = _existing_parquet_globs([missing_pattern, present_pattern])
    assert out == [present_pattern]


def test_existing_parquet_globs_returns_empty_when_nothing_matches(tmp_path: Path):
    p1 = str(tmp_path / "topic=offer" / "year=2026" / "month=04" / "day=17" / "hour=01" / "*.parquet")
    p2 = str(tmp_path / "topic=score" / "year=2026" / "month=04" / "day=17" / "hour=02" / "*.parquet")
    assert _existing_parquet_globs([p1, p2]) == []


def test_replay_returns_empty_instead_of_500_when_no_partition_matches(tmp_path: Path, monkeypatch):
    # Regression test for CI: replay used to 500 when requested time window had
    # no matching parquet partitions for topic=* globs.
    monkeypatch.setattr(copilot_router_module, "EVENTS_PATH", tmp_path)

    app = FastAPI()
    app.include_router(copilot_router_module.copilot_router)
    with TestClient(app) as client:
        response = client.get(
            "/copilot/replay",
            params={
                "from": "2000-01-01T00:00:00+00:00",
                "to": "2000-01-01T00:10:00+00:00",
                "limit": 100,
            },
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0


def test_replay_includes_position_fields_and_merged_stream(tmp_path: Path, monkeypatch):
    events_path = tmp_path / "events"
    positions_path = tmp_path / "positions"
    events_path.mkdir(parents=True)
    positions_path.mkdir(parents=True)

    monkeypatch.setattr(copilot_router_module, "EVENTS_PATH", events_path)
    monkeypatch.setattr(copilot_router_module, "POSITIONS_PATH", positions_path)
    monkeypatch.setattr(
        copilot_router_module,
        "_existing_parquet_globs",
        lambda globs: [globs[0]] if globs else [],
    )

    captured: dict[str, object] = {}

    async def _fake_duck_query(_request, sql, params=None):  # noqa: ANN001
        captured["sql"] = sql
        captured["params"] = list(params or [])
        return [
            (
                "2026-04-17 20:00:00+00:00",
                "order-events-v1",
                "order.event.v1",
                "evt_1",
                "drv_demo_001",
                "off_001",
                "ord_001",
                "accepted",
                "nyc_100",
                13.5,
                None,
                1.2,
                0.9,
                "tlc_hvfhv_historical",
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "2026-04-17 20:01:00+00:00",
                "livreurs-gps",
                "courier.position.v1",
                "",
                "drv_demo_001",
                "",
                "",
                "delivering",
                "",
                None,
                None,
                None,
                None,
                "position_lake",
                40.758,
                -73.9855,
                17.2,
                98.0,
                7.5,
                91.0,
            ),
        ]

    monkeypatch.setattr(copilot_router_module, "_duck_query", _fake_duck_query)

    app = FastAPI()
    app.include_router(copilot_router_module.copilot_router)
    with TestClient(app) as client:
        response = client.get(
            "/copilot/replay",
            params={
                "from": "2026-04-17T19:55:00+00:00",
                "to": "2026-04-17T20:05:00+00:00",
                "driver_id": "drv_demo_001",
                "limit": 50,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert payload["events"][0]["event_type"] == "order.event.v1"
    assert payload["events"][0]["lat"] is None
    assert payload["events"][1]["event_type"] == "courier.position.v1"
    assert payload["events"][1]["lat"] == 40.758
    assert payload["events"][1]["lon"] == -73.9855
    assert payload["events"][1]["speed_kmh"] == 17.2
    assert payload["events"][1]["heading_deg"] == 98.0

    sql_text = str(captured.get("sql") or "")
    assert "UNION ALL" in sql_text
    assert "courier.position.v1" in sql_text


def test_replay_returns_empty_when_only_positions_path_exists_without_partitions(tmp_path: Path, monkeypatch):
    positions_path = tmp_path / "positions"
    positions_path.mkdir(parents=True)

    monkeypatch.setattr(copilot_router_module, "EVENTS_PATH", tmp_path / "events_missing")
    monkeypatch.setattr(copilot_router_module, "POSITIONS_PATH", positions_path)
    monkeypatch.setattr(copilot_router_module, "_existing_parquet_globs", lambda _globs: [])

    app = FastAPI()
    app.include_router(copilot_router_module.copilot_router)
    with TestClient(app) as client:
        response = client.get(
            "/copilot/replay",
            params={
                "from": "2026-04-17T19:00:00+00:00",
                "to": "2026-04-17T20:00:00+00:00",
                "driver_id": "drv_demo_001",
                "limit": 20,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0


def test_replay_returns_404_when_both_replay_paths_missing(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(copilot_router_module, "EVENTS_PATH", tmp_path / "events_missing")
    monkeypatch.setattr(copilot_router_module, "POSITIONS_PATH", tmp_path / "positions_missing")

    app = FastAPI()
    app.include_router(copilot_router_module.copilot_router)
    with TestClient(app) as client:
        response = client.get(
            "/copilot/replay",
            params={
                "from": "2026-04-17T19:00:00+00:00",
                "to": "2026-04-17T20:00:00+00:00",
                "limit": 20,
            },
        )

    assert response.status_code == 404
    detail = str(response.json().get("detail", ""))
    assert "replay paths missing" in detail


def test_replay_sanitizes_non_finite_numbers_and_quotes_globs(tmp_path: Path, monkeypatch):
    events_path = tmp_path / "events"
    positions_path = tmp_path / "positions"
    events_path.mkdir(parents=True)
    positions_path.mkdir(parents=True)

    monkeypatch.setattr(copilot_router_module, "EVENTS_PATH", events_path)
    monkeypatch.setattr(copilot_router_module, "POSITIONS_PATH", positions_path)
    monkeypatch.setattr(
        copilot_router_module,
        "_existing_parquet_globs",
        lambda _globs: ["C:/tmp/it's/parquet/*.parquet"],
    )

    captured: dict[str, object] = {}

    async def _fake_duck_query(_request, sql, params=None):  # noqa: ANN001
        captured["sql"] = sql
        captured["params"] = list(params or [])
        return [
            (
                "2026-04-17 20:02:00+00:00",
                "livreurs-gps",
                "courier.position.v1",
                "",
                "drv_demo_001",
                "",
                "",
                "delivering",
                "",
                float("nan"),
                float("inf"),
                float("-inf"),
                float("nan"),
                "position_lake",
                float("nan"),
                float("inf"),
                float("-inf"),
                float("nan"),
                float("nan"),
                float("inf"),
            )
        ]

    monkeypatch.setattr(copilot_router_module, "_duck_query", _fake_duck_query)

    app = FastAPI()
    app.include_router(copilot_router_module.copilot_router)
    with TestClient(app) as client:
        response = client.get(
            "/copilot/replay",
            params={
                "from": "2026-04-17T20:00:00+00:00",
                "to": "2026-04-17T20:05:00+00:00",
                "driver_id": "drv_demo_001",
                "limit": 20,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    evt = payload["events"][0]
    assert evt["estimated_fare_eur"] is None
    assert evt["actual_fare_eur"] is None
    assert evt["demand_index"] is None
    assert evt["supply_index"] is None
    assert evt["lat"] is None
    assert evt["lon"] is None
    assert evt["speed_kmh"] is None
    assert evt["heading_deg"] is None
    assert evt["accuracy_m"] is None
    assert evt["battery_pct"] is None

    sql_text = str(captured.get("sql") or "")
    assert "it''s" in sql_text
