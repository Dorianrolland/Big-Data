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
