"""Tests de contrat pour le rapport de session exportable (COP-023)."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import copilot_router as router


class _FakeRedis:
    def __init__(self, missions: list[dict]):
        self._missions = missions

    async def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        items = self._missions[start : stop + 1]
        return [json.dumps(m).encode() for m in items]


_SAMPLE_MISSIONS = [
    {
        "mission_id": f"m_{i:03d}",
        "driver_id": "drv_001",
        "zone_id": "Z01" if i % 2 == 0 else "Z02",
        "dispatch_strategy": "balanced",
        "elapsed_min": 15.0 + i,
        "initial_distance_km": 2.0,
        "final_distance_km": 0.1,
        "predicted_potential_eur_h": 14.0,
        "baseline_eur_h": 10.0,
        "realized_best_offer_eur_h": 13.0 + i * 0.5,
        "predicted_delta_eur_h": 4.0,
        "realized_delta_eur_h": 3.0 + i * 0.5,
        "alignment_score": 0.82,
        "success": True,
    }
    for i in range(5)
]


def _make_request(missions: list[dict]):
    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                redis=_FakeRedis(missions),
            )
        )
    )


def test_session_report_json_structure():
    req = _make_request(_SAMPLE_MISSIONS)
    result = asyncio.run(router.session_report(req, "drv_001", fmt="json", limit=50))
    assert result["driver_id"] == "drv_001"
    assert "kpis" in result
    assert "decisions" in result
    kpis = result["kpis"]
    assert kpis["missions_count"] == 5
    assert kpis["success_rate_pct"] == 100.0
    assert isinstance(kpis["avg_elapsed_min"], float)
    assert isinstance(kpis["avg_realized_delta_eur_h"], float)


def test_session_report_kpis_no_missions():
    req = _make_request([])
    result = asyncio.run(router.session_report(req, "drv_empty", fmt="json", limit=50))
    assert result["kpis"]["missions_count"] == 0
    assert result["kpis"]["success_rate_pct"] is None


def test_session_report_csv_format():
    req = _make_request(_SAMPLE_MISSIONS)
    response = asyncio.run(router.session_report(req, "drv_001", fmt="csv", limit=50))
    assert response.media_type == "text/csv"
    content = response.body.decode()
    assert "FleetStream" in content
    assert "missions_count" in content or "# KPIs" in content


def test_session_report_csv_parseable():
    req = _make_request(_SAMPLE_MISSIONS)
    response = asyncio.run(router.session_report(req, "drv_001", fmt="csv", limit=50))
    content = response.body.decode()
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    assert len(rows) > 3


def test_session_report_pdf_format():
    req = _make_request(_SAMPLE_MISSIONS)
    response = asyncio.run(router.session_report(req, "drv_001", fmt="pdf", limit=50))
    assert response.media_type == "application/pdf"
    assert response.body[:4] == b"%PDF"


def test_build_session_report_top_zones():
    missions = _SAMPLE_MISSIONS + [
        {**_SAMPLE_MISSIONS[0], "zone_id": "Z01"},
        {**_SAMPLE_MISSIONS[0], "zone_id": "Z01"},
    ]
    report = router._build_session_report("drv_001", missions)
    zones = dict(report["kpis"]["top_zones"])
    assert zones.get("Z01", 0) >= zones.get("Z02", 0)
