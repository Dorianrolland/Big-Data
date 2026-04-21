"""Tests API GET /copilot/fleet/overview (COP-028)."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import copilot_router as router


def _courier_state(cid: str, lat: float, lon: float, **kwargs) -> dict[str, str]:
    base = {
        "lat": str(lat),
        "lon": str(lon),
        "status": "moving",
        "zone_id": "Z01",
        "demand_index": "0.75",
        "supply_index": "0.30",
        "weather_factor": "1.1",
        "traffic_factor": "0.95",
    }
    base.update({k: str(v) for k, v in kwargs.items()})
    return base


class _FakeRedis:
    def __init__(self, couriers: dict[str, dict[str, str]]):
        self._couriers = couriers

    async def scan_iter(self, match: str = "*", count: int = 100):
        prefix = router.COURIER_HASH_PREFIX
        for cid in self._couriers:
            yield f"{prefix}{cid}"

    async def hgetall(self, key: str) -> dict[str, str]:
        if isinstance(key, bytes):
            key = key.decode()
        prefix = router.COURIER_HASH_PREFIX
        cid = key[len(prefix):]
        return dict(self._couriers.get(cid, {}))


def _make_request(couriers: dict):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(redis=_FakeRedis(couriers)))
    )


_SAMPLE_COURIERS = {
    f"drv_{i:03d}": _courier_state(
        f"drv_{i:03d}",
        lat=40.7 + i * 0.01,
        lon=-74.0 + i * 0.01,
        demand_index=str(0.5 + i * 0.05),
        supply_index=str(max(0.1, 0.5 - i * 0.04)),
        zone_id=f"Z0{(i % 3) + 1}",
    )
    for i in range(10)
}


def test_fleet_overview_returns_drivers():
    req = _make_request(_SAMPLE_COURIERS)
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=100))
    assert result["active_drivers"] == 10
    assert len(result["drivers"]) == 10


def test_fleet_overview_sorted_by_score():
    req = _make_request(_SAMPLE_COURIERS)
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=100))
    scores = [d["opportunity_score"] for d in result["drivers"]]
    assert scores == sorted(scores, reverse=True)


def test_fleet_overview_filter_by_zone():
    req = _make_request(_SAMPLE_COURIERS)
    result = asyncio.run(router.fleet_overview(req, zone="Z01", status=None, min_score=0.0, limit=100))
    assert all(d["zone_id"] == "Z01" for d in result["drivers"])
    assert result["active_drivers"] > 0


def test_fleet_overview_filter_by_min_score():
    req = _make_request(_SAMPLE_COURIERS)
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.5, limit=100))
    assert all(d["opportunity_score"] >= 0.5 for d in result["drivers"])


def test_fleet_overview_empty_fleet():
    req = _make_request({})
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=100))
    assert result["active_drivers"] == 0
    assert any(a["type"] == "no_active_drivers" for a in result["alerts"])


def test_fleet_overview_kpis_present():
    req = _make_request(_SAMPLE_COURIERS)
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=100))
    assert "avg_demand_index" in result
    assert "avg_pressure_ratio" in result
    assert isinstance(result["top_zones"], list)
    assert isinstance(result["status_distribution"], dict)


def test_fleet_overview_driver_fields():
    req = _make_request({"drv_001": _courier_state("drv_001", lat=40.75, lon=-74.0)})
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=100))
    d = result["drivers"][0]
    assert "courier_id" in d
    assert "opportunity_score" in d
    assert "best_action" in d
    assert 0.0 <= d["opportunity_score"] <= 1.0
