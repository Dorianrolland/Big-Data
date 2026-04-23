"""Tests API GET /copilot/fleet/overview (COP-028)."""
from __future__ import annotations

import asyncio
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
    def __init__(self, couriers: dict[str, dict[str, str]], zones: dict[str, dict[str, str]] | None = None):
        self._couriers = couriers
        self._zones = zones or {}

    async def scan_iter(self, match: str = "*", count: int = 100):
        _ = count
        if match.startswith(router.ZONE_CONTEXT_PREFIX):
            prefix = router.ZONE_CONTEXT_PREFIX
            for zone_id in self._zones:
                yield f"{prefix}{zone_id}"
            return
        prefix = router.COURIER_HASH_PREFIX
        for cid in self._couriers:
            yield f"{prefix}{cid}"

    async def hgetall(self, key: str) -> dict[str, str]:
        if isinstance(key, bytes):
            key = key.decode()
        courier_prefix = router.COURIER_HASH_PREFIX
        if key.startswith(courier_prefix):
            cid = key[len(courier_prefix):]
            return dict(self._couriers.get(cid, {}))
        zone_prefix = router.ZONE_CONTEXT_PREFIX
        if key.startswith(zone_prefix):
            zone_id = key[len(zone_prefix):]
            return dict(self._zones.get(zone_id, {}))
        return {}

    def pipeline(self, transaction: bool = False):
        _ = transaction
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, redis: _FakeRedis):
        self._redis = redis
        self._keys: list[str] = []

    def hgetall(self, key: str) -> None:
        self._keys.append(key)

    async def execute(self) -> list[dict[str, str]]:
        return [await self._redis.hgetall(key) for key in self._keys]


def _make_request(couriers: dict, zones: dict[str, dict[str, str]] | None = None):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(redis=_FakeRedis(couriers, zones)))
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


def test_fleet_overview_limit_does_not_cap_active_driver_count():
    req = _make_request(_SAMPLE_COURIERS)
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=3))
    assert result["active_drivers"] == 10
    assert result["drivers_returned"] == 3
    assert result["truncated"] is True
    assert len(result["drivers"]) == 3


def test_fleet_overview_enriches_unknown_zone_from_nearest_context():
    couriers = {
        "drv_unknown": {
            "lat": "40.7582",
            "lon": "-73.9853",
            "status": "delivering",
            "ts": "2024-01-01T14:00:00+00:00",
        }
    }
    zones = {
        "nyc_161": {
            "zone_id": "nyc_161",
            "demand_index": "2.1",
            "supply_index": "0.7",
            "weather_factor": "1.1",
            "traffic_factor": "1.2",
        }
    }
    req = _make_request(couriers, zones)
    result = asyncio.run(router.fleet_overview(req, zone=None, status=None, min_score=0.0, limit=10))
    driver = result["drivers"][0]
    assert driver["zone_id"] == "nyc_161"
    assert driver["zone_source"] == "nearest_zone_context"
    assert driver["demand_index"] > 0.0
    assert driver["zone_lat"] is not None
    assert driver["zone_lon"] is not None


def test_api_dockerfile_copies_zone_centroids():
    content = (_API_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY context_poller/nyc_zone_centroids.json /app/context_poller/nyc_zone_centroids.json" in content
    assert "COPY tlc_replay/nyc_zone_centroids.json /app/tlc_replay/nyc_zone_centroids.json" in content
