from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import copilot_router as router  # noqa: E402


def test_rank_shift_plan_items_uses_deterministic_tie_break():
    items = [
        {"zone_id": "nyc_20", "shift_score": 1.0, "estimated_net_eur_h": 21.0, "confidence": 0.7},
        {"zone_id": "nyc_10", "shift_score": 1.0, "estimated_net_eur_h": 21.0, "confidence": 0.7},
        {"zone_id": "nyc_30", "shift_score": 0.9, "estimated_net_eur_h": 19.0, "confidence": 0.6},
    ]
    ranked = router._rank_shift_plan_items(items)
    assert [row["zone_id"] for row in ranked] == ["nyc_10", "nyc_20", "nyc_30"]


def test_build_shift_plan_item_includes_why_now_and_reasons():
    zone = {
        "zone_id": "40.7580_-73.9855",
        "opportunity_score": 1.12,
        "demand_index": 1.5,
        "supply_index": 0.9,
        "weather_factor": 1.0,
        "traffic_factor": 1.1,
        "event_pressure": 0.2,
        "temporal_pressure": 0.14,
        "demand_trend_ema": 0.09,
    }
    forecast = {
        "forecast_opportunity_score": 1.26,
        "forecast_pressure_ratio": 1.31,
        "forecast_volatility": 0.21,
    }
    reposition_cost = {"reposition_total_cost_eur": 2.4}

    out = router._build_shift_plan_item(
        zone=zone,
        horizon_min=60,
        target_hourly_net_eur=18.0,
        route_distance_km=3.1,
        eta_min=9.5,
        reposition_cost=reposition_cost,
        forecast=forecast,
    )

    assert out["shift_score"] > 0
    assert out["estimated_net_eur_h"] >= 0
    assert out["why_now"]
    assert isinstance(out["reasons"], list) and out["reasons"]
    assert out["horizon_min"] == 60


class _FakeRedis:
    async def hgetall(self, key: str) -> dict[str, str]:
        if key == router.FUEL_CONTEXT_KEY:
            return {
                "fuel_price_eur_l": "1.78",
                "vehicle_consumption_l_100km": "7.4",
            }
        return {}


def test_shift_plan_endpoint_returns_sorted_candidates(monkeypatch):
    async def fake_load_zones(_redis):
        return [
            {
                "zone_id": "40.7580_-73.9855",
                "demand_index": 1.6,
                "supply_index": 0.9,
                "weather_factor": 1.0,
                "traffic_factor": 1.05,
                "gbfs_demand_boost": 0.12,
                "demand_trend_ema": 0.08,
                "event_pressure": 0.18,
                "temporal_pressure": 0.13,
                "opportunity_score": 1.35,
            },
            {
                "zone_id": "40.7420_-73.9910",
                "demand_index": 1.5,
                "supply_index": 1.0,
                "weather_factor": 1.0,
                "traffic_factor": 1.08,
                "gbfs_demand_boost": 0.08,
                "demand_trend_ema": 0.05,
                "event_pressure": 0.12,
                "temporal_pressure": 0.09,
                "opportunity_score": 1.12,
            },
            {
                "zone_id": "40.7300_-73.9800",
                "demand_index": 1.4,
                "supply_index": 1.0,
                "weather_factor": 1.0,
                "traffic_factor": 1.02,
                "gbfs_demand_boost": 0.06,
                "demand_trend_ema": 0.03,
                "event_pressure": 0.06,
                "temporal_pressure": 0.08,
                "opportunity_score": 1.01,
            },
            {
                "zone_id": "40.7050_-74.0100",
                "demand_index": 1.25,
                "supply_index": 1.1,
                "weather_factor": 1.0,
                "traffic_factor": 1.14,
                "gbfs_demand_boost": 0.03,
                "demand_trend_ema": 0.01,
                "event_pressure": 0.04,
                "temporal_pressure": 0.04,
                "opportunity_score": 0.89,
            },
        ]

    async def fake_leg(
        *,
        origin_lat: float,
        origin_lon: float,
        dest_lat: float,
        dest_lon: float,
        traffic_factor: float,
        use_osrm: bool,
        osrm_client,
    ):
        _ = origin_lat, origin_lon, dest_lon, use_osrm, osrm_client
        distance = max(abs(dest_lat - 40.70) * 75.0, 1.0)
        eta = (distance / max(20.0 / max(traffic_factor, 0.7), 8.0)) * 60.0
        return distance, max(eta, 1.0), "estimated"

    def fake_forecast(
        *,
        demand_index: float,
        supply_index: float,
        weather_factor: float,
        traffic_factor: float,
        gbfs_demand_boost: float,
        demand_trend: float,
        horizon_minutes: float,
    ):
        _ = horizon_minutes
        pressure = (demand_index / max(supply_index, 0.2)) * max(weather_factor, 0.5) / max(traffic_factor, 0.2)
        boosted = pressure * (1.0 + (gbfs_demand_boost * 0.2) + (demand_trend * 0.3))
        return {
            "forecast_demand_index": demand_index * 1.02,
            "forecast_supply_index": max(supply_index, 0.2),
            "forecast_pressure_ratio": pressure,
            "forecast_opportunity_score": boosted,
            "forecast_volatility": 0.18,
        }

    def fake_reposition(
        *,
        route_distance_km: float,
        eta_min: float,
        fuel_price_eur_l: float,
        vehicle_consumption_l_100km: float,
        target_hourly_net_eur: float,
        traffic_factor: float,
        forecast_volatility: float,
    ):
        _ = target_hourly_net_eur, traffic_factor, forecast_volatility
        travel_cost = route_distance_km * (vehicle_consumption_l_100km / 100.0) * fuel_price_eur_l
        time_cost = (eta_min / 60.0) * 1.8
        total = travel_cost + time_cost
        return {
            "travel_cost_eur": travel_cost,
            "time_cost_eur": time_cost,
            "risk_cost_eur": 0.0,
            "reposition_total_cost_eur": total,
            "reposition_cost_penalty": min(total / 12.0, 1.0),
        }

    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_load_zones)
    monkeypatch.setattr(router, "_estimate_reposition_leg", fake_leg)
    monkeypatch.setattr(router, "_forecast_zone_metrics", fake_forecast)
    monkeypatch.setattr(router, "_reposition_cost_model", fake_reposition)

    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()

    with TestClient(app) as client:
        response = client.get(
            "/copilot/driver/drv_demo_001/shift-plan",
            params={
                "horizon_min": 60,
                "top_k": 3,
                "lat": 40.758,
                "lon": -73.9855,
                "distance_weight": 0.3,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["driver_id"] == "drv_demo_001"
    assert isinstance(payload.get("origin_lat"), float)
    assert isinstance(payload.get("origin_lon"), float)
    assert payload["horizon_min"] == 60
    assert payload["count"] >= 3
    assert len(payload["items"]) >= 3
    assert [item["rank"] for item in payload["items"][:3]] == [1, 2, 3]
    assert all(str(item.get("why_now", "")).strip() for item in payload["items"][:3])
    assert payload["items"][0]["shift_score"] >= payload["items"][1]["shift_score"]


def test_shift_plan_endpoint_falls_back_cleanly_when_no_zone_data(monkeypatch):
    async def fake_load_zones(_redis):
        return []

    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_load_zones)

    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()

    with TestClient(app) as client:
        response = client.get(
            "/copilot/driver/drv_demo_001/shift-plan",
            params={
                "horizon_min": 90,
                "top_k": 3,
                "lat": 40.758,
                "lon": -73.9855,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 0
    assert payload["items"] == []


def test_shift_plan_endpoint_rejects_invalid_horizon(monkeypatch):
    async def fake_load_zones(_redis):
        return []

    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_load_zones)

    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()

    with TestClient(app) as client:
        response = client.get(
            "/copilot/driver/drv_demo_001/shift-plan",
            params={
                "horizon_min": 75,
                "top_k": 3,
                "lat": 40.758,
                "lon": -73.9855,
            },
        )

    assert response.status_code == 422
