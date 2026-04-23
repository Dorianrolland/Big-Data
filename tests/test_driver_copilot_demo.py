from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import copilot_router as router  # noqa: E402


class _FakeRedis:
    def __init__(
        self,
        hashes: dict[str, dict[str, str]] | None = None,
        lists: dict[str, list[str]] | None = None,
    ):
        self.hashes = {key: dict(value) for key, value in (hashes or {}).items()}
        self.lists = {key: list(value) for key, value in (lists or {}).items()}

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = list(self.lists.get(key, []))
        if end < 0:
            end = len(values) - 1
        return values[start : end + 1]

    async def scan_iter(self, match: str | None = None, count: int | None = None):  # noqa: ANN001
        _ = count
        prefix = None
        if match and match.endswith("*"):
            prefix = match[:-1]
        for key in self.hashes:
            if prefix is None or key.startswith(prefix):
                yield key


def _app(redis: _FakeRedis | None = None) -> FastAPI:
    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = redis or _FakeRedis()
    app.state.copilot_model = None
    app.state.copilot_model_quality_gate = {"accepted": True}
    return app


def test_driver_app_page_served():
    with TestClient(_app()) as client:
        response = client.get("/copilot/driver-app")

    assert response.status_code == 200
    assert "FleetStream Driver Copilot" in response.text
    assert "Demo login" in response.text
    assert "Quick onboarding" in response.text


def test_driver_copilot_brief_prefers_best_offer_and_enriches_blocks(monkeypatch):
    async def fake_profile(_request, driver_id):  # noqa: ANN001
        return {
            "driver_id": driver_id,
            "target_eur_h": 18.0,
            "consommation_l_100": 7.0,
            "aversion_risque": 0.5,
            "max_eta": 16.0,
            "source": "manual",
        }

    async def fake_position(_request, driver_id, include_zone_context=False):  # noqa: ANN001
        _ = include_zone_context
        return {
            "driver_id": driver_id,
            "available": True,
            "status": "available",
            "lat": 40.758,
            "lon": -73.9855,
            "zone_id": "40.758_-73.986",
            "zone_context": {"traffic_factor": 1.02},
        }

    async def fake_best_offers(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {
            "offers": [
                {
                    "offer_id": "offer_001",
                    "zone_id": "40.761_-73.982",
                    "accept_score": 0.83,
                    "recommendation_score": 0.89,
                    "recommendation_action": "accept",
                    "eur_per_hour_net": 24.6,
                    "estimated_net_eur": 8.8,
                    "target_gap_eur_h": 5.1,
                    "distance_to_pickup_km": 0.9,
                    "eta_to_pickup_min": 4.1,
                    "route_duration_min": 15.2,
                    "route_distance_km": 4.6,
                    "route_source": "osrm",
                    "reason_codes": ["GAIN_STRONG", "TIME_EFFICIENT"],
                    "costs": {"fuel_cost_eur": 1.24},
                    "explanation": ["above_target_hourly_goal"],
                    "explanation_details": [
                        {"label": "Net hourly yield", "impact": "positive"},
                    ],
                }
            ]
        }

    async def fake_driver_offers(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"offers": []}

    async def fake_dispatch(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {
            "reposition_option": {
                "zone_id": "40.755_-73.99",
                "zone_lat": 40.755,
                "zone_lon": -73.99,
                "dispatch_score": 0.62,
                "risk_adjusted_potential_eur_h": 19.2,
                "net_gain_vs_stay_eur_h": 1.3,
                "travel_cost_eur": 1.1,
                "eta_min": 9.0,
                "route_distance_km": 2.7,
                "route_source": "osrm",
                "traffic_factor": 1.08,
                "forecast_pressure_ratio": 1.24,
            }
        }

    async def fake_next_zone(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {
            "recommendations": [
                {"zone_id": "40.755_-73.99", "lat": 40.755, "lon": -73.99, "traffic_factor": 1.08}
            ]
        }

    async def fake_shift(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {
            "count": 1,
            "items": [
                {
                    "rank": 1,
                    "zone_id": "40.755_-73.99",
                    "shift_score": 1.18,
                    "estimated_net_eur_h": 21.2,
                    "eta_min": 8.6,
                    "confidence": 0.82,
                    "why_now": "Strong forecast pressure in the next hour.",
                    "reasons": ["Strong forecast pressure in the next hour."],
                    "context_fallback_applied": False,
                }
            ],
        }

    async def fake_fuel(_request):  # noqa: ANN001
        return {"fuel_price_eur_l": 1.81, "fuel_sync_status": "ok"}

    async def fake_health(_request):  # noqa: ANN001
        return {
            "model_quality_gate": {"accepted": True},
            "fuel_context": {"fuel_sync_status": "ok"},
            "data_quality": {"stale_sources_count": 0},
            "tlc_replay": {
                "route_linear_fallback": "0",
                "route_hold_fallback": "0",
                "route_osrm_errors": "0",
            },
        }

    async def fake_zones(_redis):  # noqa: ANN001
        return [
            {"zone_id": "40.761_-73.982", "demand_index": 1.7, "supply_index": 0.8, "traffic_factor": 1.02},
            {"zone_id": "40.72_-73.99", "demand_index": 0.7, "supply_index": 1.5, "traffic_factor": 1.0},
        ]

    monkeypatch.setattr(router, "get_driver_profile", fake_profile)
    monkeypatch.setattr(router, "driver_position", fake_position)
    monkeypatch.setattr(router, "best_offers_around", fake_best_offers)
    monkeypatch.setattr(router, "driver_offers", fake_driver_offers)
    monkeypatch.setattr(router, "instant_dispatch", fake_dispatch)
    monkeypatch.setattr(router, "next_best_zone", fake_next_zone)
    monkeypatch.setattr(router, "shift_plan", fake_shift)
    monkeypatch.setattr(router, "fuel_context", fake_fuel)
    monkeypatch.setattr(router, "copilot_health", fake_health)
    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_zones)

    redis = _FakeRedis(
        {
            f"{router.OFFER_KEY_PREFIX}offer_001": {
                "pickup_lat": "40.7612",
                "pickup_lon": "-73.9819",
                "dropoff_lat": "40.7478",
                "dropoff_lon": "-73.9712",
            }
        }
    )

    with TestClient(_app(redis)) as client:
        response = client.get("/copilot/driver/drv_demo_001/copilot-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["driver"]["driver_id"] == "drv_demo_001"
    assert payload["primary_recommendation"]["kind"] == "offer"
    assert payload["primary_recommendation"]["pickup"]["lat"] == 40.7612
    assert payload["primary_recommendation"]["dropoff"]["lon"] == -73.9712
    assert payload["zones"]["hot_zones"][0]["zone_id"] == "40.761_-73.982"
    assert payload["shift"]["count"] == 1
    assert payload["system"]["stale"] is False


def test_driver_copilot_brief_falls_back_to_reposition(monkeypatch):
    async def fake_profile(_request, driver_id):  # noqa: ANN001
        return {
            "driver_id": driver_id,
            "target_eur_h": 18.0,
            "consommation_l_100": 7.0,
            "aversion_risque": 0.5,
            "max_eta": 16.0,
            "source": "manual",
        }

    async def fake_position(_request, driver_id, include_zone_context=False):  # noqa: ANN001
        _ = driver_id, include_zone_context
        return {"available": True, "status": "available", "lat": 40.758, "lon": -73.9855}

    async def fake_empty(*_args, **_kwargs):  # noqa: ANN001
        return {"offers": []}

    async def fake_dispatch(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {
            "reposition_option": {
                "zone_id": "40.755_-73.99",
                "zone_lat": 40.755,
                "zone_lon": -73.99,
                "dispatch_score": 0.74,
                "risk_adjusted_potential_eur_h": 20.8,
                "net_gain_vs_stay_eur_h": 2.6,
                "travel_cost_eur": 1.0,
                "eta_min": 8.0,
                "route_distance_km": 2.3,
                "route_source": "osrm",
                "traffic_factor": 1.04,
                "forecast_pressure_ratio": 1.31,
            },
            "reposition_candidates": [],
        }

    async def fake_next_zone(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"recommendations": []}

    async def fake_shift(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"count": 0, "items": []}

    async def fake_fuel(_request):  # noqa: ANN001
        return {"fuel_price_eur_l": 1.81, "fuel_sync_status": "ok"}

    async def fake_health(_request):  # noqa: ANN001
        return {
            "model_quality_gate": {"accepted": True},
            "fuel_context": {"fuel_sync_status": "ok"},
            "data_quality": {"stale_sources_count": 0},
            "tlc_replay": {"route_linear_fallback": "0", "route_hold_fallback": "0", "route_osrm_errors": "0"},
        }

    async def fake_zones(_redis):  # noqa: ANN001
        return []

    monkeypatch.setattr(router, "get_driver_profile", fake_profile)
    monkeypatch.setattr(router, "driver_position", fake_position)
    monkeypatch.setattr(router, "best_offers_around", fake_empty)
    monkeypatch.setattr(router, "driver_offers", fake_empty)
    monkeypatch.setattr(router, "instant_dispatch", fake_dispatch)
    monkeypatch.setattr(router, "next_best_zone", fake_next_zone)
    monkeypatch.setattr(router, "shift_plan", fake_shift)
    monkeypatch.setattr(router, "fuel_context", fake_fuel)
    monkeypatch.setattr(router, "copilot_health", fake_health)
    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_zones)

    with TestClient(_app()) as client:
        response = client.get("/copilot/driver/drv_demo_001/copilot-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["primary_recommendation"]["kind"] == "reposition"
    assert payload["primary_recommendation"]["zone_id"] == "40.755_-73.99"


def test_driver_copilot_brief_falls_back_to_hold_and_stays_up(monkeypatch):
    async def fake_profile(_request, driver_id):  # noqa: ANN001
        return {
            "driver_id": driver_id,
            "target_eur_h": 18.0,
            "consommation_l_100": 7.0,
            "aversion_risque": 0.5,
            "max_eta": 16.0,
            "source": "manual",
        }

    async def fake_position(_request, driver_id, include_zone_context=False):  # noqa: ANN001
        _ = driver_id, include_zone_context
        return {"available": False, "status": "offline", "lat": None, "lon": None}

    async def fake_best_offers(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=404, detail="Position missing")

    async def fake_driver_offers(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"offers": []}

    async def fake_dispatch(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=503, detail="dispatch unavailable")

    async def fake_next_zone(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {
            "recommendations": [
                {"zone_id": "40.761_-73.982", "lat": 40.761, "lon": -73.982, "traffic_factor": 1.05}
            ]
        }

    async def fake_shift(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="shift timeout")

    async def fake_fuel(_request):  # noqa: ANN001
        return {"fuel_price_eur_l": 1.81, "fuel_sync_status": "cached"}

    async def fake_health(_request):  # noqa: ANN001
        return {
            "model_quality_gate": {"accepted": True},
            "fuel_context": {"fuel_sync_status": "cached"},
            "data_quality": {"stale_sources_count": 3},
            "tlc_replay": {"route_linear_fallback": "0", "route_hold_fallback": "0", "route_osrm_errors": "0"},
        }

    async def fake_zones(_redis):  # noqa: ANN001
        return [{"zone_id": "40.761_-73.982", "demand_index": 1.5, "supply_index": 0.8, "traffic_factor": 1.05}]

    monkeypatch.setattr(router, "get_driver_profile", fake_profile)
    monkeypatch.setattr(router, "driver_position", fake_position)
    monkeypatch.setattr(router, "best_offers_around", fake_best_offers)
    monkeypatch.setattr(router, "driver_offers", fake_driver_offers)
    monkeypatch.setattr(router, "instant_dispatch", fake_dispatch)
    monkeypatch.setattr(router, "next_best_zone", fake_next_zone)
    monkeypatch.setattr(router, "shift_plan", fake_shift)
    monkeypatch.setattr(router, "fuel_context", fake_fuel)
    monkeypatch.setattr(router, "copilot_health", fake_health)
    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_zones)

    with TestClient(_app()) as client:
        response = client.get("/copilot/driver/drv_demo_001/copilot-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["primary_recommendation"]["kind"] == "hold"
    assert payload["system"]["stale"] is True
    assert payload["shift"]["stale"] is True


def test_driver_copilot_brief_hot_and_calm_zones_are_sorted(monkeypatch):
    async def fake_profile(_request, driver_id):  # noqa: ANN001
        return {
            "driver_id": driver_id,
            "target_eur_h": 18.0,
            "consommation_l_100": 7.0,
            "aversion_risque": 0.5,
            "max_eta": 16.0,
            "source": "manual",
        }

    async def fake_position(_request, driver_id, include_zone_context=False):  # noqa: ANN001
        _ = driver_id, include_zone_context
        return {"available": True, "status": "available", "lat": 40.758, "lon": -73.9855}

    async def fake_empty(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"offers": [], "recommendations": []}

    async def fake_dispatch(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"reposition_option": None, "reposition_candidates": []}

    async def fake_shift(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"count": 0, "items": []}

    async def fake_fuel(_request):  # noqa: ANN001
        return {"fuel_price_eur_l": 1.81, "fuel_sync_status": "ok"}

    async def fake_health(_request):  # noqa: ANN001
        return {
            "model_quality_gate": {"accepted": True},
            "fuel_context": {"fuel_sync_status": "ok"},
            "data_quality": {"stale_sources_count": 0},
            "tlc_replay": {"route_linear_fallback": "0", "route_hold_fallback": "0", "route_osrm_errors": "0"},
        }

    async def fake_zones(_redis):  # noqa: ANN001
        return [
            {"zone_id": "40.761_-73.982", "demand_index": 1.8, "supply_index": 0.6, "traffic_factor": 1.0},
            {"zone_id": "40.755_-73.99", "demand_index": 1.2, "supply_index": 0.8, "traffic_factor": 1.0},
            {"zone_id": "40.72_-73.99", "demand_index": 0.6, "supply_index": 1.6, "traffic_factor": 1.0},
        ]

    monkeypatch.setattr(router, "get_driver_profile", fake_profile)
    monkeypatch.setattr(router, "driver_position", fake_position)
    monkeypatch.setattr(router, "best_offers_around", fake_empty)
    monkeypatch.setattr(router, "driver_offers", fake_empty)
    monkeypatch.setattr(router, "instant_dispatch", fake_dispatch)
    monkeypatch.setattr(router, "next_best_zone", fake_empty)
    monkeypatch.setattr(router, "shift_plan", fake_shift)
    monkeypatch.setattr(router, "fuel_context", fake_fuel)
    monkeypatch.setattr(router, "copilot_health", fake_health)
    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_zones)

    with TestClient(_app()) as client:
        response = client.get("/copilot/driver/drv_demo_001/copilot-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["zones"]["hot_zones"][0]["zone_id"] == "40.761_-73.982"
    assert payload["zones"]["calm_zones"][0]["zone_id"] == "40.72_-73.99"


def test_driver_copilot_brief_aliases_demo_driver_to_live_driver_when_needed(monkeypatch):
    async def fake_profile(_request, driver_id):  # noqa: ANN001
        return {
            "driver_id": driver_id,
            "target_eur_h": 18.0,
            "consommation_l_100": 7.0,
            "aversion_risque": 0.5,
            "max_eta": 16.0,
            "source": "manual",
        }

    async def fake_position(_request, driver_id, include_zone_context=False):  # noqa: ANN001
        _ = include_zone_context
        if driver_id == "drv_demo_025":
            return {
                "driver_id": driver_id,
                "available": True,
                "status": "available",
                "lat": 40.758,
                "lon": -73.9855,
                "zone_id": "40.758_-73.986",
                "zone_context": {"traffic_factor": 1.01},
            }
        return {
            "driver_id": driver_id,
            "available": False,
            "status": "offline",
            "lat": None,
            "lon": None,
            "zone_id": None,
            "zone_context": None,
        }

    async def fake_best_offers(_request, driver_id, **_kwargs):  # noqa: ANN001
        assert driver_id == "drv_demo_025"
        return {
            "offers": [
                {
                    "offer_id": "offer_alias",
                    "zone_id": "40.761_-73.982",
                    "accept_score": 0.91,
                    "recommendation_score": 0.94,
                    "recommendation_action": "accept",
                    "eur_per_hour_net": 28.4,
                    "estimated_net_eur": 9.6,
                    "target_gap_eur_h": 7.4,
                    "distance_to_pickup_km": 0.8,
                    "eta_to_pickup_min": 3.8,
                    "route_duration_min": 16.4,
                    "route_distance_km": 5.1,
                    "route_source": "osrm",
                    "reason_codes": ["GAIN_STRONG", "TIME_EFFICIENT"],
                    "costs": {"fuel_cost_eur": 1.12},
                    "explanation_details": [{"label": "Net hourly yield", "impact": "positive"}],
                }
            ]
        }

    async def fake_driver_offers(_request, driver_id, **_kwargs):  # noqa: ANN001
        return {"driver_id": driver_id, "offers": []}

    async def fake_dispatch(_request, driver_id, **_kwargs):  # noqa: ANN001
        return {"driver_id": driver_id, "reposition_option": None, "reposition_candidates": []}

    async def fake_next_zone(_request, driver_id, **_kwargs):  # noqa: ANN001
        return {"driver_id": driver_id, "recommendations": []}

    async def fake_shift(_request, driver_id, **_kwargs):  # noqa: ANN001
        return {"driver_id": driver_id, "count": 0, "items": []}

    async def fake_fuel(_request):  # noqa: ANN001
        return {"fuel_price_eur_l": 1.81, "fuel_sync_status": "ok"}

    async def fake_health(_request):  # noqa: ANN001
        return {
            "model_quality_gate": {"accepted": True},
            "fuel_context": {"fuel_sync_status": "ok"},
            "data_quality": {"stale_sources_count": 0},
            "tlc_replay": {"route_linear_fallback": "0", "route_hold_fallback": "0", "route_osrm_errors": "0"},
        }

    async def fake_zones(_redis):  # noqa: ANN001
        return []

    monkeypatch.setattr(router, "get_driver_profile", fake_profile)
    monkeypatch.setattr(router, "driver_position", fake_position)
    monkeypatch.setattr(router, "best_offers_around", fake_best_offers)
    monkeypatch.setattr(router, "driver_offers", fake_driver_offers)
    monkeypatch.setattr(router, "instant_dispatch", fake_dispatch)
    monkeypatch.setattr(router, "next_best_zone", fake_next_zone)
    monkeypatch.setattr(router, "shift_plan", fake_shift)
    monkeypatch.setattr(router, "fuel_context", fake_fuel)
    monkeypatch.setattr(router, "copilot_health", fake_health)
    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_zones)

    redis = _FakeRedis(
        hashes={
            f"{router.COURIER_HASH_PREFIX}drv_demo_001": {"lat": "0", "lon": "0", "status": "offline"},
            f"{router.COURIER_HASH_PREFIX}drv_demo_025": {
                "lat": "40.758",
                "lon": "-73.9855",
                "status": "available",
                "zone_id": "40.758_-73.986",
                "demand_index": "1.4",
                "supply_index": "0.7",
                "traffic_factor": "1.01",
            },
            f"{router.OFFER_KEY_PREFIX}offer_alias": {
                "pickup_lat": "40.7612",
                "pickup_lon": "-73.9819",
                "dropoff_lat": "40.7478",
                "dropoff_lon": "-73.9712",
            },
        },
        lists={
            f"{router.DRIVER_OFFERS_PREFIX}drv_demo_025:offers": ["offer_alias"],
        },
    )

    with TestClient(_app(redis)) as client:
        response = client.get("/copilot/driver/drv_demo_001/copilot-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["demo_context"]["alias_active"] is True
    assert payload["demo_context"]["source_driver_id"] == "drv_demo_025"
    assert payload["driver"]["position"]["source_driver_id"] == "drv_demo_025"
    assert payload["primary_recommendation"]["kind"] == "offer"
    assert payload["primary_recommendation"]["pickup"]["lat"] == 40.7612


def test_driver_copilot_brief_uses_gbfs_market_fallback_when_zone_contexts_are_missing(monkeypatch):
    async def fake_profile(_request, driver_id):  # noqa: ANN001
        return {
            "driver_id": driver_id,
            "target_eur_h": 18.0,
            "consommation_l_100": 7.0,
            "aversion_risque": 0.5,
            "max_eta": 16.0,
            "source": "manual",
        }

    async def fake_position(_request, driver_id, include_zone_context=False):  # noqa: ANN001
        _ = driver_id, include_zone_context
        return {"available": True, "status": "available", "lat": 40.758, "lon": -73.9855}

    async def fake_empty(_request, _driver_id, **_kwargs):  # noqa: ANN001
        return {"offers": [], "recommendations": []}

    async def fake_dispatch(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=503, detail="dispatch unavailable")

    async def fake_shift(*_args, **_kwargs):  # noqa: ANN001
        raise HTTPException(status_code=504, detail="shift timeout")

    async def fake_fuel(_request):  # noqa: ANN001
        return {"fuel_price_eur_l": 1.81, "fuel_sync_status": "ok"}

    async def fake_health(_request):  # noqa: ANN001
        return {
            "model_quality_gate": {"accepted": True},
            "fuel_context": {"fuel_sync_status": "ok"},
            "data_quality": {"stale_sources_count": 0},
            "tlc_replay": {"route_linear_fallback": "0", "route_hold_fallback": "0", "route_osrm_errors": "0"},
        }

    async def fake_zones(_redis):  # noqa: ANN001
        return []

    monkeypatch.setattr(router, "get_driver_profile", fake_profile)
    monkeypatch.setattr(router, "driver_position", fake_position)
    monkeypatch.setattr(router, "best_offers_around", fake_empty)
    monkeypatch.setattr(router, "driver_offers", fake_empty)
    monkeypatch.setattr(router, "instant_dispatch", fake_dispatch)
    monkeypatch.setattr(router, "next_best_zone", fake_empty)
    monkeypatch.setattr(router, "shift_plan", fake_shift)
    monkeypatch.setattr(router, "fuel_context", fake_fuel)
    monkeypatch.setattr(router, "copilot_health", fake_health)
    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_zones)

    redis = _FakeRedis(
        hashes={
            f"{router.COURIER_HASH_PREFIX}drv_demo_001": {
                "lat": "40.758",
                "lon": "-73.9855",
                "status": "available",
            },
            "copilot:context:gbfs:zone:40.720_-73.860": {
                "zone_id": "40.720_-73.860",
                "bikes_available": "0",
                "docks_available": "0",
                "occupancy_ratio": "0.0",
                "demand_boost": "0.5",
                "stations_count": "8",
            },
            "copilot:context:gbfs:zone:40.760_-73.980": {
                "zone_id": "40.760_-73.980",
                "bikes_available": "12",
                "docks_available": "22",
                "occupancy_ratio": "0.75",
                "demand_boost": "0.1",
                "stations_count": "11",
            },
        }
    )

    with TestClient(_app(redis)) as client:
        response = client.get("/copilot/driver/drv_demo_001/copilot-brief")

    assert response.status_code == 200
    payload = response.json()
    assert payload["zones"]["hot_zones"][0]["zone_id"] == "40.720_-73.860"
    assert payload["zones"]["calm_zones"][0]["zone_id"] == "40.760_-73.980"
    assert payload["zones"]["best_reposition"]["kind"] == "reposition"
    assert payload["shift"]["count"] >= 1


def test_driver_copilot_static_assets_exist():
    root = Path(__file__).resolve().parent.parent / "api" / "static" / "driver_app"
    html = (root / "index.html").read_text(encoding="utf-8")
    js = (root / "app.js").read_text(encoding="utf-8")
    manifest = (root / "manifest.webmanifest").read_text(encoding="utf-8")

    assert "FleetStream Driver Copilot" in html
    assert "Demo login" in html
    assert "Quick onboarding" in html
    assert 'data-tab="home"' in html
    assert 'data-tab="orders"' in html
    assert "PRESETS" in js
    assert "/copilot/driver/" in js
    assert "localStorage" in js
    assert '"start_url": "/copilot/driver-app"' in manifest
