from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

import copilot_router as router  # noqa: E402


class _FakeRedis:
    def __init__(self, hashes: dict[str, dict[str, str]] | None = None):
        self.hashes = {k: dict(v) for k, v in (hashes or {}).items()}

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str, mapping: dict[str, object]) -> int:
        bucket = self.hashes.setdefault(key, {})
        for map_key, value in mapping.items():
            bucket[str(map_key)] = str(value)
        return len(mapping)

    async def scan_iter(self, match: str | None = None, count: int | None = None):
        _ = count
        prefix = None
        if isinstance(match, str) and match.endswith("*"):
            prefix = match[:-1]
        for key in self.hashes:
            if prefix is None or key.startswith(prefix):
                yield key


def test_driver_profile_default_and_update_persistence():
    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()

    with TestClient(app) as client:
        default_resp = client.get("/copilot/driver/drv_demo_001/profile")
        assert default_resp.status_code == 200
        default_payload = default_resp.json()
        assert default_payload["driver_id"] == "drv_demo_001"
        assert default_payload["source"] == "default"

        update_resp = client.put(
            "/copilot/driver/drv_demo_001/profile",
            json={
                "target_eur_h": 22.5,
                "vehicle_mpg": 28.7,
                "aversion_risque": 0.71,
                "max_eta": 14.0,
            },
        )
        assert update_resp.status_code == 200
        update_payload = update_resp.json()
        assert update_payload["source"] == "manual"
        assert update_payload["target_eur_h"] == 22.5
        assert update_payload["vehicle_mpg"] == 28.7
        assert update_payload["aversion_risque"] == 0.71
        assert update_payload["max_eta"] == 14.0

        read_back_resp = client.get("/copilot/driver/drv_demo_001/profile")
        assert read_back_resp.status_code == 200
        read_back = read_back_resp.json()
        assert read_back["source"] == "manual"
        assert read_back["target_eur_h"] == 22.5
        assert read_back["vehicle_mpg"] == 28.7
        assert read_back["aversion_risque"] == 0.71
        assert read_back["max_eta"] == 14.0


def test_driver_profile_partial_update_preserves_existing_values():
    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()

    with TestClient(app) as client:
        first_update = client.put(
            "/copilot/driver/drv_demo_002/profile",
            json={
                "target_eur_h": 20.0,
                "vehicle_mpg": 26.7,
                "aversion_risque": 0.65,
                "max_eta": 16.0,
            },
        )
        assert first_update.status_code == 200

        second_update = client.put(
            "/copilot/driver/drv_demo_002/profile",
            json={
                "max_eta": 12.0,
            },
        )
        assert second_update.status_code == 200
        payload = second_update.json()
        assert payload["target_eur_h"] == 20.0
        assert payload["vehicle_mpg"] == 26.7
        assert payload["aversion_risque"] == 0.65
        assert payload["max_eta"] == 12.0


def test_driver_profile_blank_driver_id_returns_422():
    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()

    with TestClient(app) as client:
        get_resp = client.get("/copilot/driver/%20/profile")
        put_resp = client.put("/copilot/driver/%20/profile", json={"target_eur_h": 20.0})

    assert get_resp.status_code == 422
    assert put_resp.status_code == 422
    assert "driver_id" in str(get_resp.json().get("detail", "")).lower()
    assert "driver_id" in str(put_resp.json().get("detail", "")).lower()


def test_rank_offers_changes_with_driver_profile_consumption():
    redis = _FakeRedis(
        {
            router._driver_profile_key("drv_low_consumption"): {
                "target_eur_h": "18.0",
                "vehicle_mpg": "47.0",
                "aversion_risque": "0.3",
                "max_eta": "20",
                "source": "manual",
            },
            router._driver_profile_key("drv_high_consumption"): {
                "target_eur_h": "18.0",
                "vehicle_mpg": "10.0",
                "aversion_risque": "0.3",
                "max_eta": "20",
                "source": "manual",
            },
        }
    )
    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = redis
    app.state.copilot_model = None

    base_offers = [
        {
            "offer_id": "offer_long_distance",
            "estimated_fare_eur": 26.0,
            "estimated_distance_km": 20.0,
            "estimated_duration_min": 38.0,
            "distance_to_pickup_km": 2.0,
            "eta_to_pickup_min": 4.0,
            "demand_index": 1.2,
            "supply_index": 1.0,
            "weather_factor": 1.0,
            "traffic_factor": 1.0,
        },
        {
            "offer_id": "offer_short_distance",
            "estimated_fare_eur": 16.0,
            "estimated_distance_km": 4.0,
            "estimated_duration_min": 28.0,
            "distance_to_pickup_km": 1.0,
            "eta_to_pickup_min": 2.0,
            "demand_index": 1.2,
            "supply_index": 1.0,
            "weather_factor": 1.0,
            "traffic_factor": 1.0,
        },
    ]

    with TestClient(app) as client:
        low_resp = client.post(
            "/copilot/rank-offers",
            json={
                "offers": [{**offer, "courier_id": "drv_low_consumption"} for offer in base_offers],
                "rank_by": "eur_per_hour_net",
            },
        )
        high_resp = client.post(
            "/copilot/rank-offers",
            json={
                "offers": [{**offer, "courier_id": "drv_high_consumption"} for offer in base_offers],
                "rank_by": "eur_per_hour_net",
            },
        )

    assert low_resp.status_code == 200
    assert high_resp.status_code == 200
    low_top = low_resp.json()["items"][0]["offer_id"]
    high_top = high_resp.json()["items"][0]["offer_id"]
    assert low_top != high_top


def test_rank_offers_objective_weights_change_top_pick():
    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = _FakeRedis()
    app.state.copilot_model = None

    offers = [
        {
            "offer_id": "gain_heavy",
            "courier_id": "drv_obj_001",
            "estimated_fare_eur": 38.0,
            "estimated_distance_km": 20.0,
            "estimated_duration_min": 35.0,
            "distance_to_pickup_km": 3.5,
            "eta_to_pickup_min": 5.0,
            "demand_index": 1.5,
            "supply_index": 0.9,
            "fuel_price_usd_gallon": 3.8,
            "vehicle_mpg": 21.4,
            "target_hourly_net_eur": 16.0,
        },
        {
            "offer_id": "efficient",
            "courier_id": "drv_obj_001",
            "estimated_fare_eur": 16.0,
            "estimated_distance_km": 4.5,
            "estimated_duration_min": 23.0,
            "distance_to_pickup_km": 0.5,
            "eta_to_pickup_min": 2.0,
            "demand_index": 1.2,
            "supply_index": 1.0,
            "fuel_price_usd_gallon": 3.8,
            "vehicle_mpg": 31.4,
            "target_hourly_net_eur": 16.0,
        },
    ]

    with TestClient(app) as client:
        gain_first = client.post(
            "/copilot/rank-offers",
            json={
                "offers": offers,
                "rank_by": "objective_score",
                "w_gain": 100.0,
                "w_time": 0.0,
                "w_fuel": 0.0,
            },
        )
        time_fuel_first = client.post(
            "/copilot/rank-offers",
            json={
                "offers": offers,
                "rank_by": "objective_score",
                "w_gain": 0.0,
                "w_time": 65.0,
                "w_fuel": 35.0,
            },
        )

    assert gain_first.status_code == 200
    assert time_fuel_first.status_code == 200
    gain_top = gain_first.json()["items"][0]["offer_id"]
    time_fuel_top = time_fuel_first.json()["items"][0]["offer_id"]
    assert gain_top != time_fuel_top


def test_instant_dispatch_uses_profile_when_query_params_not_overridden(monkeypatch):
    async def fake_best_offers_around(*_args, **_kwargs):
        return {
            "offers": [
                {
                    "offer_id": "stay_1",
                    "zone_id": "40.7600_-73.9800",
                    "recommendation_action": "consider",
                    "recommendation_score": 0.33,
                    "accept_score": 0.42,
                    "eur_per_hour_net": 13.0,
                    "target_gap_eur_h": -4.0,
                    "distance_to_pickup_km": 1.2,
                    "route_duration_min": 11.0,
                    "route_source": "estimated",
                    "explanation": ["below_target_hourly_goal"],
                    "recommendation_signals": ["low_estimated_net_revenue"],
                }
            ]
        }

    async def fake_next_best_zone(*_args, **_kwargs):
        return {
            "recommendations": [
                {
                    "zone_id": "40.7400_-73.9900",
                    "demand_index": 1.45,
                    "supply_index": 0.92,
                    "weather_factor": 1.0,
                    "traffic_factor": 1.12,
                    "gbfs_demand_boost": 0.06,
                    "demand_trend_ema": 0.05,
                    "opportunity_score": 1.25,
                }
            ]
        }

    async def fake_estimate_leg(
        *,
        origin_lat: float,
        origin_lon: float,
        dest_lat: float,
        dest_lon: float,
        traffic_factor: float,
        use_osrm: bool,
        osrm_client,
    ):
        _ = origin_lat, origin_lon, dest_lat, dest_lon, traffic_factor, use_osrm, osrm_client
        return 3.2, 10.4, "estimated"

    monkeypatch.setattr(router, "best_offers_around", fake_best_offers_around)
    monkeypatch.setattr(router, "next_best_zone", fake_next_best_zone)
    monkeypatch.setattr(router, "_estimate_reposition_leg", fake_estimate_leg)

    redis = _FakeRedis(
        {
            router._driver_profile_key("drv_profile_001"): {
                "target_eur_h": "21.0",
                "vehicle_mpg": "25.8",
                "aversion_risque": "0.9",
                "max_eta": "14.0",
                "source": "manual",
            },
            f"{router.COURIER_HASH_PREFIX}drv_profile_001": {
                "lat": "40.7580",
                "lon": "-73.9855",
            },
            router.FUEL_CONTEXT_KEY: {
                "fuel_price_usd_gallon": "3.64",
            },
        }
    )

    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = redis

    with TestClient(app) as client:
        resp = client.get(
            "/copilot/driver/drv_profile_001/instant-dispatch",
            params={
                "around_radius_km": 4.0,
                "around_limit": 1,
                "scan_limit": 20,
                "zone_top_k": 1,
                "min_accept_score": 0.0,
            },
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["dispatch_strategy"] == "conservative"
    assert payload["target_hourly_net_eur"] == 21.0
    assert payload["max_reposition_eta_min"] == 14.0
    assert payload["driver_profile"]["aversion_risque"] == 0.9


def test_next_best_zone_is_safe_when_called_internally_without_optional_query_args(monkeypatch):
    async def fake_load_zone_context_payloads(_redis):
        return [
            {
                "zone_id": "40.7615_-73.9777",
                "demand_index": 1.35,
                "supply_index": 0.92,
                "weather_factor": 1.0,
                "traffic_factor": 1.08,
                "opportunity_score": 1.58,
            }
        ]

    monkeypatch.setattr(router, "_load_zone_context_payloads", fake_load_zone_context_payloads)

    redis = _FakeRedis(
        {
            f"{router.COURIER_HASH_PREFIX}drv_demo_011": {
                "lat": "40.7580",
                "lon": "-73.9855",
            },
            router.FUEL_CONTEXT_KEY: {
                "fuel_price_usd_gallon": "3.64",
            },
        }
    )

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(redis=redis)))
    payload = asyncio.run(router.next_best_zone(request, "drv_demo_011"))  # type: ignore[arg-type]

    assert payload["driver_id"] == "drv_demo_011"
    assert payload["count"] == 1
    assert payload["home_lat"] is None
    assert payload["home_lon"] is None
    assert payload["recommendations"][0]["zone_id"] == "40.7615_-73.9777"


def test_instant_dispatch_uses_live_source_when_demo_driver_feed_is_missing(monkeypatch):
    async def fake_select_driver_brief_context(_redis, requested_driver_id: str):
        return {
            "requested_driver_id": requested_driver_id,
            "source_driver_id": "drv_live_900",
            "alias_active": True,
            "requested_summary": {"driver_id": requested_driver_id, "available": False, "offer_count": 0},
            "source_summary": {"driver_id": "drv_live_900", "available": True, "offer_count": 2},
            "note": "Live demo alias",
        }

    async def fake_best_offers_around(*_args, **_kwargs):
        return {
            "offers": [
                {
                    "offer_id": "stay_1",
                    "zone_id": "40.7600_-73.9800",
                    "recommendation_action": "consider",
                    "recommendation_score": 0.41,
                    "accept_score": 0.58,
                    "eur_per_hour_net": 16.5,
                    "target_gap_eur_h": -1.5,
                    "distance_to_pickup_km": 1.2,
                    "route_duration_min": 9.0,
                    "route_source": "estimated",
                    "explanation": ["close_pickup"],
                    "recommendation_signals": ["good_local_fit"],
                }
            ]
        }

    async def fake_next_best_zone(*_args, **_kwargs):
        return {
            "recommendations": [
                {
                    "zone_id": "40.7400_-73.9900",
                    "demand_index": 1.45,
                    "supply_index": 0.92,
                    "weather_factor": 1.0,
                    "traffic_factor": 1.12,
                    "gbfs_demand_boost": 0.06,
                    "demand_trend_ema": 0.05,
                    "opportunity_score": 1.25,
                }
            ]
        }

    async def fake_estimate_leg(
        *,
        origin_lat: float,
        origin_lon: float,
        dest_lat: float,
        dest_lon: float,
        traffic_factor: float,
        use_osrm: bool,
        osrm_client,
    ):
        _ = origin_lat, origin_lon, dest_lat, dest_lon, traffic_factor, use_osrm, osrm_client
        return 2.6, 8.2, "estimated"

    monkeypatch.setattr(router, "_select_driver_brief_context", fake_select_driver_brief_context)
    monkeypatch.setattr(router, "best_offers_around", fake_best_offers_around)
    monkeypatch.setattr(router, "next_best_zone", fake_next_best_zone)
    monkeypatch.setattr(router, "_estimate_reposition_leg", fake_estimate_leg)

    redis = _FakeRedis(
        {
            router._driver_profile_key("drv_demo_011"): {
                "target_eur_h": "19.0",
                "vehicle_mpg": "30.0",
                "aversion_risque": "0.5",
                "max_eta": "18.0",
                "source": "manual",
            },
            f"{router.COURIER_HASH_PREFIX}drv_live_900": {
                "lat": "40.7580",
                "lon": "-73.9855",
            },
            router.FUEL_CONTEXT_KEY: {
                "fuel_price_usd_gallon": "3.64",
            },
        }
    )

    app = FastAPI()
    app.include_router(router.copilot_router)
    app.state.redis = redis

    with TestClient(app) as client:
        resp = client.get(
            "/copilot/driver/drv_demo_011/instant-dispatch",
            params={
                "around_radius_km": 4.0,
                "around_limit": 1,
                "scan_limit": 20,
                "zone_top_k": 1,
                "min_accept_score": 0.0,
                "use_osrm": False,
            },
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["driver_id"] == "drv_demo_011"
    assert payload["source_driver_id"] == "drv_live_900"
    assert payload["alias_active"] is True
    assert payload["origin"] == {"lat": 40.758, "lon": -73.9855}
