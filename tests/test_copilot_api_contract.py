"""API response-contract tests for copilot scoring models."""
from __future__ import annotations

import sys
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from copilot_router import (  # noqa: E402
    RankedOfferItem,
    ScoreOfferResponse,
    ShiftPlanResponse,
    ShiftPlanZoneItem,
)


def test_score_offer_response_backward_compatible_without_explanation_details():
    payload = ScoreOfferResponse(
        offer_id="A1",
        courier_id="C1",
        accept_score=0.71,
        decision="accept",
        decision_threshold=0.53,
        eur_per_hour_net=22.4,
        estimated_net_eur=7.2,
        target_hourly_net_eur=18.0,
        target_gap_eur_h=4.4,
        costs={"estimated_net_eur_h": 22.4},
        route_source="estimated",
        route_distance_km=3.1,
        route_duration_min=18.0,
        route_notes=[],
        model_used="heuristic",
        explanation=["high_estimated_net_revenue"],
    )
    data = payload.model_dump()
    assert "accept_score" in data
    assert "decision" in data
    assert "decision_threshold" in data
    assert "explanation" in data
    assert data.get("explanation_details") is None


def test_score_offer_response_accepts_optional_explanation_details():
    payload = ScoreOfferResponse(
        offer_id="A2",
        courier_id="C2",
        accept_score=0.55,
        decision="reject",
        decision_threshold=0.61,
        eur_per_hour_net=11.8,
        estimated_net_eur=3.0,
        target_hourly_net_eur=18.0,
        target_gap_eur_h=-6.2,
        costs={"estimated_net_eur_h": 11.8},
        route_source="osrm",
        route_distance_km=6.2,
        route_duration_min=34.0,
        route_notes=["osrm_pickup_leg_unavailable"],
        model_used="ml",
        explanation=["below_target_hourly_goal"],
        explanation_details=[
            {
                "code": "net_hourly",
                "label": "Net hourly yield",
                "impact": "negative",
                "value": 11.8,
                "unit": "eur_per_hour",
                "source": "cost",
            }
        ],
    )
    data = payload.model_dump()
    assert isinstance(data["explanation_details"], list)
    assert data["explanation_details"][0]["code"] == "net_hourly"
    assert data["decision_threshold"] == 0.61


def test_ranked_offer_item_backward_compatible_with_optional_details():
    item = RankedOfferItem(
        rank=1,
        top_pick=True,
        recommendation="top_pick",
        offer_id="R1",
        courier_id="C1",
        accept_score=0.78,
        decision="accept",
        decision_threshold=0.5,
        eur_per_hour_net=25.0,
        estimated_net_eur=9.0,
        delta_vs_top_eur_h=0.0,
        delta_vs_median_eur_h=6.0,
        costs={"estimated_net_eur_h": 25.0},
        route_source="estimated",
        route_distance_km=2.4,
        route_duration_min=14.0,
        model_used="heuristic",
        explanation=["high_estimated_net_revenue"],
        explanation_details=[
            {
                "code": "target_gap",
                "label": "Target gap",
                "impact": "positive",
                "value": 4.0,
                "unit": "eur_per_hour",
                "source": "target",
            }
        ],
    )
    dumped = item.model_dump()
    assert dumped["recommendation"] == "top_pick"
    assert dumped["explanation_details"][0]["source"] == "target"
    assert dumped["decision_threshold"] == 0.5


def test_shift_plan_zone_item_contract_fields():
    item = ShiftPlanZoneItem(
        rank=1,
        zone_id="40.7580_-73.9855",
        zone_lat=40.7580,
        zone_lon=-73.9855,
        shift_score=1.214,
        confidence=0.78,
        estimated_net_eur_h=25.4,
        estimated_gross_eur_h=28.2,
        net_gain_vs_target_eur_h=7.4,
        horizon_min=60,
        why_now="Strong forecast pressure in next 60 min.",
        reasons=["Strong forecast pressure", "Quick reposition"],
        demand_index=1.62,
        supply_index=0.91,
        weather_factor=1.03,
        traffic_factor=1.08,
        event_pressure=0.17,
        temporal_pressure=0.12,
        forecast_pressure_ratio=1.34,
        forecast_volatility=0.18,
        distance_km=2.5,
        eta_min=9.4,
        reposition_total_cost_eur=2.1,
        context_fallback_applied=False,
        freshness_policy="stale_neutral_v1",
    )
    dumped = item.model_dump()
    assert dumped["rank"] == 1
    assert dumped["why_now"]
    assert dumped["horizon_min"] == 60
    assert dumped["confidence"] == 0.78
    assert dumped["demand_index"] == 1.62


def test_shift_plan_response_contract_is_backward_safe():
    payload = ShiftPlanResponse(
        driver_id="drv_demo_001",
        origin_lat=40.758,
        origin_lon=-73.9855,
        horizon_min=90,
        generated_at="2026-04-19T09:12:00+00:00",
        target_hourly_net_eur=18.0,
        source="zone_context_v2",
        count=1,
        items=[
            ShiftPlanZoneItem(
                rank=1,
                zone_id="40.7611_-73.9776",
                zone_lat=40.7611,
                zone_lon=-73.9776,
                shift_score=1.02,
                confidence=0.64,
                estimated_net_eur_h=21.8,
                estimated_gross_eur_h=24.2,
                net_gain_vs_target_eur_h=3.8,
                horizon_min=90,
                why_now="Healthy demand/supply outlook over 90 min.",
                reasons=["Healthy demand/supply outlook"],
                demand_index=1.4,
                supply_index=0.95,
                weather_factor=1.0,
                traffic_factor=1.12,
                event_pressure=0.09,
                temporal_pressure=0.07,
                forecast_pressure_ratio=1.18,
                forecast_volatility=0.22,
                distance_km=3.2,
                eta_min=11.6,
                reposition_total_cost_eur=2.8,
                context_fallback_applied=False,
                freshness_policy="stale_neutral_v1",
            )
        ],
    )
    out = payload.model_dump()
    assert out["driver_id"] == "drv_demo_001"
    assert out["origin_lat"] == 40.758
    assert out["origin_lon"] == -73.9855
    assert out["horizon_min"] == 90
    assert out["count"] == 1
    assert isinstance(out["items"], list)
    assert out["items"][0]["zone_id"] == "40.7611_-73.9776"
