"""API response-contract tests for copilot scoring models."""
from __future__ import annotations

import sys
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from copilot_router import (  # noqa: E402
    DriverProfileResponse,
    RankOffersResponse,
    RankedOfferItem,
    ScoreBreakdown,
    ScoreBreakdownDimension,
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
    assert data.get("score_breakdown") is None
    assert data.get("reason_codes") is None


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
    assert dumped.get("score_breakdown") is None
    assert dumped.get("reason_codes") is None


def test_score_offer_response_accepts_breakdown_and_reason_codes():
    payload = ScoreOfferResponse(
        offer_id="A3",
        courier_id="C3",
        accept_score=0.81,
        decision="accept",
        decision_threshold=0.54,
        eur_per_hour_net=23.7,
        estimated_net_eur=8.2,
        target_hourly_net_eur=18.0,
        target_gap_eur_h=5.7,
        costs={"estimated_net_eur_h": 23.7},
        route_source="estimated",
        route_distance_km=3.8,
        route_duration_min=16.0,
        route_notes=[],
        model_used="ml",
        explanation=["high_estimated_net_revenue"],
        score_breakdown=ScoreBreakdown(
            version="v2",
            total_score=0.8123,
            dimensions={
                "gain": ScoreBreakdownDimension(
                    label="Gain quality",
                    score=0.91,
                    weight=0.42,
                    contribution=0.3822,
                    impact="positive",
                ),
                "time": ScoreBreakdownDimension(
                    label="Time efficiency",
                    score=0.72,
                    weight=0.2,
                    contribution=0.144,
                    impact="positive",
                ),
                "fuel": ScoreBreakdownDimension(
                    label="Fuel efficiency",
                    score=0.68,
                    weight=0.18,
                    contribution=0.1224,
                    impact="positive",
                ),
                "risk": ScoreBreakdownDimension(
                    label="Risk resilience",
                    score=0.82,
                    weight=0.2,
                    contribution=0.164,
                    impact="positive",
                ),
            },
        ),
        reason_codes=["GAIN_STRONG", "RISK_LOW", "DECISION_CONFIDENT"],
    )
    dumped = payload.model_dump()
    assert dumped["score_breakdown"]["version"] == "v2"
    assert dumped["score_breakdown"]["dimensions"]["gain"]["label"] == "Gain quality"
    assert dumped["reason_codes"] == ["GAIN_STRONG", "RISK_LOW", "DECISION_CONFIDENT"]


def test_ranked_offer_item_accepts_breakdown_and_reason_codes():
    item = RankedOfferItem(
        rank=1,
        top_pick=True,
        recommendation="top_pick",
        offer_id="R3",
        courier_id="C2",
        accept_score=0.84,
        decision="accept",
        decision_threshold=0.53,
        objective_score=0.8,
        eur_per_hour_net=27.0,
        estimated_net_eur=9.2,
        delta_vs_top_eur_h=0.0,
        delta_vs_median_eur_h=4.6,
        costs={"estimated_net_eur_h": 27.0},
        route_source="estimated",
        route_distance_km=2.2,
        route_duration_min=13.0,
        model_used="ml",
        explanation=["high_estimated_net_revenue"],
        score_breakdown={
            "version": "v2",
            "total_score": 0.84,
            "dimensions": {
                "gain": {
                    "label": "Gain quality",
                    "score": 0.93,
                    "weight": 0.42,
                    "contribution": 0.3906,
                    "impact": "positive",
                },
                "time": {
                    "label": "Time efficiency",
                    "score": 0.78,
                    "weight": 0.2,
                    "contribution": 0.156,
                    "impact": "positive",
                },
                "fuel": {
                    "label": "Fuel efficiency",
                    "score": 0.73,
                    "weight": 0.18,
                    "contribution": 0.1314,
                    "impact": "positive",
                },
                "risk": {
                    "label": "Risk resilience",
                    "score": 0.81,
                    "weight": 0.2,
                    "contribution": 0.162,
                    "impact": "positive",
                },
            },
        },
        reason_codes=["GAIN_STRONG", "TIME_EFFICIENT", "DECISION_CONFIDENT"],
    )
    dumped = item.model_dump()
    assert dumped["score_breakdown"]["dimensions"]["risk"]["impact"] == "positive"
    assert dumped["reason_codes"][0] == "GAIN_STRONG"


def test_ranked_offer_item_accepts_optional_objective_score():
    item = RankedOfferItem(
        rank=2,
        top_pick=False,
        recommendation="viable",
        offer_id="R2",
        courier_id="C9",
        accept_score=0.66,
        decision="accept",
        decision_threshold=0.52,
        objective_score=0.73,
        eur_per_hour_net=19.2,
        estimated_net_eur=6.4,
        delta_vs_top_eur_h=-4.8,
        delta_vs_median_eur_h=-1.2,
        costs={"estimated_net_eur_h": 19.2},
        route_source="estimated",
        route_distance_km=3.0,
        route_duration_min=20.0,
        model_used="heuristic",
        explanation=["balanced_offer_profile"],
    )
    dumped = item.model_dump()
    assert dumped["objective_score"] == 0.73


def test_rank_offers_response_accepts_optional_objective_weights():
    payload = RankOffersResponse(
        count=1,
        ranked_by="objective_score",
        top_pick_offer_id="R1",
        best_eur_h=24.0,
        worst_eur_h=24.0,
        median_eur_h=24.0,
        hourly_gain_vs_worst_eur_h=0.0,
        objective_weights={"w_gain": 0.62, "w_time": 0.23, "w_fuel": 0.15},
        items=[
            RankedOfferItem(
                rank=1,
                top_pick=True,
                recommendation="top_pick",
                offer_id="R1",
                courier_id="C1",
                accept_score=0.8,
                decision="accept",
                decision_threshold=0.5,
                objective_score=0.82,
                eur_per_hour_net=24.0,
                estimated_net_eur=8.4,
                delta_vs_top_eur_h=0.0,
                delta_vs_median_eur_h=0.0,
                costs={"estimated_net_eur_h": 24.0},
                route_source="estimated",
                route_distance_km=2.6,
                route_duration_min=15.0,
                model_used="heuristic",
                explanation=["high_estimated_net_revenue"],
            )
        ],
    )
    dumped = payload.model_dump()
    assert dumped["ranked_by"] == "objective_score"
    assert dumped["objective_weights"]["w_gain"] == 0.62


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


def test_driver_profile_response_contract():
    payload = DriverProfileResponse(
        driver_id="drv_demo_001",
        target_eur_h=19.5,
        vehicle_mpg=32.7,
        aversion_risque=0.45,
        max_eta=18.0,
        source="manual",
        updated_at="2026-04-20T10:00:00+00:00",
    )
    out = payload.model_dump()
    assert out["driver_id"] == "drv_demo_001"
    assert out["target_eur_h"] == 19.5
    assert out["vehicle_mpg"] == 32.7
    assert out["aversion_risque"] == 0.45
    assert out["max_eta"] == 18.0
    assert out["source"] == "manual"
