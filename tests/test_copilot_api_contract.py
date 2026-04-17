"""API response-contract tests for copilot scoring models."""
from __future__ import annotations

import sys
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from api.copilot_router import RankedOfferItem, ScoreOfferResponse  # noqa: E402


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
