"""Unit tests for explainability helper propagation in copilot_router."""
from __future__ import annotations

import sys
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from api.copilot_router import _build_scored_offer_snapshot, _dispatch_offer_summary  # noqa: E402


def test_build_scored_offer_snapshot_includes_explainability_fields():
    snapshot = _build_scored_offer_snapshot(
        payload={"offer_id": "off_1", "courier_id": "drv_1"},
        features={
            "estimated_net_eur": 8.5,
            "target_hourly_net_eur": 18.0,
            "target_gap_eur_h": 4.5,
        },
        eur_per_hour=22.5,
        accept_prob=0.81,
        decision="accept",
        decision_threshold=0.54,
        breakdown={"estimated_net_eur_h": 22.5},
        route_meta={
            "route_source": "estimated",
            "route_distance_km": 4.2,
            "route_duration_min": 16.3,
            "route_notes": [],
        },
        model_used="ml",
        reasons=["high_estimated_net_revenue"],
        details=[
            {
                "code": "net_hourly",
                "label": "Net hourly yield",
                "impact": "positive",
                "value": 22.5,
                "unit": "eur_per_hour",
                "source": "cost",
            }
        ],
        score_breakdown={
            "version": "v2",
            "total_score": 0.83,
            "dimensions": {
                "gain": {
                    "label": "Gain quality",
                    "score": 0.9,
                    "weight": 0.42,
                    "contribution": 0.378,
                    "impact": "positive",
                },
                "time": {
                    "label": "Time efficiency",
                    "score": 0.72,
                    "weight": 0.2,
                    "contribution": 0.144,
                    "impact": "positive",
                },
                "fuel": {
                    "label": "Fuel efficiency",
                    "score": 0.66,
                    "weight": 0.18,
                    "contribution": 0.1188,
                    "impact": "positive",
                },
                "risk": {
                    "label": "Risk resilience",
                    "score": 0.76,
                    "weight": 0.2,
                    "contribution": 0.152,
                    "impact": "positive",
                },
            },
        },
        reason_codes=["GAIN_STRONG", "DECISION_CONFIDENT"],
    )
    assert snapshot["offer_id"] == "off_1"
    assert snapshot["decision"] == "accept"
    assert snapshot["score_breakdown"]["version"] == "v2"
    assert snapshot["reason_codes"] == ["GAIN_STRONG", "DECISION_CONFIDENT"]


def test_dispatch_offer_summary_preserves_explainability_fields():
    summary = _dispatch_offer_summary(
        {
            "offer_id": "off_1",
            "zone_id": "zone_1",
            "recommendation_action": "accept",
            "recommendation_score": 0.78,
            "objective_score": 0.73,
            "accept_score": 0.81,
            "eur_per_hour_net": 22.5,
            "target_gap_eur_h": 4.5,
            "distance_to_pickup_km": 1.2,
            "route_duration_min": 16.3,
            "route_source": "estimated",
            "recommendation_signals": ["above_target_hourly_goal"],
            "explanation": ["high_estimated_net_revenue"],
            "score_breakdown": {"version": "v2", "total_score": 0.83, "dimensions": {}},
            "reason_codes": ["GAIN_STRONG"],
        }
    )
    assert summary["score_breakdown"]["version"] == "v2"
    assert summary["reason_codes"] == ["GAIN_STRONG"]
    assert summary["signals"] == ["above_target_hourly_goal"]
    assert summary["reasons"] == ["high_estimated_net_revenue"]


def test_dispatch_offer_summary_uses_safe_defaults_when_missing_fields():
    summary = _dispatch_offer_summary(
        {
            "offer_id": "off_2",
            "zone_id": "zone_2",
            "recommendation_action": "skip",
            "recommendation_score": 0.1,
            "objective_score": 0.2,
            "accept_score": 0.05,
            "eur_per_hour_net": 8.0,
            "target_gap_eur_h": -10.0,
            "distance_to_pickup_km": 4.0,
            "route_duration_min": 28.0,
            "route_source": "estimated",
            "reason_codes": "not-a-list",
        }
    )
    assert summary["score_breakdown"] is None
    assert summary["signals"] == []
    assert summary["reasons"] == []
    assert summary["reason_codes"] == []
