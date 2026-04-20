"""Unit tests for the pure ranking helper behind POST /copilot/rank-offers.

The helper is factored out of the FastAPI handler so we can validate sort
order, threshold tags, top-pick quality gates, and delta metrics with no
Redis/OSRM dependency.
"""
from __future__ import annotations

import sys
from pathlib import Path

# copilot_router.py uses sibling-level imports (`from copilot_logic import ...`)
# because it's run with api/ as CWD inside the container. For test runs we
# put api/ on sys.path BEFORE importing it so those sibling imports resolve.
_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from api.copilot_router import _rank_offer_items  # noqa: E402


def _mk(
    offer_id: str,
    eur_h: float,
    net_eur: float,
    accept: float = 0.5,
    *,
    target_h: float = 0.0,
    objective_score: float | None = None,
):
    """Minimal scored-offer dict shaped like what the handler produces."""
    return {
        "offer_id": offer_id,
        "courier_id": "L1",
        "accept_score": accept,
        "decision": "accept" if accept >= 0.5 else "reject",
        "decision_threshold": 0.5,
        "objective_score": objective_score,
        "eur_per_hour_net": eur_h,
        "estimated_net_eur": net_eur,
        "target_hourly_net_eur": target_h,
        "costs": {},
        "route_source": "estimated",
        "route_distance_km": 2.0,
        "route_duration_min": 10.0,
        "model_used": "heuristic",
        "explanation": [],
        "score_breakdown": {
            "version": "v2",
            "total_score": 0.5,
            "dimensions": {
                "gain": {
                    "label": "Gain quality",
                    "score": 0.5,
                    "weight": 0.42,
                    "contribution": 0.21,
                    "impact": "neutral",
                },
                "time": {
                    "label": "Time efficiency",
                    "score": 0.5,
                    "weight": 0.2,
                    "contribution": 0.1,
                    "impact": "neutral",
                },
                "fuel": {
                    "label": "Fuel efficiency",
                    "score": 0.5,
                    "weight": 0.18,
                    "contribution": 0.09,
                    "impact": "neutral",
                },
                "risk": {
                    "label": "Risk resilience",
                    "score": 0.5,
                    "weight": 0.2,
                    "contribution": 0.1,
                    "impact": "neutral",
                },
            },
        },
        "reason_codes": ["PROFILE_BALANCED"],
        "explanation_details": [
            {
                "code": "net_hourly",
                "label": "Net hourly yield",
                "impact": "neutral",
                "value": float(eur_h),
                "unit": "eur_per_hour",
                "source": "cost",
            }
        ],
    }


def test_rank_by_eur_per_hour_puts_best_first():
    scored = [
        _mk("A", 12.0, 5.0),
        _mk("B", 28.0, 10.0),
        _mk("C", 22.0, 8.0),
    ]
    items, best, worst, median = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert [it.offer_id for it in items] == ["B", "C", "A"]
    assert items[0].rank == 1
    assert items[0].top_pick is True
    assert items[0].recommendation == "top_pick"
    assert items[1].top_pick is False
    assert best == 28.0
    assert worst == 12.0
    assert median == 22.0


def test_rank_by_net_eur_overrides_eur_per_hour():
    scored = [
        _mk("short", 40.0, 3.0),
        _mk("long", 22.0, 18.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "estimated_net_eur", None)
    assert items[0].offer_id == "long"
    # Ranked first by absolute net, but no top-pick badge because it fails the
    # EUR/h edge quality gate against the second candidate.
    assert items[0].top_pick is False


def test_rank_respects_floor_and_tags_reject():
    scored = [
        _mk("good", 25.0, 9.0),
        _mk("meh", 14.0, 4.0),
        _mk("garbage", 6.0, 1.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", reject_below_eur_h=12.0)
    assert items[0].offer_id == "good"
    assert items[0].recommendation == "top_pick"
    assert items[1].recommendation == "viable"
    assert items[2].recommendation == "reject"

    items2, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", reject_below_eur_h=30.0)
    assert all(it.recommendation == "reject" for it in items2)
    assert items2[0].top_pick is False


def test_rank_uses_default_reject_floor_when_not_provided():
    scored = [
        _mk("ok", 12.0, 5.0),
        _mk("low", 8.5, 2.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].recommendation in {"top_pick", "viable"}
    assert items[1].recommendation == "reject"


def test_below_target_tag_when_hourly_misses_personal_goal():
    scored = [
        _mk("A", 28.0, 10.0, target_h=25.0),
        _mk("B", 20.0, 12.0, target_h=25.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].recommendation == "top_pick"
    assert items[1].recommendation == "below_target"


def test_below_target_slack_edge_stays_viable():
    scored = [
        _mk("A", 24.0, 8.5, target_h=20.0),
        _mk("B", 19.25, 7.0, target_h=20.0),
    ]
    items, _, _, _ = _rank_offer_items(
        scored,
        "eur_per_hour_net",
        None,
        below_target_slack_eur_h=0.75,
    )
    assert items[1].recommendation == "viable"


def test_top_pick_requires_min_quality_and_edge():
    scored = [
        _mk("A", 20.0, 7.0),
        _mk("B", 19.5, 8.0),
    ]
    items, _, _, _ = _rank_offer_items(
        scored,
        "eur_per_hour_net",
        None,
        top_pick_min_eur_h=14.0,
        top_pick_min_edge_eur_h=1.0,
    )
    assert items[0].recommendation == "viable"
    assert items[0].top_pick is False


def test_top_pick_rejected_if_hourly_below_min_quality():
    scored = [
        _mk("A", 13.0, 7.0),
        _mk("B", 11.0, 7.5),
    ]
    items, _, _, _ = _rank_offer_items(
        scored,
        "eur_per_hour_net",
        None,
        top_pick_min_eur_h=14.0,
        top_pick_min_edge_eur_h=0.5,
    )
    assert items[0].recommendation == "viable"
    assert items[0].top_pick is False


def test_delta_vs_top_and_median_are_correct():
    scored = [
        _mk("A", 10.0, 3.0),
        _mk("B", 20.0, 6.0),
        _mk("C", 30.0, 9.0),
    ]
    items, _, _, median = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert median == 20.0
    assert items[0].delta_vs_top_eur_h == 0.0
    assert items[0].delta_vs_median_eur_h == 10.0
    assert items[1].delta_vs_top_eur_h == -10.0
    assert items[1].delta_vs_median_eur_h == 0.0
    assert items[2].delta_vs_top_eur_h == -20.0
    assert items[2].delta_vs_median_eur_h == -10.0


def test_tie_break_prefers_higher_absolute_net():
    scored = [
        _mk("small", 25.0, 4.0),
        _mk("big", 25.0, 12.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].offer_id == "big"


def test_tie_break_prefers_higher_accept_score_when_hourly_and_net_tie():
    scored = [
        _mk("low_accept", 25.0, 10.0, accept=0.45),
        _mk("high_accept", 25.0, 10.0, accept=0.82),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].offer_id == "high_accept"


def test_tie_break_is_deterministic_on_full_tie():
    scored = [
        _mk("B_offer", 25.0, 10.0, accept=0.8),
        _mk("A_offer", 25.0, 10.0, accept=0.8),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert [it.offer_id for it in items] == ["A_offer", "B_offer"]


def test_rank_by_invalid_value_falls_back_to_hourly():
    scored = [
        _mk("low", 12.0, 8.0, accept=0.9),
        _mk("high", 28.0, 4.0, accept=0.1),
    ]
    items, _, _, _ = _rank_offer_items(scored, "unexpected_metric", None)
    assert items[0].offer_id == "high"


def test_rank_by_objective_score_uses_custom_metric():
    scored = [
        _mk("gain_first", 30.0, 10.0, accept=0.9, objective_score=0.31),
        _mk("fuel_time_first", 22.0, 8.0, accept=0.72, objective_score=0.88),
    ]
    items, _, _, _ = _rank_offer_items(scored, "objective_score", None)
    assert items[0].offer_id == "fuel_time_first"
    assert items[0].objective_score == 0.88
    assert items[1].objective_score == 0.31


def test_non_finite_reject_floor_falls_back_to_default():
    scored = [
        _mk("ok", 12.0, 5.0),
        _mk("low", 8.5, 2.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", reject_below_eur_h=float("nan"))
    assert items[1].recommendation == "reject"


def test_single_offer_keeps_top_pick_when_above_min_quality():
    scored = [_mk("only", 18.0, 6.0)]
    items, best, worst, median = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert len(items) == 1
    assert items[0].top_pick is True
    assert items[0].rank == 1
    assert best == worst == median == 18.0
    assert items[0].delta_vs_top_eur_h == 0.0


def test_explanation_details_are_propagated():
    scored = [_mk("only", 22.0, 8.0)]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].explanation_details
    assert items[0].explanation_details[0].code == "net_hourly"


def test_decision_threshold_is_propagated():
    scored = [_mk("only", 22.0, 8.0)]
    scored[0]["decision_threshold"] = 0.57
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].decision_threshold == 0.57


def test_explainability_fields_are_propagated():
    scored = [_mk("only", 22.0, 8.0)]
    scored[0]["reason_codes"] = ["GAIN_STRONG", "DECISION_CONFIDENT"]
    scored[0]["score_breakdown"] = {
        "version": "v2",
        "total_score": 0.8123,
        "dimensions": {
            "gain": {
                "label": "Gain quality",
                "score": 0.91,
                "weight": 0.42,
                "contribution": 0.3822,
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
                "score": 0.68,
                "weight": 0.18,
                "contribution": 0.1224,
                "impact": "positive",
            },
            "risk": {
                "label": "Risk resilience",
                "score": 0.82,
                "weight": 0.2,
                "contribution": 0.164,
                "impact": "positive",
            },
        },
    }
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].reason_codes == ["GAIN_STRONG", "DECISION_CONFIDENT"]
    assert items[0].score_breakdown is not None
    assert items[0].score_breakdown.dimensions["gain"].impact == "positive"
