"""Unit tests for the pure ranking helper behind POST /copilot/rank-offers.

The helper is factored out of the FastAPI handler specifically so that we
can test the sort order, floor filter, recommendation tags, and delta
metrics without spinning up Redis / OSRM / the FastAPI app. Everything
here is local, millisecond-fast, and deterministic.
"""
from __future__ import annotations

import sys
from pathlib import Path

# copilot_router.py uses sibling-level imports (`from copilot_logic import …`)
# because it's run with api/ as CWD inside the container. For test runs we
# put api/ on sys.path BEFORE importing it so those sibling imports resolve.
_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from api.copilot_router import _rank_offer_items  # noqa: E402


def _mk(offer_id: str, eur_h: float, net_eur: float, accept: float = 0.5, *, target_h: float = 0.0):
    """Minimal scored-offer dict shaped like what the handler produces."""
    return {
        "offer_id": offer_id,
        "courier_id": "L1",
        "accept_score": accept,
        "decision": "accept" if accept >= 0.5 else "reject",
        "eur_per_hour_net": eur_h,
        "estimated_net_eur": net_eur,
        "target_hourly_net_eur": target_h,
        "costs": {},
        "route_source": "estimated",
        "route_distance_km": 2.0,
        "route_duration_min": 10.0,
        "model_used": "heuristic",
        "explanation": [],
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
    # Offer with lower €/h but higher absolute net is preferred under net_eur
    scored = [
        _mk("short", 40.0, 3.0),   # 40 €/h but only 3 € in pocket
        _mk("long",  22.0, 18.0),  # 22 €/h but 18 € on the table
    ]
    items, _, _, _ = _rank_offer_items(scored, "estimated_net_eur", None)
    assert items[0].offer_id == "long"
    assert items[0].top_pick is True


def test_rank_respects_floor_and_tags_reject():
    scored = [
        _mk("good",    25.0, 9.0),
        _mk("meh",     14.0, 4.0),
        _mk("garbage", 6.0, 1.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", reject_below_eur_h=12.0)
    assert items[0].offer_id == "good"
    assert items[0].recommendation == "top_pick"
    assert items[1].recommendation == "viable"          # 14 €/h >= 12
    assert items[2].recommendation == "reject"          # 6 €/h < 12
    # Floor must also be able to reject the nominal top pick
    items2, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", reject_below_eur_h=30.0)
    # Everything below the floor: even rank=1 is flagged reject and not top_pick
    assert all(it.recommendation == "reject" for it in items2)
    assert items2[0].top_pick is False


def test_below_target_tag_when_hourly_misses_personal_goal():
    # Driver wants 25 €/h minimum; the second offer falls below that.
    scored = [
        _mk("A", 28.0, 10.0, target_h=25.0),
        _mk("B", 20.0, 12.0, target_h=25.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].recommendation == "top_pick"
    assert items[1].recommendation == "below_target"


def test_delta_vs_top_and_median_are_correct():
    scored = [
        _mk("A", 10.0, 3.0),
        _mk("B", 20.0, 6.0),
        _mk("C", 30.0, 9.0),
    ]
    items, _, _, median = _rank_offer_items(scored, "eur_per_hour_net", None)
    # Ranked order: C(30), B(20), A(10) → median = 20
    assert median == 20.0
    assert items[0].delta_vs_top_eur_h == 0.0
    assert items[0].delta_vs_median_eur_h == 10.0
    assert items[1].delta_vs_top_eur_h == -10.0
    assert items[1].delta_vs_median_eur_h == 0.0
    assert items[2].delta_vs_top_eur_h == -20.0
    assert items[2].delta_vs_median_eur_h == -10.0


def test_tie_break_prefers_higher_absolute_net():
    # Two offers equal on €/h → tie-break on estimated_net_eur
    scored = [
        _mk("small", 25.0, 4.0),
        _mk("big",   25.0, 12.0),
    ]
    items, _, _, _ = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert items[0].offer_id == "big"


def test_single_offer_still_gets_top_pick():
    scored = [_mk("only", 18.0, 6.0)]
    items, best, worst, median = _rank_offer_items(scored, "eur_per_hour_net", None)
    assert len(items) == 1
    assert items[0].top_pick is True
    assert items[0].rank == 1
    assert best == worst == median == 18.0
    assert items[0].delta_vs_top_eur_h == 0.0
