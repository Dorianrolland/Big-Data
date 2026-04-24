"""Unit tests for the pure zone ranking helper behind /copilot/driver/{id}/next-best-zone.

The helper is factored out so we can verify the distance penalty, the max-
distance hard filter, and the homeward bonus without spinning up Redis or
the FastAPI app. Tests use bare "lat_lon" zone ids because the resolver
accepts that format directly — no NYC centroid JSON needed.
"""
from __future__ import annotations

import sys
from pathlib import Path

_API_DIR = Path(__file__).resolve().parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

from api.copilot_router import _rank_zone_recommendations  # noqa: E402


def _zone(zone_id: str, opportunity: float, demand: float = 1.0, supply: float = 1.0):
    """Minimal zone dict shaped like what next_best_zone builds from Redis."""
    return {
        "zone_id": zone_id,
        "demand_index": demand,
        "supply_index": supply,
        "weather_factor": 1.0,
        "traffic_factor": 1.0,
        "opportunity_score": opportunity,
    }


# ── position-blind fallback ───────────────────────────────────────────────────
def test_unchanged_when_driver_position_unknown():
    zones = [
        _zone("40.7580_-73.9855", 1.50),
        _zone("40.7000_-74.0100", 1.20),
    ]
    out = _rank_zone_recommendations(zones, driver_lat=None, driver_lon=None)
    # Without a driver position, the adjusted score equals the base and the
    # helper must not try to compute distances.
    assert [z["zone_id"] for z in out] == ["40.7580_-73.9855", "40.7000_-74.0100"]
    assert out[0]["adjusted_opportunity_score"] == 1.5
    assert out[0]["distance_km"] is None
    assert out[0]["reposition_cost_usd"] is None


def test_unchanged_when_distance_weight_zero():
    # Driver in midtown, zone a bit south. distance_weight=0 → order is by base.
    zones = [
        _zone("40.7580_-73.9855", 1.20),   # right next to driver
        _zone("40.6500_-73.9500", 1.80),   # further but higher opportunity
    ]
    out = _rank_zone_recommendations(
        zones, driver_lat=40.7580, driver_lon=-73.9855, distance_weight=0.0
    )
    assert out[0]["zone_id"] == "40.6500_-73.9500"
    assert out[0]["adjusted_opportunity_score"] == 1.8
    # But distance metadata is still present — caller may display it anyway
    assert out[0]["distance_km"] is not None and out[0]["distance_km"] > 0
    assert out[1]["distance_km"] is not None and out[1]["distance_km"] >= 0


# ── hard distance filter ──────────────────────────────────────────────────────
def test_max_distance_filter_drops_far_zones():
    zones = [
        _zone("40.7580_-73.9855", 1.00),  # ~0 km
        _zone("40.6500_-73.9500", 2.00),  # ~12 km away
    ]
    out = _rank_zone_recommendations(
        zones,
        driver_lat=40.7580,
        driver_lon=-73.9855,
        max_distance_km=5.0,
    )
    assert len(out) == 1
    assert out[0]["zone_id"] == "40.7580_-73.9855"


# ── distance penalty ──────────────────────────────────────────────────────────
def test_distance_penalty_demotes_far_zone_of_similar_value():
    # Two comparable opportunities: one close, one far. With weight=0.6 the
    # far one must drop below the close one.
    zones = [
        _zone("40.7580_-73.9855", 1.50),  # close (~0 km)
        _zone("40.6500_-73.9500", 1.70),  # far  (~12 km)
    ]
    out = _rank_zone_recommendations(
        zones,
        driver_lat=40.7580,
        driver_lon=-73.9855,
        distance_weight=0.6,
    )
    assert out[0]["zone_id"] == "40.7580_-73.9855"
    assert out[0]["distance_penalty_pct"] == 0.0  # distance_km ≈ 0
    assert out[1]["distance_penalty_pct"] > 0.0
    assert out[0]["adjusted_opportunity_score"] > out[1]["adjusted_opportunity_score"]


def test_fuel_and_time_cost_are_computed_for_reached_zones():
    zones = [_zone("40.6500_-73.9500", 1.20)]
    out = _rank_zone_recommendations(
        zones,
        driver_lat=40.7580,
        driver_lon=-73.9855,
        fuel_price_usd_gallon=4.0,
        vehicle_mpg=25.0,
    )
    assert len(out) == 1
    item = out[0]
    assert item["distance_km"] is not None and item["distance_km"] > 5.0
    # Fuel cost = miles / mpg * USD/gallon.
    expected_fuel = round((item["distance_km"] * 0.621371 / 25.0) * 4.0, 3)
    assert abs(item["reposition_cost_usd"] - expected_fuel) < 1e-3
    assert item["reposition_time_min"] > 0.0


# ── homeward bonus ────────────────────────────────────────────────────────────
def test_homeward_bonus_prefers_zones_on_the_way_home():
    # Driver in midtown, home in downtown (south). Two zones, equal
    # opportunity — one north (away from home), one south (on the way home).
    zones = [
        _zone("40.7800_-73.9700", 1.50),  # north of driver, away from home
        _zone("40.7300_-73.9900", 1.50),  # south of driver, toward home
    ]
    out = _rank_zone_recommendations(
        zones,
        driver_lat=40.7580,
        driver_lon=-73.9855,
        home_lat=40.7050,
        home_lon=-74.0100,
        distance_weight=0.5,
    )
    southern = next(z for z in out if z["zone_id"] == "40.7300_-73.9900")
    northern = next(z for z in out if z["zone_id"] == "40.7800_-73.9700")
    assert southern["homeward_factor"] is not None and southern["homeward_factor"] > 0
    assert northern["homeward_factor"] is not None and northern["homeward_factor"] < 0
    # Homeward gain only kicks in for positive factor
    assert southern["homeward_gain_pct"] > 0.0
    assert northern["homeward_gain_pct"] == 0.0
    # Southern (homeward) must outrank the northern at equal base opportunity
    assert out[0]["zone_id"] == "40.7300_-73.9900"


def test_homeward_factor_none_without_home_hint():
    zones = [_zone("40.7600_-73.9800", 1.00)]
    out = _rank_zone_recommendations(
        zones, driver_lat=40.7580, driver_lon=-73.9855, distance_weight=0.3
    )
    assert out[0]["homeward_factor"] is None
    assert out[0]["homeward_gain_pct"] == 0.0


# ── robustness ───────────────────────────────────────────────────────────────
def test_unresolvable_zone_id_is_kept_but_not_enriched():
    # Neither "nyc_XXX" nor "lat_lon": _resolve_zone_coordinates returns None
    zones = [_zone("unknown_thing", 1.00)]
    out = _rank_zone_recommendations(
        zones, driver_lat=40.7580, driver_lon=-73.9855, distance_weight=0.5
    )
    assert len(out) == 1
    assert out[0]["distance_km"] is None
    assert out[0]["adjusted_opportunity_score"] == 1.0


def test_sort_is_stable_on_adjusted_score_tie():
    # Two zones with identical base AND identical distance → adjusted equal →
    # Python's sort is stable so original order is preserved.
    zones = [
        _zone("40.7580_-73.9855", 1.00),
        _zone("40.7580_-73.9855", 1.00),
    ]
    zones[0]["zone_id"] = "A_40.7580_-73.9855"
    zones[1]["zone_id"] = "B_40.7580_-73.9855"
    # Rewrite them with parseable ids
    zones = [
        {**_zone("40.7580_-73.9855", 1.00), "label": "A"},
        {**_zone("40.7580_-73.9855", 1.00), "label": "B"},
    ]
    out = _rank_zone_recommendations(
        zones,
        driver_lat=40.7000,
        driver_lon=-74.0000,
        distance_weight=0.5,
    )
    assert [z["label"] for z in out] == ["A", "B"]
