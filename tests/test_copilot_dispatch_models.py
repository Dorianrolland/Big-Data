from api.copilot_logic import (
    dispatch_decision,
    dispatch_strategy_profile,
    forecast_zone_metrics,
    reposition_cost_model,
)


def test_forecast_zone_metrics_boosts_positive_trend():
    metrics = forecast_zone_metrics(
        demand_index=1.2,
        supply_index=0.9,
        weather_factor=1.05,
        traffic_factor=1.0,
        gbfs_demand_boost=0.25,
        demand_trend=0.35,
        horizon_minutes=30.0,
    )
    assert metrics["forecast_demand_index"] > 1.2
    assert metrics["forecast_pressure_ratio"] > (1.2 / 0.9)
    assert 0.0 <= metrics["forecast_volatility"] <= 1.0


def test_forecast_zone_metrics_penalizes_traffic_drag():
    metrics = forecast_zone_metrics(
        demand_index=1.35,
        supply_index=1.0,
        weather_factor=1.02,
        traffic_factor=1.6,
        gbfs_demand_boost=0.0,
        demand_trend=0.0,
        horizon_minutes=60.0,
    )
    current_opportunity = (1.35 / 1.0) * 1.02 / 1.6
    assert metrics["forecast_opportunity_score"] <= current_opportunity
    assert metrics["forecast_supply_index"] >= 1.0


def test_reposition_cost_model_scales_with_time_and_risk():
    fast = reposition_cost_model(
        route_distance_km=2.0,
        eta_min=6.0,
        fuel_price_usd_gallon=3.8,
        vehicle_mpg=29.4,
        target_hourly_net_eur=20.0,
        traffic_factor=1.0,
        forecast_volatility=0.1,
    )
    slow = reposition_cost_model(
        route_distance_km=2.0,
        eta_min=18.0,
        fuel_price_usd_gallon=3.8,
        vehicle_mpg=29.4,
        target_hourly_net_eur=20.0,
        traffic_factor=1.4,
        forecast_volatility=0.8,
    )
    assert slow["reposition_total_cost_eur"] > fast["reposition_total_cost_eur"]
    assert slow["risk_cost_eur"] > fast["risk_cost_eur"]
    assert 0.0 <= slow["reposition_cost_penalty"] <= 1.0


def test_dispatch_decision_prefers_stay_on_negative_net_gain():
    decision, confidence, reasons = dispatch_decision(
        stay_score=0.58,
        stay_gap_eur_h=-1.0,
        stay_eur_h=17.5,
        move_score=0.76,
        move_eta_min=8.0,
        move_potential_eur_h=19.0,
        max_reposition_eta_min=20.0,
        move_net_gain_eur_h=-2.2,
    )
    assert decision == "stay"
    assert confidence >= 0.44
    assert "reposition_net_gain_negative" in reasons


def test_dispatch_decision_repositions_when_local_offer_is_weak():
    decision, confidence, reasons = dispatch_decision(
        stay_score=0.24,
        stay_gap_eur_h=-5.5,
        stay_eur_h=11.0,
        move_score=0.47,
        move_eta_min=9.0,
        move_potential_eur_h=15.0,
        max_reposition_eta_min=20.0,
        move_net_gain_eur_h=2.8,
    )
    assert decision == "reposition"
    assert confidence >= 0.46
    assert set(reasons) & {
        "local_offer_weak_reposition_has_positive_edge",
        "zone_opportunity_outweighs_local_offer",
    }


def test_dispatch_strategy_profiles_are_distinct():
    conservative = dispatch_strategy_profile("conservative")
    balanced = dispatch_strategy_profile("balanced")
    aggressive = dispatch_strategy_profile("aggressive")
    fallback = dispatch_strategy_profile("unknown")

    assert conservative["strategy"] == "conservative"
    assert balanced["strategy"] == "balanced"
    assert aggressive["strategy"] == "aggressive"
    assert fallback["strategy"] == "balanced"

    assert conservative["eta_limit_multiplier"] < balanced["eta_limit_multiplier"] < aggressive["eta_limit_multiplier"]
    assert conservative["min_net_gain_for_reposition"] > balanced["min_net_gain_for_reposition"]
    assert aggressive["min_net_gain_for_reposition"] < balanced["min_net_gain_for_reposition"]


def test_dispatch_decision_custom_thresholds_can_block_reposition():
    decision_balanced, _, _ = dispatch_decision(
        stay_score=0.42,
        stay_gap_eur_h=-2.0,
        stay_eur_h=15.0,
        move_score=0.56,
        move_eta_min=8.0,
        move_potential_eur_h=16.5,
        max_reposition_eta_min=20.0,
        move_net_gain_eur_h=-0.2,
    )
    decision_conservative, _, reasons_conservative = dispatch_decision(
        stay_score=0.42,
        stay_gap_eur_h=-2.0,
        stay_eur_h=15.0,
        move_score=0.56,
        move_eta_min=8.0,
        move_potential_eur_h=16.5,
        max_reposition_eta_min=20.0,
        move_net_gain_eur_h=-0.2,
        delta_threshold=0.12,
        min_net_gain_for_reposition=0.1,
    )
    assert decision_balanced == "reposition"
    assert decision_conservative == "stay"
    assert "best_local_offer_available" in reasons_conservative
