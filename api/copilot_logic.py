"""Shared copilot scoring and model validation logic."""
from __future__ import annotations

import math
from typing import Any

FEATURE_COLUMNS = [
    "estimated_fare_eur",
    "estimated_distance_km",
    "estimated_duration_min",
    "demand_index",
    "supply_index",
    "weather_factor",
    "traffic_factor",
    "pressure_ratio",
    "estimated_net_eur_h",
]


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def build_feature_map(raw: dict[str, Any]) -> dict[str, float]:
    estimated_fare = max(as_float(raw.get("estimated_fare_eur"), 0.0), 0.0)
    estimated_distance = max(as_float(raw.get("estimated_distance_km"), 0.0), 0.0)
    estimated_duration = max(as_float(raw.get("estimated_duration_min"), 1.0), 1.0)
    distance_to_pickup = max(as_float(raw.get("distance_to_pickup_km"), 0.0), 0.0)
    eta_to_pickup = max(as_float(raw.get("eta_to_pickup_min"), 0.0), 0.0)
    demand_index = as_float(raw.get("demand_index"), 1.0)
    supply_index = max(as_float(raw.get("supply_index"), 1.0), 0.2)
    weather_factor = as_float(raw.get("weather_factor"), 1.0)
    traffic_factor = as_float(raw.get("traffic_factor"), 1.0)
    fuel_price_eur_l = max(as_float(raw.get("fuel_price_eur_l"), 1.85), 0.5)
    vehicle_consumption_l_100km = max(as_float(raw.get("vehicle_consumption_l_100km"), 7.5), 2.0)
    platform_fee_pct = min(max(as_float(raw.get("platform_fee_pct"), 25.0), 0.0), 60.0)
    other_costs_eur = max(as_float(raw.get("other_costs_eur"), 0.0), 0.0)
    target_hourly_net_eur = max(as_float(raw.get("target_hourly_net_eur"), 18.0), 0.0)

    total_distance_km = estimated_distance + distance_to_pickup
    total_duration_min = max(1.0, estimated_duration + eta_to_pickup)
    fuel_cost_eur = total_distance_km * (vehicle_consumption_l_100km / 100.0) * fuel_price_eur_l
    platform_fee_eur = estimated_fare * (platform_fee_pct / 100.0)
    estimated_net_eur = estimated_fare - fuel_cost_eur - platform_fee_eur - other_costs_eur
    pressure_ratio = demand_index / supply_index
    estimated_net_eur_h = (estimated_net_eur / total_duration_min) * 60.0
    target_gap_eur_h = estimated_net_eur_h - target_hourly_net_eur

    return {
        "estimated_fare_eur": estimated_fare,
        "estimated_distance_km": estimated_distance,
        "estimated_duration_min": estimated_duration,
        "distance_to_pickup_km": distance_to_pickup,
        "eta_to_pickup_min": eta_to_pickup,
        "total_distance_km": total_distance_km,
        "total_duration_min": total_duration_min,
        "demand_index": demand_index,
        "supply_index": supply_index,
        "weather_factor": weather_factor,
        "traffic_factor": traffic_factor,
        "fuel_price_eur_l": fuel_price_eur_l,
        "vehicle_consumption_l_100km": vehicle_consumption_l_100km,
        "platform_fee_pct": platform_fee_pct,
        "other_costs_eur": other_costs_eur,
        "fuel_cost_eur": fuel_cost_eur,
        "platform_fee_eur": platform_fee_eur,
        "estimated_net_eur": estimated_net_eur,
        "target_hourly_net_eur": target_hourly_net_eur,
        "target_gap_eur_h": target_gap_eur_h,
        "pressure_ratio": pressure_ratio,
        "estimated_net_eur_h": estimated_net_eur_h,
    }


def heuristic_score(features: dict[str, float]) -> tuple[float, float, list[str]]:
    estimated_fare = features["estimated_fare_eur"]
    total_distance_km = max(features.get("total_distance_km", features["estimated_distance_km"]), 0.0)
    distance_to_pickup = max(features.get("distance_to_pickup_km", 0.0), 0.0)
    demand_index = features["demand_index"]
    supply_index = max(features["supply_index"], 0.2)
    weather_factor = features["weather_factor"]
    traffic_factor = features["traffic_factor"]
    platform_fee_pct = max(features.get("platform_fee_pct", 0.0), 0.0)
    fuel_cost_eur = max(features.get("fuel_cost_eur", 0.0), 0.0)
    target_gap_eur_h = features.get("target_gap_eur_h", 0.0)

    eur_per_hour_net = features.get("estimated_net_eur_h", 0.0)
    pressure_ratio = demand_index / supply_index
    pickup_ratio = min(distance_to_pickup / max(total_distance_km, 0.1), 1.0)
    target_signal = max(-1.0, min(target_gap_eur_h / 12.0, 1.0))

    logits = (
        (eur_per_hour_net - 16.0) / 9.5
        + (pressure_ratio - 1.0) * 0.85
        + (weather_factor - 1.0) * 0.4
        - (traffic_factor - 1.0) * 0.35
        + target_signal * 0.35
        - pickup_ratio * 0.25
    )
    score = max(0.01, min(sigmoid(logits), 0.99))

    reasons: list[str] = []
    if eur_per_hour_net >= 22:
        reasons.append("high_estimated_net_revenue")
    elif eur_per_hour_net < 14:
        reasons.append("low_estimated_net_revenue")
    if target_gap_eur_h >= 2:
        reasons.append("above_target_hourly_goal")
    elif target_gap_eur_h < -2:
        reasons.append("below_target_hourly_goal")

    if pressure_ratio >= 1.25:
        reasons.append("strong_demand_pressure")
    elif pressure_ratio < 0.9:
        reasons.append("weak_demand_pressure")

    if weather_factor < 0.95:
        reasons.append("weather_downside")
    if traffic_factor > 1.2:
        reasons.append("traffic_penalty")
    if platform_fee_pct >= 30:
        reasons.append("high_platform_fee")
    if estimated_fare > 0 and fuel_cost_eur / estimated_fare > 0.2:
        reasons.append("fuel_cost_penalty")
    if pickup_ratio > 0.45:
        reasons.append("long_pickup_detour")

    if not reasons:
        reasons.append("balanced_offer_profile")

    return score, eur_per_hour_net, reasons


def cost_breakdown(features: dict[str, float]) -> dict[str, float]:
    return {
        "estimated_fare_eur": round(float(features.get("estimated_fare_eur", 0.0)), 3),
        "total_distance_km": round(float(features.get("total_distance_km", 0.0)), 3),
        "total_duration_min": round(float(features.get("total_duration_min", 0.0)), 3),
        "fuel_cost_eur": round(float(features.get("fuel_cost_eur", 0.0)), 3),
        "platform_fee_eur": round(float(features.get("platform_fee_eur", 0.0)), 3),
        "other_costs_eur": round(float(features.get("other_costs_eur", 0.0)), 3),
        "estimated_net_eur": round(float(features.get("estimated_net_eur", 0.0)), 3),
        "estimated_net_eur_h": round(float(features.get("estimated_net_eur_h", 0.0)), 3),
        "target_hourly_net_eur": round(float(features.get("target_hourly_net_eur", 0.0)), 3),
        "target_gap_eur_h": round(float(features.get("target_gap_eur_h", 0.0)), 3),
    }


def recommendation_score(features: dict[str, float], accept_score: float) -> tuple[float, str, list[str]]:
    pressure_ratio = max(float(features.get("pressure_ratio", 1.0)), 0.0)
    distance_to_pickup = max(float(features.get("distance_to_pickup_km", 0.0)), 0.0)
    target_gap = float(features.get("target_gap_eur_h", 0.0))

    accept_component = max(0.0, min(float(accept_score), 1.0))
    target_component = max(0.0, min((target_gap + 8.0) / 16.0, 1.0))
    pressure_component = max(0.0, min((pressure_ratio - 0.8) / 1.2, 1.0))
    pickup_component = max(0.0, min(1.0 - (distance_to_pickup / 5.0), 1.0))

    score = (
        0.55 * accept_component
        + 0.25 * target_component
        + 0.12 * pressure_component
        + 0.08 * pickup_component
    )
    score = round(max(0.0, min(score, 1.0)), 4)

    action = "accept" if score >= 0.68 else "consider" if score >= 0.45 else "skip"

    signals: list[str] = []
    if target_gap >= 2.0:
        signals.append("above_target_hourly_goal")
    elif target_gap < -2.0:
        signals.append("below_target_hourly_goal")

    if pressure_ratio >= 1.3:
        signals.append("zone_high_demand_pressure")
    elif pressure_ratio < 0.95:
        signals.append("zone_low_demand_pressure")

    if distance_to_pickup > 3.0:
        signals.append("pickup_far")
    elif distance_to_pickup <= 1.0:
        signals.append("pickup_near")

    return score, action, signals


def forecast_zone_metrics(
    *,
    demand_index: float,
    supply_index: float,
    weather_factor: float,
    traffic_factor: float,
    gbfs_demand_boost: float,
    demand_trend: float,
    horizon_minutes: float,
) -> dict[str, float]:
    horizon_scale = clamp(horizon_minutes / 30.0, 0.5, 2.5)
    trend_adjust = demand_trend * (0.4 + 0.22 * horizon_scale)
    gbfs_adjust = gbfs_demand_boost * 0.7
    weather_adjust = (weather_factor - 1.0) * 0.25
    traffic_drag = max(0.0, traffic_factor - 1.0) * 0.35 * horizon_scale

    forecast_demand = max(0.3, demand_index + trend_adjust + gbfs_adjust + weather_adjust - traffic_drag)
    supply_drift = max(0.0, traffic_factor - 1.0) * 0.18 * horizon_scale
    forecast_supply = max(0.2, supply_index * (1.0 + supply_drift))
    forecast_pressure = forecast_demand / max(forecast_supply, 0.2)
    forecast_opportunity = max((forecast_pressure * weather_factor) / max(traffic_factor, 0.2), 0.0)
    forecast_volatility = clamp(
        abs(demand_trend) * 1.7 + abs(traffic_factor - 1.0) * 0.95 + abs(weather_factor - 1.0) * 0.55,
        0.0,
        1.0,
    )

    return {
        "forecast_demand_index": float(forecast_demand),
        "forecast_supply_index": float(forecast_supply),
        "forecast_pressure_ratio": float(forecast_pressure),
        "forecast_opportunity_score": float(forecast_opportunity),
        "forecast_volatility": float(forecast_volatility),
    }


def reposition_cost_model(
    *,
    route_distance_km: float,
    eta_min: float,
    fuel_price_eur_l: float,
    vehicle_consumption_l_100km: float,
    target_hourly_net_eur: float,
    traffic_factor: float,
    forecast_volatility: float,
    time_cost_share: float = 0.32,
    risk_cost_share: float = 0.22,
) -> dict[str, float]:
    fuel_cost_eur = max(0.0, route_distance_km * (vehicle_consumption_l_100km / 100.0) * fuel_price_eur_l)
    time_cost_eur = max(0.0, (eta_min / 60.0) * (max(target_hourly_net_eur, 8.0) * time_cost_share))
    congestion_risk = clamp(max(traffic_factor - 1.0, 0.0) * 0.8 + forecast_volatility, 0.0, 1.5)
    risk_cost_eur = max(0.0, time_cost_eur * risk_cost_share * (1.0 + congestion_risk))
    total_cost_eur = fuel_cost_eur + time_cost_eur + risk_cost_eur
    penalty = clamp(total_cost_eur / max(6.0, target_hourly_net_eur * 0.55), 0.0, 1.0)

    return {
        "travel_cost_eur": float(fuel_cost_eur),
        "time_cost_eur": float(time_cost_eur),
        "risk_cost_eur": float(risk_cost_eur),
        "reposition_total_cost_eur": float(total_cost_eur),
        "reposition_cost_penalty": float(penalty),
    }


def dispatch_strategy_profile(strategy: str) -> dict[str, float | str]:
    normalized = (strategy or "balanced").strip().lower()
    if normalized == "conservative":
        return {
            "strategy": "conservative",
            "eta_limit_multiplier": 0.85,
            "forecast_horizon_multiplier": 0.8,
            "opportunity_current_weight": 0.58,
            "opportunity_forecast_weight": 0.42,
            "weight_opportunity": 0.34,
            "weight_eta": 0.17,
            "weight_pressure": 0.15,
            "weight_target_need": 0.14,
            "weight_net_gain": 0.16,
            "penalty_cost": 0.19,
            "penalty_volatility": 0.12,
            "negative_net_gain_guard": -0.5,
            "delta_threshold": 0.12,
            "min_net_gain_for_reposition": 0.1,
        }
    if normalized == "aggressive":
        return {
            "strategy": "aggressive",
            "eta_limit_multiplier": 1.2,
            "forecast_horizon_multiplier": 1.25,
            "opportunity_current_weight": 0.34,
            "opportunity_forecast_weight": 0.66,
            "weight_opportunity": 0.29,
            "weight_eta": 0.12,
            "weight_pressure": 0.14,
            "weight_target_need": 0.12,
            "weight_net_gain": 0.25,
            "penalty_cost": 0.1,
            "penalty_volatility": 0.06,
            "negative_net_gain_guard": -2.1,
            "delta_threshold": 0.08,
            "min_net_gain_for_reposition": -1.0,
        }
    return {
        "strategy": "balanced",
        "eta_limit_multiplier": 1.0,
        "forecast_horizon_multiplier": 1.0,
        "opportunity_current_weight": 0.45,
        "opportunity_forecast_weight": 0.55,
        "weight_opportunity": 0.32,
        "weight_eta": 0.16,
        "weight_pressure": 0.14,
        "weight_target_need": 0.12,
        "weight_net_gain": 0.2,
        "penalty_cost": 0.14,
        "penalty_volatility": 0.08,
        "negative_net_gain_guard": -1.0,
        "delta_threshold": 0.1,
        "min_net_gain_for_reposition": -0.5,
    }


def dispatch_decision(
    *,
    stay_score: float,
    stay_gap_eur_h: float,
    stay_eur_h: float,
    move_score: float,
    move_eta_min: float,
    move_potential_eur_h: float,
    max_reposition_eta_min: float,
    move_net_gain_eur_h: float = 0.0,
    negative_net_gain_guard: float = -1.0,
    delta_threshold: float = 0.1,
    min_net_gain_for_reposition: float = -0.5,
) -> tuple[str, float, list[str]]:
    if stay_score <= 0 and move_score <= 0:
        return "wait", 0.2, ["insufficient_real_time_candidates"]

    if stay_score <= 0:
        if move_eta_min <= max_reposition_eta_min:
            return "reposition", max(0.45, move_score), ["no_profitable_local_offer"]
        return "wait", 0.3, ["no_profitable_local_offer", "reposition_eta_too_high"]

    if move_score <= 0:
        return "stay", max(0.45, stay_score), ["best_local_offer_available"]

    if move_net_gain_eur_h < negative_net_gain_guard and stay_score >= 0.35:
        return "stay", max(0.44, stay_score), ["reposition_net_gain_negative"]

    delta = move_score - stay_score
    if stay_score >= 0.72 and delta < 0.13:
        return "stay", max(0.5, stay_score), ["strong_local_offer_available"]

    if (
        delta >= delta_threshold
        and move_eta_min <= max_reposition_eta_min
        and move_net_gain_eur_h >= min_net_gain_for_reposition
    ):
        conf = clamp(0.55 + (delta * 0.9), 0.45, 0.98)
        return "reposition", conf, ["zone_opportunity_outweighs_local_offer"]

    if (
        stay_gap_eur_h < -2.5
        and move_potential_eur_h >= stay_eur_h + 2.0
        and move_net_gain_eur_h >= 0.0
        and move_eta_min <= max_reposition_eta_min
    ):
        return "reposition", max(0.48, move_score), ["below_hourly_target_reposition_can_help"]

    if move_eta_min > max_reposition_eta_min and stay_score >= 0.4:
        return "stay", max(0.45, stay_score), ["reposition_eta_too_high"]

    if stay_score < 0.35 and move_eta_min <= max_reposition_eta_min and move_net_gain_eur_h >= 1.0:
        return "reposition", max(0.46, move_score), ["local_offer_weak_reposition_has_positive_edge"]

    return "stay", max(0.42, stay_score), ["best_local_offer_available"]


def validate_model_payload(
    model_payload: dict[str, Any] | None,
    min_auc: float,
    min_avg_precision: float,
    min_rows: int,
) -> tuple[bool, str]:
    if not model_payload or not isinstance(model_payload, dict):
        return False, "missing_payload"

    model = model_payload.get("model")
    if model is None or not hasattr(model, "predict_proba"):
        return False, "missing_model_or_predict_proba"

    columns = model_payload.get("feature_columns")
    if not isinstance(columns, list) or not columns:
        return False, "missing_feature_columns"

    missing_cols = [col for col in columns if col not in FEATURE_COLUMNS]
    if missing_cols:
        return False, f"unsupported_feature_columns:{','.join(missing_cols)}"

    trained_rows = int(model_payload.get("trained_rows") or 0)
    if trained_rows < min_rows:
        return False, f"trained_rows_below_min:{trained_rows}<{min_rows}"

    metrics = model_payload.get("metrics") or {}
    roc_auc = metrics.get("roc_auc", model_payload.get("auc"))
    avg_precision = metrics.get("average_precision")

    if roc_auc is None:
        return False, "missing_metric_roc_auc"
    if avg_precision is None:
        return False, "missing_metric_average_precision"

    try:
        roc_auc = float(roc_auc)
        avg_precision = float(avg_precision)
    except (TypeError, ValueError):
        return False, "invalid_metrics_type"

    if roc_auc < min_auc:
        return False, f"roc_auc_below_min:{roc_auc:.4f}<{min_auc:.4f}"
    if avg_precision < min_avg_precision:
        return False, f"avg_precision_below_min:{avg_precision:.4f}<{min_avg_precision:.4f}"

    return True, "ok"


def model_score(model_payload: dict[str, Any] | None, features: dict[str, float]) -> tuple[float | None, str]:
    if not model_payload:
        return None, "heuristic"

    model = model_payload.get("model")
    columns = model_payload.get("feature_columns") or FEATURE_COLUMNS
    if model is None:
        return None, "heuristic"

    row = [[features.get(col, 0.0) for col in columns]]
    prob = float(model.predict_proba(row)[0][1])
    return max(0.01, min(prob, 0.99)), "ml"
