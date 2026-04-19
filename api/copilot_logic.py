"""Shared copilot scoring and model validation logic."""
from __future__ import annotations

import math
from typing import Any, Mapping

FEATURE_COLUMNS = [
    "estimated_fare_eur",
    "estimated_distance_km",
    "estimated_duration_min",
    "demand_index",
    "supply_index",
    "weather_factor",
    "traffic_factor",
    "event_pressure",
    "event_count_nearby",
    "weather_precip_mm",
    "weather_wind_kmh",
    "weather_intensity",
    "temporal_hour_local",
    "is_peak_hour",
    "is_weekend",
    "is_holiday",
    "temporal_pressure",
    "context_fallback_applied",
    "context_stale_sources",
    "pressure_ratio",
    "estimated_net_eur_h",
]


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def as_float(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


DEFAULT_SCORE_WEIGHTS = {
    "net_hourly": 0.46,
    "net_trip": 0.18,
    "fuel_efficiency": 0.16,
    "time_efficiency": 0.12,
    "context": 0.08,
}


def normalize_score_weights(raw_weights: Mapping[str, float] | None = None) -> dict[str, float]:
    weights = {k: float(v) for k, v in DEFAULT_SCORE_WEIGHTS.items()}
    if raw_weights:
        for key in DEFAULT_SCORE_WEIGHTS:
            if key in raw_weights:
                weights[key] = max(0.0, float(raw_weights[key]))

    total = sum(weights.values())
    if total <= 0.0:
        return {k: float(v) for k, v in DEFAULT_SCORE_WEIGHTS.items()}
    return {k: float(v / total) for k, v in weights.items()}


def _context_freshness_penalty(*, stale_sources: float, fallback_applied: float) -> float:
    stale_component = clamp(stale_sources / 4.0, 0.0, 1.0) * 0.22
    fallback_component = clamp(fallback_applied, 0.0, 1.0) * 0.12
    return stale_component + fallback_component


def score_components(features: dict[str, float]) -> dict[str, float]:
    net_hourly = float(features.get("estimated_net_eur_h", 0.0))
    net_trip = float(features.get("estimated_net_eur", 0.0))
    total_duration_min = max(float(features.get("total_duration_min", 1.0)), 1.0)
    pressure_ratio = max(float(features.get("pressure_ratio", 1.0)), 0.0)
    weather_factor = max(float(features.get("weather_factor", 1.0)), 0.0)
    traffic_factor = max(float(features.get("traffic_factor", 1.0)), 0.0)
    event_pressure = max(float(features.get("event_pressure", 0.0)), 0.0)
    event_count_nearby = max(float(features.get("event_count_nearby", 0.0)), 0.0)
    weather_intensity = clamp(float(features.get("weather_intensity", 0.0)), 0.0, 1.0)
    temporal_pressure = max(float(features.get("temporal_pressure", 0.0)), 0.0)
    is_peak_hour = 1.0 if float(features.get("is_peak_hour", 0.0)) >= 0.5 else 0.0
    is_weekend = 1.0 if float(features.get("is_weekend", 0.0)) >= 0.5 else 0.0
    is_holiday = 1.0 if float(features.get("is_holiday", 0.0)) >= 0.5 else 0.0
    context_fallback_applied = 1.0 if float(features.get("context_fallback_applied", 0.0)) >= 0.5 else 0.0
    context_stale_sources = max(float(features.get("context_stale_sources", 0.0)), 0.0)
    estimated_fare = max(float(features.get("estimated_fare_eur", 0.0)), 0.0)
    fuel_cost = max(float(features.get("fuel_cost_eur", 0.0)), 0.0)

    fuel_share = fuel_cost / max(estimated_fare, 0.01)
    pressure_component = clamp((pressure_ratio - 0.75) / 1.0, 0.0, 1.0)
    weather_component = clamp((weather_factor - 0.85) / 0.35, 0.0, 1.0)
    traffic_component = 1.0 - clamp((traffic_factor - 0.9) / 0.7, 0.0, 1.0)
    event_component = clamp((event_pressure / 0.8) * 0.72 + (min(event_count_nearby, 5.0) / 5.0) * 0.28, 0.0, 1.0)
    temporal_component = clamp(
        (temporal_pressure / 0.35) * 0.7 + ((is_peak_hour * 0.55) + (is_weekend * 0.25) + (is_holiday * 0.20)),
        0.0,
        1.0,
    )
    freshness_penalty = _context_freshness_penalty(
        stale_sources=context_stale_sources,
        fallback_applied=context_fallback_applied,
    )
    base_context = (
        (pressure_component * 0.47)
        + (weather_component * 0.18)
        + (traffic_component * 0.17)
        + (event_component * 0.10)
        + (temporal_component * 0.08)
        + (weather_intensity * 0.06)
    )

    return {
        "net_hourly": clamp((net_hourly + 5.0) / 35.0, 0.0, 1.0),
        "net_trip": clamp((net_trip + 1.5) / 18.0, 0.0, 1.0),
        "fuel_efficiency": 1.0 - clamp((fuel_share - 0.06) / 0.25, 0.0, 1.0),
        "time_efficiency": 1.0 - clamp((total_duration_min - 14.0) / 32.0, 0.0, 1.0),
        "context": clamp(base_context - freshness_penalty, 0.0, 1.0),
    }


def weighted_offer_score(
    features: dict[str, float],
    *,
    weights: Mapping[str, float] | None = None,
) -> tuple[float, dict[str, float], dict[str, float]]:
    components = score_components(features)
    normalized = normalize_score_weights(weights)
    contributions = {
        key: float(normalized[key] * components[key]) for key in DEFAULT_SCORE_WEIGHTS
    }
    base_score = sum(contributions.values())
    score = clamp((0.06 + 0.88 * base_score), 0.01, 0.99)
    return score, components, contributions


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
    event_pressure = max(as_float(raw.get("event_pressure"), 0.0), 0.0)
    event_count_nearby = max(as_float(raw.get("event_count_nearby"), 0.0), 0.0)
    weather_precip_mm = max(as_float(raw.get("weather_precip_mm"), 0.0), 0.0)
    weather_wind_kmh = max(as_float(raw.get("weather_wind_kmh"), 0.0), 0.0)
    weather_intensity_derived = clamp((weather_precip_mm / 6.0) * 0.65 + (weather_wind_kmh / 50.0) * 0.35, 0.0, 1.0)
    weather_intensity = clamp(as_float(raw.get("weather_intensity"), weather_intensity_derived), 0.0, 1.0)
    temporal_hour_local = clamp(as_float(raw.get("temporal_hour_local"), -1.0), -1.0, 24.0)
    is_peak_hour = 1.0 if as_float(raw.get("is_peak_hour"), 0.0) >= 0.5 else 0.0
    is_weekend = 1.0 if as_float(raw.get("is_weekend"), 0.0) >= 0.5 else 0.0
    is_holiday = 1.0 if as_float(raw.get("is_holiday"), 0.0) >= 0.5 else 0.0
    temporal_pressure_derived = (
        (0.14 if is_peak_hour >= 0.5 else 0.0)
        + (0.05 if (temporal_hour_local >= 11.0 and temporal_hour_local < 14.0 and is_peak_hour < 0.5) else 0.0)
        + (0.08 if is_weekend >= 0.5 else 0.0)
        + (0.12 if is_holiday >= 0.5 else 0.0)
    )
    temporal_pressure = max(as_float(raw.get("temporal_pressure"), temporal_pressure_derived), 0.0)
    context_fallback_applied = 1.0 if as_float(raw.get("context_fallback_applied"), 0.0) >= 0.5 else 0.0
    context_stale_sources = max(as_float(raw.get("context_stale_sources"), 0.0), 0.0)
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
        "event_pressure": event_pressure,
        "event_count_nearby": event_count_nearby,
        "weather_precip_mm": weather_precip_mm,
        "weather_wind_kmh": weather_wind_kmh,
        "weather_intensity": weather_intensity,
        "temporal_hour_local": temporal_hour_local,
        "is_peak_hour": is_peak_hour,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday,
        "temporal_pressure": temporal_pressure,
        "context_fallback_applied": context_fallback_applied,
        "context_stale_sources": context_stale_sources,
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


def heuristic_score(
    features: dict[str, float],
    *,
    weights: Mapping[str, float] | None = None,
) -> tuple[float, float, list[str]]:
    estimated_fare = features["estimated_fare_eur"]
    total_distance_km = max(features.get("total_distance_km", features["estimated_distance_km"]), 0.0)
    total_duration_min = max(features.get("total_duration_min", features["estimated_duration_min"]), 1.0)
    distance_to_pickup = max(features.get("distance_to_pickup_km", 0.0), 0.0)
    demand_index = features["demand_index"]
    supply_index = max(features["supply_index"], 0.2)
    weather_factor = features["weather_factor"]
    traffic_factor = features["traffic_factor"]
    event_pressure = max(features.get("event_pressure", 0.0), 0.0)
    temporal_pressure = max(features.get("temporal_pressure", 0.0), 0.0)
    context_fallback_applied = 1.0 if float(features.get("context_fallback_applied", 0.0)) >= 0.5 else 0.0
    platform_fee_pct = max(features.get("platform_fee_pct", 0.0), 0.0)
    fuel_cost_eur = max(features.get("fuel_cost_eur", 0.0), 0.0)
    target_gap_eur_h = features.get("target_gap_eur_h", 0.0)

    eur_per_hour_net = features.get("estimated_net_eur_h", 0.0)
    pressure_ratio = demand_index / supply_index
    pickup_ratio = min(distance_to_pickup / max(total_distance_km, 0.1), 1.0)
    score, _, _ = weighted_offer_score(features, weights=weights)

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
    if event_pressure >= 0.12:
        reasons.append("event_demand_uplift")
    if temporal_pressure >= 0.1:
        reasons.append("temporal_demand_uplift")
    if context_fallback_applied >= 0.5:
        reasons.append("context_fallback_mode")
    if total_duration_min >= 35.0:
        reasons.append("long_offer_duration")
    elif total_duration_min <= 15.0 and eur_per_hour_net >= 18.0:
        reasons.append("short_offer_efficiency")
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


def hybrid_accept_threshold(
    features: dict[str, float],
    *,
    base_threshold: float = 0.5,
    below_target_penalty_max: float = 0.12,
    high_fuel_penalty: float = 0.06,
    above_target_bonus: float = 0.04,
) -> float:
    target_gap = float(features.get("target_gap_eur_h", 0.0))
    estimated_fare = max(float(features.get("estimated_fare_eur", 0.0)), 0.0)
    fuel_cost = max(float(features.get("fuel_cost_eur", 0.0)), 0.0)
    fuel_share = fuel_cost / max(estimated_fare, 0.01)

    below_target_penalty = 0.0
    if target_gap < 0.0:
        below_target_penalty = clamp(abs(target_gap) / 12.0, 0.0, 1.0) * max(0.0, below_target_penalty_max)

    fuel_penalty = 0.0
    if fuel_share > 0.18:
        fuel_penalty = clamp((fuel_share - 0.18) / 0.22, 0.0, 1.0) * max(0.0, high_fuel_penalty)

    target_bonus = 0.0
    if target_gap > 3.0:
        target_bonus = clamp((target_gap - 3.0) / 9.0, 0.0, 1.0) * max(0.0, above_target_bonus)

    threshold = float(base_threshold) + below_target_penalty + fuel_penalty - target_bonus
    return clamp(threshold, 0.35, 0.9)


def hybrid_accept_decision(
    *,
    features: dict[str, float],
    accept_score: float,
    base_threshold: float = 0.5,
    below_target_penalty_max: float = 0.12,
    high_fuel_penalty: float = 0.06,
    above_target_bonus: float = 0.04,
) -> tuple[str, float]:
    threshold = hybrid_accept_threshold(
        features,
        base_threshold=base_threshold,
        below_target_penalty_max=below_target_penalty_max,
        high_fuel_penalty=high_fuel_penalty,
        above_target_bonus=above_target_bonus,
    )
    return ("accept" if float(accept_score) >= threshold else "reject", threshold)


def _detail_impact(value: float, *, good_threshold: float, bad_threshold: float) -> str:
    if value >= good_threshold:
        return "positive"
    if value <= bad_threshold:
        return "negative"
    return "neutral"


def explanation_details(
    *,
    features: dict[str, float],
    accept_score: float,
    decision_threshold: float | None = None,
    route_source: str | None = None,
    route_duration_min: float | None = None,
) -> list[dict[str, Any]]:
    comps = score_components(features)
    estimated_net_eur_h = float(features.get("estimated_net_eur_h", 0.0))
    estimated_net_eur = float(features.get("estimated_net_eur", 0.0))
    total_duration_min = max(float(features.get("total_duration_min", 1.0)), 1.0)
    target_gap = float(features.get("target_gap_eur_h", 0.0))
    pressure_ratio = max(float(features.get("pressure_ratio", 1.0)), 0.0)
    traffic_factor = max(float(features.get("traffic_factor", 1.0)), 0.0)
    event_pressure = max(float(features.get("event_pressure", 0.0)), 0.0)
    temporal_pressure = max(float(features.get("temporal_pressure", 0.0)), 0.0)
    weather_intensity = clamp(float(features.get("weather_intensity", 0.0)), 0.0, 1.0)
    context_fallback_applied = 1.0 if float(features.get("context_fallback_applied", 0.0)) >= 0.5 else 0.0
    context_stale_sources = max(float(features.get("context_stale_sources", 0.0)), 0.0)
    estimated_fare = max(float(features.get("estimated_fare_eur", 0.0)), 0.0)
    fuel_cost = max(float(features.get("fuel_cost_eur", 0.0)), 0.0)
    fuel_share_pct = (fuel_cost / max(estimated_fare, 0.01)) * 100.0

    details: list[dict[str, Any]] = [
        {
            "code": "net_hourly",
            "label": "Net hourly yield",
            "impact": _detail_impact(estimated_net_eur_h, good_threshold=20.0, bad_threshold=12.0),
            "value": round(estimated_net_eur_h, 3),
            "unit": "eur_per_hour",
            "source": "cost",
        },
        {
            "code": "net_trip",
            "label": "Net trip gain",
            "impact": _detail_impact(estimated_net_eur, good_threshold=8.0, bad_threshold=3.0),
            "value": round(estimated_net_eur, 3),
            "unit": "eur",
            "source": "cost",
        },
        {
            "code": "fuel_share_of_fare",
            "label": "Fuel share of fare",
            "impact": "negative" if fuel_share_pct >= 22.0 else "neutral" if fuel_share_pct >= 12.0 else "positive",
            "value": round(fuel_share_pct, 3),
            "unit": "pct",
            "source": "fuel",
        },
        {
            "code": "total_time",
            "label": "Total time to complete",
            "impact": "negative" if total_duration_min >= 35.0 else "neutral" if total_duration_min >= 22.0 else "positive",
            "value": round(total_duration_min, 3),
            "unit": "min",
            "source": "time",
        },
        {
            "code": "target_gap",
            "label": "Target gap",
            "impact": "positive" if target_gap >= 1.0 else "negative" if target_gap <= -1.0 else "neutral",
            "value": round(target_gap, 3),
            "unit": "eur_per_hour",
            "source": "target",
        },
        {
            "code": "demand_pressure",
            "label": "Demand pressure ratio",
            "impact": _detail_impact(pressure_ratio, good_threshold=1.2, bad_threshold=0.9),
            "value": round(pressure_ratio, 3),
            "unit": "ratio",
            "source": "context",
        },
        {
            "code": "traffic_factor",
            "label": "Traffic factor",
            "impact": "negative" if traffic_factor >= 1.2 else "neutral" if traffic_factor >= 1.05 else "positive",
            "value": round(traffic_factor, 3),
            "unit": "factor",
            "source": "context",
        },
        {
            "code": "event_pressure",
            "label": "Local event pressure",
            "impact": "positive" if event_pressure >= 0.18 else "neutral" if event_pressure >= 0.05 else "negative",
            "value": round(event_pressure, 4),
            "unit": "factor",
            "source": "context",
        },
        {
            "code": "temporal_pressure",
            "label": "Temporal demand pressure",
            "impact": "positive" if temporal_pressure >= 0.14 else "neutral" if temporal_pressure >= 0.06 else "negative",
            "value": round(temporal_pressure, 4),
            "unit": "factor",
            "source": "context",
        },
        {
            "code": "weather_intensity",
            "label": "Weather intensity",
            "impact": "positive" if weather_intensity >= 0.22 else "neutral" if weather_intensity >= 0.08 else "negative",
            "value": round(weather_intensity, 4),
            "unit": "normalized",
            "source": "context",
        },
        {
            "code": "context_fallback",
            "label": "Context fallback mode",
            "impact": "negative" if context_fallback_applied >= 0.5 else "neutral",
            "value": round(context_fallback_applied, 3),
            "unit": "flag",
            "source": "context",
        },
        {
            "code": "context_stale_sources",
            "label": "Stale context sources",
            "impact": "negative" if context_stale_sources >= 2.0 else "neutral",
            "value": round(context_stale_sources, 3),
            "unit": "count",
            "source": "context",
        },
        {
            "code": "score_component_net_hourly",
            "label": "Score component net hourly",
            "impact": _detail_impact(comps["net_hourly"], good_threshold=0.62, bad_threshold=0.42),
            "value": round(comps["net_hourly"], 3),
            "unit": "normalized",
            "source": "cost",
        },
        {
            "code": "score_component_fuel",
            "label": "Score component fuel efficiency",
            "impact": _detail_impact(comps["fuel_efficiency"], good_threshold=0.62, bad_threshold=0.42),
            "value": round(comps["fuel_efficiency"], 3),
            "unit": "normalized",
            "source": "fuel",
        },
        {
            "code": "accept_score",
            "label": "Acceptance score",
            "impact": _detail_impact(float(accept_score), good_threshold=0.65, bad_threshold=0.45),
            "value": round(float(accept_score), 4),
            "unit": "probability",
            "source": "context",
        },
    ]

    if decision_threshold is not None:
        details.append(
            {
                "code": "decision_threshold",
                "label": "Hybrid accept threshold",
                "impact": "neutral",
                "value": round(float(decision_threshold), 4),
                "unit": "probability",
                "source": "target",
            }
        )

    if route_duration_min is not None:
        label = "Route duration estimate"
        if route_source:
            label = f"Route duration ({route_source})"
        details.append(
            {
                "code": "route_duration",
                "label": label,
                "impact": "negative" if route_duration_min >= 30.0 else "neutral" if route_duration_min >= 18.0 else "positive",
                "value": round(float(route_duration_min), 3),
                "unit": "min",
                "source": "routing",
            }
        )

    return details


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
    if not math.isfinite(prob):
        return None, "heuristic"
    return max(0.01, min(prob, 0.99)), "ml"
