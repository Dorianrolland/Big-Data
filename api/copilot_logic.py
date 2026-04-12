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


def build_feature_map(raw: dict[str, Any]) -> dict[str, float]:
    estimated_fare = as_float(raw.get("estimated_fare_eur"), 0.0)
    estimated_distance = as_float(raw.get("estimated_distance_km"), 0.0)
    estimated_duration = max(as_float(raw.get("estimated_duration_min"), 1.0), 1.0)
    demand_index = as_float(raw.get("demand_index"), 1.0)
    supply_index = max(as_float(raw.get("supply_index"), 1.0), 0.2)
    weather_factor = as_float(raw.get("weather_factor"), 1.0)
    traffic_factor = as_float(raw.get("traffic_factor"), 1.0)

    pressure_ratio = demand_index / supply_index
    estimated_net_eur_h = ((estimated_fare - (estimated_distance * 0.35)) / estimated_duration) * 60.0

    return {
        "estimated_fare_eur": estimated_fare,
        "estimated_distance_km": estimated_distance,
        "estimated_duration_min": estimated_duration,
        "demand_index": demand_index,
        "supply_index": supply_index,
        "weather_factor": weather_factor,
        "traffic_factor": traffic_factor,
        "pressure_ratio": pressure_ratio,
        "estimated_net_eur_h": estimated_net_eur_h,
    }


def heuristic_score(features: dict[str, float]) -> tuple[float, float, list[str]]:
    estimated_fare = features["estimated_fare_eur"]
    estimated_distance = features["estimated_distance_km"]
    estimated_duration = max(features["estimated_duration_min"], 1.0)
    demand_index = features["demand_index"]
    supply_index = max(features["supply_index"], 0.2)
    weather_factor = features["weather_factor"]
    traffic_factor = features["traffic_factor"]

    variable_cost = estimated_distance * 0.35
    eur_per_hour_net = ((estimated_fare - variable_cost) / estimated_duration) * 60
    pressure_ratio = demand_index / supply_index

    logits = (
        (eur_per_hour_net - 16.0) / 9.5
        + (pressure_ratio - 1.0) * 0.85
        + (weather_factor - 1.0) * 0.4
        - (traffic_factor - 1.0) * 0.35
    )
    score = max(0.01, min(sigmoid(logits), 0.99))

    reasons: list[str] = []
    if eur_per_hour_net >= 22:
        reasons.append("high_estimated_net_revenue")
    elif eur_per_hour_net < 14:
        reasons.append("low_estimated_net_revenue")

    if pressure_ratio >= 1.25:
        reasons.append("strong_demand_pressure")
    elif pressure_ratio < 0.9:
        reasons.append("weak_demand_pressure")

    if weather_factor < 0.95:
        reasons.append("weather_downside")
    if traffic_factor > 1.2:
        reasons.append("traffic_penalty")

    if not reasons:
        reasons.append("balanced_offer_profile")

    return score, eur_per_hour_net, reasons


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
