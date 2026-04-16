from api.copilot_logic import (
    FEATURE_COLUMNS,
    build_feature_map,
    cost_breakdown,
    heuristic_score,
    model_score,
    recommendation_score,
    validate_model_payload,
)


class _DummyModel:
    def predict_proba(self, rows):
        return [[0.3, 0.7] for _ in rows]


def test_build_feature_map_defaults():
    features = build_feature_map({"estimated_fare_eur": "12.5"})
    assert features["estimated_fare_eur"] == 12.5
    assert features["estimated_duration_min"] >= 1.0
    assert features["supply_index"] >= 0.2
    assert features["pressure_ratio"] > 0


def test_heuristic_score_range():
    features = build_feature_map(
        {
            "estimated_fare_eur": 15.0,
            "estimated_distance_km": 3.2,
            "estimated_duration_min": 20,
            "demand_index": 1.4,
            "supply_index": 0.9,
            "weather_factor": 1.0,
            "traffic_factor": 1.1,
        }
    )
    prob, eur_h, reasons = heuristic_score(features)
    assert 0.0 < prob < 1.0
    assert isinstance(eur_h, float)
    assert len(reasons) >= 1


def test_build_feature_map_with_cost_inputs():
    features = build_feature_map(
        {
            "estimated_fare_eur": 18.0,
            "estimated_distance_km": 4.0,
            "distance_to_pickup_km": 2.0,
            "estimated_duration_min": 18.0,
            "eta_to_pickup_min": 6.0,
            "fuel_price_eur_l": 1.9,
            "vehicle_consumption_l_100km": 8.0,
            "platform_fee_pct": 22.0,
            "other_costs_eur": 1.5,
            "target_hourly_net_eur": 20.0,
        }
    )
    assert features["total_distance_km"] == 6.0
    assert features["total_duration_min"] == 24.0
    assert features["estimated_net_eur"] < features["estimated_fare_eur"]
    assert round(features["target_gap_eur_h"], 3) == round(features["estimated_net_eur_h"] - 20.0, 3)


def test_heuristic_reasons_include_target_gap():
    features = build_feature_map(
        {
            "estimated_fare_eur": 8.0,
            "estimated_distance_km": 6.0,
            "distance_to_pickup_km": 3.5,
            "estimated_duration_min": 28.0,
            "eta_to_pickup_min": 8.0,
            "platform_fee_pct": 30.0,
            "target_hourly_net_eur": 22.0,
            "traffic_factor": 1.25,
        }
    )
    _, _, reasons = heuristic_score(features)
    assert "below_target_hourly_goal" in reasons
    assert "high_platform_fee" in reasons


def test_cost_breakdown_shape():
    features = build_feature_map({"estimated_fare_eur": 12.0, "estimated_distance_km": 3.0})
    breakdown = cost_breakdown(features)
    assert "estimated_net_eur_h" in breakdown
    assert "fuel_cost_eur" in breakdown
    assert isinstance(breakdown["estimated_net_eur"], float)


def test_recommendation_score_range_and_action():
    features = build_feature_map(
        {
            "estimated_fare_eur": 20.0,
            "estimated_distance_km": 3.0,
            "distance_to_pickup_km": 0.7,
            "estimated_duration_min": 18.0,
            "demand_index": 1.4,
            "supply_index": 0.9,
            "target_hourly_net_eur": 17.0,
        }
    )
    score, action, signals = recommendation_score(features, accept_score=0.74)
    assert 0.0 <= score <= 1.0
    assert action in {"accept", "consider", "skip"}
    assert isinstance(signals, list)


def test_recommendation_score_flags_far_pickup():
    features = build_feature_map(
        {
            "estimated_fare_eur": 9.0,
            "estimated_distance_km": 4.0,
            "distance_to_pickup_km": 4.2,
            "estimated_duration_min": 26.0,
            "demand_index": 0.9,
            "supply_index": 1.2,
            "target_hourly_net_eur": 22.0,
        }
    )
    score, action, signals = recommendation_score(features, accept_score=0.41)
    assert score <= 0.65
    assert action in {"consider", "skip"}
    assert "pickup_far" in signals


def test_model_score_fallback_without_model():
    features = build_feature_map({})
    prob, source = model_score(None, features)
    assert prob is None
    assert source == "heuristic"


def test_validate_model_payload_accepts_good_metrics():
    payload = {
        "model": _DummyModel(),
        "feature_columns": FEATURE_COLUMNS,
        "trained_rows": 1200,
        "metrics": {
            "roc_auc": 0.81,
            "average_precision": 0.77,
        },
    }
    ok, reason = validate_model_payload(payload, min_auc=0.62, min_avg_precision=0.45, min_rows=300)
    assert ok is True
    assert reason == "ok"


def test_validate_model_payload_rejects_low_auc():
    payload = {
        "model": _DummyModel(),
        "feature_columns": FEATURE_COLUMNS,
        "trained_rows": 1200,
        "metrics": {
            "roc_auc": 0.51,
            "average_precision": 0.77,
        },
    }
    ok, reason = validate_model_payload(payload, min_auc=0.62, min_avg_precision=0.45, min_rows=300)
    assert ok is False
    assert "roc_auc_below_min" in reason
