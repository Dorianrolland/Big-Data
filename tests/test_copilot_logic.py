from api.copilot_logic import (
    FEATURE_COLUMNS,
    build_feature_map,
    cost_breakdown,
    explanation_details,
    heuristic_score,
    hybrid_accept_decision,
    hybrid_accept_threshold,
    model_score,
    normalize_explainability_weights,
    normalize_objective_weights,
    normalize_score_weights,
    recommendation_score,
    ranking_objective_score,
    score_decomposition,
    score_components,
    standardized_reason_codes,
    validate_model_payload,
    weighted_offer_score,
)


class _DummyModel:
    def predict_proba(self, rows):
        return [[0.3, 0.7] for _ in rows]


class _NanModel:
    def predict_proba(self, rows):
        return [[0.5, float("nan")] for _ in rows]


def test_build_feature_map_defaults():
    features = build_feature_map({"estimated_fare_eur": "12.5"})
    assert features["estimated_fare_eur"] == 12.5
    assert features["estimated_duration_min"] >= 1.0
    assert features["supply_index"] >= 0.2
    assert features["pressure_ratio"] > 0


def test_build_feature_map_sanitizes_non_finite_inputs():
    features = build_feature_map(
        {
            "estimated_fare_eur": "nan",
            "estimated_distance_km": "inf",
            "estimated_duration_min": "-inf",
            "demand_index": "nan",
            "supply_index": 0.0,
        }
    )
    assert features["estimated_fare_eur"] == 0.0
    assert features["estimated_distance_km"] == 0.0
    assert features["estimated_duration_min"] == 1.0
    assert features["demand_index"] == 1.0
    assert features["supply_index"] == 0.2


def test_build_feature_map_ingests_context_v2_fields():
    features = build_feature_map(
        {
            "estimated_fare_eur": 15.0,
            "estimated_distance_km": 3.0,
            "event_pressure": 0.24,
            "event_count_nearby": 5,
            "weather_precip_mm": 2.4,
            "weather_wind_kmh": 20.0,
            "weather_intensity": 0.42,
            "temporal_hour_local": 17.5,
            "is_peak_hour": 1,
            "is_weekend": 0,
            "is_holiday": 0,
            "temporal_pressure": 0.14,
            "context_fallback_applied": 1,
            "context_stale_sources": 2,
        }
    )
    assert features["event_pressure"] == 0.24
    assert features["event_count_nearby"] == 5.0
    assert features["weather_intensity"] == 0.42
    assert features["is_peak_hour"] == 1.0
    assert features["temporal_pressure"] == 0.14
    assert features["context_fallback_applied"] == 1.0
    assert features["context_stale_sources"] == 2.0


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
            "fuel_price_usd_gallon": 3.8,
            "vehicle_mpg": 29.4,
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


def test_heuristic_reasons_include_context_fallback_mode():
    features = build_feature_map(
        {
            "estimated_fare_eur": 13.0,
            "estimated_distance_km": 3.0,
            "estimated_duration_min": 16.0,
            "context_fallback_applied": 1,
            "context_stale_sources": 2,
        }
    )
    _, _, reasons = heuristic_score(features)
    assert "context_fallback_mode" in reasons


def test_cost_breakdown_shape():
    features = build_feature_map({"estimated_fare_eur": 12.0, "estimated_distance_km": 3.0})
    breakdown = cost_breakdown(features)
    assert "estimated_net_eur_h" in breakdown
    assert "fuel_cost_usd" in breakdown
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


def test_recommendation_score_changes_with_objective_weights():
    features = build_feature_map(
        {
            "estimated_fare_eur": 24.0,
            "estimated_distance_km": 16.0,
            "distance_to_pickup_km": 4.0,
            "estimated_duration_min": 40.0,
            "eta_to_pickup_min": 7.0,
            "target_hourly_net_eur": 16.0,
            "fuel_price_usd_gallon": 10.0,
            "vehicle_mpg": 8.0,
        }
    )
    gain_first, _, _ = recommendation_score(
        features,
        accept_score=0.78,
        objective_weights={"w_gain": 1.0, "w_time": 0.0, "w_fuel": 0.0},
    )
    fuel_first, _, _ = recommendation_score(
        features,
        accept_score=0.78,
        objective_weights={"w_gain": 0.0, "w_time": 0.0, "w_fuel": 1.0},
    )
    assert gain_first > fuel_first


def test_normalize_score_weights_rescales_and_guards():
    norm = normalize_score_weights(
        {
            "net_hourly": 4.0,
            "net_trip": 0.0,
            "fuel_efficiency": -1.0,
            "time_efficiency": 1.0,
            "context": 1.0,
        }
    )
    assert round(sum(norm.values()), 6) == 1.0
    assert norm["fuel_efficiency"] == 0.0
    assert norm["net_hourly"] > norm["time_efficiency"]


def test_normalize_score_weights_falls_back_to_defaults_on_zero_total():
    norm = normalize_score_weights(
        {
            "net_hourly": 0.0,
            "net_trip": -5.0,
            "fuel_efficiency": 0.0,
            "time_efficiency": -1.0,
            "context": 0.0,
        }
    )
    assert round(sum(norm.values()), 6) == 1.0
    assert round(norm["net_hourly"], 2) == 0.46
    assert round(norm["net_trip"], 2) == 0.18
    assert round(norm["context"], 2) == 0.08


def test_normalize_objective_weights_rescales_and_guards():
    norm = normalize_objective_weights(
        {
            "w_gain": 5.0,
            "w_time": 0.0,
            "w_fuel": -2.0,
        }
    )
    assert round(sum(norm.values()), 6) == 1.0
    assert norm["w_gain"] == 1.0
    assert norm["w_time"] == 0.0
    assert norm["w_fuel"] == 0.0


def test_normalize_objective_weights_falls_back_on_zero_total():
    norm = normalize_objective_weights({"w_gain": 0.0, "w_time": 0.0, "w_fuel": 0.0})
    assert round(sum(norm.values()), 6) == 1.0
    assert norm["w_gain"] > norm["w_fuel"]
    assert norm["w_time"] > 0.0


def test_normalize_explainability_weights_rescales_and_fallback():
    norm = normalize_explainability_weights({"gain": 0.0, "time": 8.0, "fuel": 2.0, "risk": -4.0})
    assert round(sum(norm.values()), 6) == 1.0
    assert norm["time"] > norm["fuel"] > norm["gain"]
    fallback = normalize_explainability_weights({"gain": 0.0, "time": 0.0, "fuel": 0.0, "risk": 0.0})
    assert round(sum(fallback.values()), 6) == 1.0
    assert round(fallback["gain"], 2) == 0.42
    assert round(fallback["risk"], 2) == 0.20


def test_score_decomposition_has_expected_axes_and_consistent_totals():
    features = build_feature_map(
        {
            "estimated_fare_eur": 19.0,
            "estimated_distance_km": 4.5,
            "distance_to_pickup_km": 0.9,
            "estimated_duration_min": 20.0,
            "target_hourly_net_eur": 18.0,
            "traffic_factor": 1.05,
            "weather_intensity": 0.12,
        }
    )
    decomposition = score_decomposition(features, 0.77)
    assert decomposition["version"] == "v2"
    assert set(decomposition["dimensions"]) == {"gain", "time", "fuel", "risk"}
    dims = decomposition["dimensions"]
    for axis in ("gain", "time", "fuel", "risk"):
        assert 0.0 <= float(dims[axis]["score"]) <= 1.0
        assert 0.0 <= float(dims[axis]["weight"]) <= 1.0
        assert 0.0 <= float(dims[axis]["contribution"]) <= 1.0
        assert dims[axis]["impact"] in {"positive", "negative", "neutral"}
    total_weight = sum(float(dims[axis]["weight"]) for axis in dims)
    total_contribution = sum(float(dims[axis]["contribution"]) for axis in dims)
    assert round(total_weight, 4) == 1.0
    assert abs(float(decomposition["total_score"]) - total_contribution) <= 1e-4


def test_standardized_reason_codes_reflect_threshold_and_context():
    features = build_feature_map(
        {
            "estimated_fare_eur": 9.0,
            "estimated_distance_km": 7.2,
            "distance_to_pickup_km": 3.4,
            "estimated_duration_min": 34.0,
            "target_hourly_net_eur": 26.0,
            "traffic_factor": 1.55,
            "weather_intensity": 0.72,
            "context_fallback_applied": 1.0,
            "context_stale_sources": 3.0,
        }
    )
    codes = standardized_reason_codes(features, 0.44, decision_threshold=0.58)
    assert "TARGET_BELOW" in codes
    assert "CONTEXT_FALLBACK" in codes
    assert "CONTEXT_STALE" in codes
    assert "DECISION_BELOW_THRESHOLD" in codes
    assert "RISK_HIGH" in codes


def test_standardized_reason_codes_balanced_profile_fallback():
    codes = standardized_reason_codes(
        {"target_gap_eur_h": 0.0, "context_fallback_applied": 0.0, "context_stale_sources": 0.0},
        0.5,
        decomposition={
            "version": "v2",
            "total_score": 0.5,
            "dimensions": {
                "gain": {"score": 0.5},
                "time": {"score": 0.5},
                "fuel": {"score": 0.5},
                "risk": {"score": 0.5},
            },
        },
    )
    assert codes == ["PROFILE_BALANCED"]


def test_ranking_objective_score_extreme_weights_change_result():
    features = build_feature_map(
        {
            "estimated_fare_eur": 26.0,
            "estimated_distance_km": 18.0,
            "distance_to_pickup_km": 3.5,
            "estimated_duration_min": 44.0,
            "eta_to_pickup_min": 8.0,
            "target_hourly_net_eur": 18.0,
            "fuel_price_usd_gallon": 10.0,
            "vehicle_mpg": 8.0,
        }
    )
    gain_score, _, _ = ranking_objective_score(
        features,
        0.82,
        objective_weights={"w_gain": 1.0, "w_time": 0.0, "w_fuel": 0.0},
    )
    fuel_score, _, _ = ranking_objective_score(
        features,
        0.82,
        objective_weights={"w_gain": 0.0, "w_time": 0.0, "w_fuel": 1.0},
    )
    assert gain_score > fuel_score


def test_score_components_are_clamped():
    features = build_feature_map(
        {
            "estimated_fare_eur": 4.0,
            "estimated_distance_km": 8.0,
            "distance_to_pickup_km": 4.0,
            "estimated_duration_min": 42.0,
            "eta_to_pickup_min": 10.0,
            "demand_index": 0.6,
            "supply_index": 1.8,
            "traffic_factor": 1.35,
            "target_hourly_net_eur": 24.0,
        }
    )
    comps = score_components(features)
    assert set(comps) == {"net_hourly", "net_trip", "fuel_efficiency", "time_efficiency", "context"}
    assert all(0.0 <= v <= 1.0 for v in comps.values())


def test_score_components_context_signal_and_fallback_edge_case():
    rich_context = build_feature_map(
        {
            "estimated_fare_eur": 16.0,
            "estimated_distance_km": 3.2,
            "estimated_duration_min": 17.0,
            "demand_index": 1.3,
            "supply_index": 0.9,
            "weather_factor": 1.08,
            "traffic_factor": 1.05,
            "event_pressure": 0.35,
            "event_count_nearby": 4,
            "weather_intensity": 0.3,
            "is_peak_hour": 1,
            "temporal_pressure": 0.18,
            "context_fallback_applied": 0,
            "context_stale_sources": 0,
        }
    )
    degraded_context = build_feature_map(
        {
            "estimated_fare_eur": 16.0,
            "estimated_distance_km": 3.2,
            "estimated_duration_min": 17.0,
            "demand_index": 1.3,
            "supply_index": 0.9,
            "weather_factor": 1.08,
            "traffic_factor": 1.05,
            "event_pressure": 0.35,
            "event_count_nearby": 4,
            "weather_intensity": 0.3,
            "is_peak_hour": 1,
            "temporal_pressure": 0.18,
            "context_fallback_applied": 1,
            "context_stale_sources": 3,
        }
    )
    rich = score_components(rich_context)
    degraded = score_components(degraded_context)
    assert rich["context"] > degraded["context"]


def test_weighted_offer_score_short_vs_long_edge_case():
    short_features = build_feature_map(
        {
            "estimated_fare_eur": 10.0,
            "estimated_distance_km": 1.2,
            "estimated_duration_min": 9.0,
            "target_hourly_net_eur": 18.0,
        }
    )
    long_features = build_feature_map(
        {
            "estimated_fare_eur": 18.0,
            "estimated_distance_km": 8.0,
            "estimated_duration_min": 40.0,
            "target_hourly_net_eur": 18.0,
        }
    )
    short_score, _, _ = weighted_offer_score(short_features)
    long_score, _, _ = weighted_offer_score(long_features)
    assert short_features["estimated_net_eur_h"] > long_features["estimated_net_eur_h"]
    assert short_score > long_score


def test_hybrid_accept_threshold_penalizes_below_target_and_fuel():
    risky = build_feature_map(
        {
            "estimated_fare_eur": 7.0,
            "estimated_distance_km": 7.0,
            "estimated_duration_min": 30.0,
            "fuel_price_usd_gallon": 4.2,
            "vehicle_mpg": 21.4,
            "target_hourly_net_eur": 24.0,
        }
    )
    clean = build_feature_map(
        {
            "estimated_fare_eur": 15.0,
            "estimated_distance_km": 2.0,
            "estimated_duration_min": 14.0,
            "target_hourly_net_eur": 18.0,
        }
    )
    assert hybrid_accept_threshold(risky) > hybrid_accept_threshold(clean)


def test_hybrid_accept_threshold_is_clamped_to_bounds():
    high_threshold_features = build_feature_map(
        {
            "estimated_fare_eur": 4.0,
            "estimated_distance_km": 8.0,
            "estimated_duration_min": 35.0,
            "fuel_price_usd_gallon": 4.8,
            "vehicle_mpg": 15.7,
            "target_hourly_net_eur": 35.0,
        }
    )
    low_threshold_features = build_feature_map(
        {
            "estimated_fare_eur": 40.0,
            "estimated_distance_km": 1.0,
            "estimated_duration_min": 8.0,
            "target_hourly_net_eur": 1.0,
        }
    )

    high = hybrid_accept_threshold(
        high_threshold_features,
        base_threshold=0.8,
        below_target_penalty_max=0.5,
        high_fuel_penalty=0.5,
        above_target_bonus=0.0,
    )
    low = hybrid_accept_threshold(
        low_threshold_features,
        base_threshold=0.4,
        below_target_penalty_max=0.0,
        high_fuel_penalty=0.0,
        above_target_bonus=0.5,
    )
    assert high == 0.9
    assert low == 0.35


def test_hybrid_accept_decision_exact_threshold_edge():
    features = build_feature_map(
        {
            "estimated_fare_eur": 13.5,
            "estimated_distance_km": 3.0,
            "estimated_duration_min": 17.0,
            "target_hourly_net_eur": 18.0,
        }
    )
    threshold = hybrid_accept_threshold(features)
    decision, used_threshold = hybrid_accept_decision(features=features, accept_score=threshold)
    assert decision == "accept"
    assert used_threshold == threshold


def test_explanation_details_shape_and_values():
    features = build_feature_map(
        {
            "estimated_fare_eur": 16.0,
            "estimated_distance_km": 3.3,
            "estimated_duration_min": 16.0,
            "traffic_factor": 1.1,
            "target_hourly_net_eur": 18.0,
        }
    )
    details = explanation_details(
        features=features,
        accept_score=0.62,
        decision_threshold=0.54,
        route_source="estimated",
        route_duration_min=18.5,
    )
    assert len(details) >= 8
    assert any(d["code"] == "net_hourly" for d in details)
    assert any(d["source"] == "routing" for d in details)
    assert any(d["code"] == "event_pressure" for d in details)


def test_explanation_details_omits_threshold_when_not_provided():
    features = build_feature_map({"estimated_fare_eur": 11.0, "estimated_distance_km": 2.0})
    details = explanation_details(features=features, accept_score=0.5)
    assert not any(d["code"] == "decision_threshold" for d in details)


def test_model_score_fallback_without_model():
    features = build_feature_map({})
    prob, source = model_score(None, features)
    assert prob is None
    assert source == "heuristic"


def test_model_score_nan_probability_falls_back_to_heuristic():
    features = build_feature_map({"estimated_fare_eur": 12.0})
    payload = {
        "model": _NanModel(),
        "feature_columns": FEATURE_COLUMNS,
    }
    prob, source = model_score(payload, features)
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
