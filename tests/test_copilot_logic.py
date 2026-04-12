from api.copilot_logic import (
    FEATURE_COLUMNS,
    build_feature_map,
    heuristic_score,
    model_score,
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
