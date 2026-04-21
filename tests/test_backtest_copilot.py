"""Tests de cohérence du backtest Copilot vs baselines (COP-015)."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "ml") not in sys.path:
    sys.path.insert(0, str(_ROOT / "ml"))

from backtest_copilot import (
    _coerce_max_offers,
    _compute_kpis,
    _generate_synthetic_offers,
    _net_eur,
    _net_eur_h,
    run_backtest,
)
import numpy as np
import pandas as pd
import tempfile


def test_net_eur_scalar():
    net = _net_eur(10.0, 5.0)
    assert net < 10.0
    assert net > 0.0


def test_net_eur_h_positive():
    val = _net_eur_h(12.0, 3.0, 20.0)
    assert val > 0.0


def test_synthetic_offers_shape():
    df = _generate_synthetic_offers(500, seed=0)
    assert len(df) == 500
    assert "estimated_fare_eur" in df.columns
    assert "estimated_net_eur_h" in df.columns
    assert (df["accept_rate"] if "accept_rate" in df.columns else df["demand_index"] >= 0).all()


def test_synthetic_offers_seed_stable():
    df1 = _generate_synthetic_offers(100, seed=42)
    df2 = _generate_synthetic_offers(100, seed=42)
    assert df1["estimated_fare_eur"].tolist() == df2["estimated_fare_eur"].tolist()


def test_compute_kpis_always_accept():
    df = _generate_synthetic_offers(200, seed=1)
    mask = np.ones(len(df), dtype=bool)
    kpis = _compute_kpis(df, mask, "always_accept")
    assert kpis["accept_rate"] == 1.0
    assert kpis["accepted"] == 200
    assert kpis["net_eur_total"] > 0


def test_compute_kpis_never_accept():
    df = _generate_synthetic_offers(100, seed=2)
    mask = np.zeros(len(df), dtype=bool)
    kpis = _compute_kpis(df, mask, "none")
    assert kpis["accept_rate"] == 0.0
    assert kpis["net_eur_total"] == 0.0


def test_compute_kpis_fallbacks_to_estimated_when_actual_is_nan():
    df = pd.DataFrame(
        {
            "courier_id": ["drv_a", "drv_b"],
            "estimated_fare_eur": [10.0, 12.0],
            "estimated_distance_km": [3.0, 4.0],
            "estimated_duration_min": [15.0, 20.0],
            "actual_fare_eur": [np.nan, np.nan],
            "actual_distance_km": [np.nan, np.nan],
            "actual_duration_min": [np.nan, np.nan],
        }
    )
    mask = np.array([True, True], dtype=bool)
    kpis = _compute_kpis(df, mask, "test")
    assert kpis["net_eur_total"] > 0.0
    assert kpis["km_total"] > 0.0


def test_backtest_produces_files():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "reports"
        parquet = _ROOT / "data" / "parquet_events"
        summary = run_backtest(parquet, out, model_path=None)

        assert (out / "backtest_summary.json").exists()
        assert (out / "backtest_summary.csv").exists()

        strategies = {s["strategy"] for s in summary["strategies"]}
        assert strategies == {"copilot", "greedy_eur_h", "random", "always_accept"}


def test_backtest_kpi_coherence():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "reports"
        parquet = _ROOT / "data" / "parquet_events"
        summary = run_backtest(parquet, out, model_path=None)

        by_strat = {s["strategy"]: s for s in summary["strategies"]}

        # always_accept should have highest km and fuel
        assert by_strat["always_accept"]["km_total"] >= by_strat["copilot"]["km_total"]
        # accept_rate in [0, 1]
        for s in summary["strategies"]:
            assert 0.0 <= s["accept_rate"] <= 1.0
        # copilot accept_rate < always_accept
        assert by_strat["copilot"]["accept_rate"] <= by_strat["always_accept"]["accept_rate"]


def test_coerce_max_offers():
    assert _coerce_max_offers(None) is None
    assert _coerce_max_offers(0) is None
    assert _coerce_max_offers(-5) is None
    assert _coerce_max_offers(123) == 123


def test_backtest_respects_max_offers_with_override_df():
    df = _generate_synthetic_offers(800, seed=123)
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "reports"
        summary = run_backtest(
            _ROOT / "data" / "parquet_events",
            out,
            model_path=None,
            _override_df=df,
            max_offers=150,
        )
        assert summary["meta"]["n_offers"] == 150
        assert summary["meta"]["max_offers"] == 150
