"""Tests scénarios de démo reproductibles (COP-021)."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "ml"))

from scenario_generator import (
    SCENARIOS,
    apply_modifiers,
    generate_scenario,
    run_all_scenarios,
)
from backtest_copilot import _generate_synthetic_offers


def _base_df(n=200):
    return _generate_synthetic_offers(n, seed=42)


def test_scenarios_list_complete():
    required = {"baseline", "heavy_rain", "traffic_jam", "fuel_spike", "peak_event"}
    assert required.issubset(set(SCENARIOS))


def test_heavy_rain_increases_duration():
    df = _base_df()
    modified = apply_modifiers(df, SCENARIOS["heavy_rain"]["modifiers"], np.random.default_rng(0))
    assert (modified["estimated_duration_min"] > df["estimated_duration_min"]).all()


def test_traffic_jam_lowers_net_eur_h():
    df = _base_df()
    modified = apply_modifiers(df, SCENARIOS["traffic_jam"]["modifiers"], np.random.default_rng(0))
    assert modified["estimated_net_eur_h"].mean() < df["estimated_net_eur_h"].mean()


def test_fuel_spike_lowers_net_eur_h():
    df = _base_df()
    modified = apply_modifiers(df, SCENARIOS["fuel_spike"]["modifiers"], np.random.default_rng(0))
    assert modified["estimated_net_eur_h"].mean() < df["estimated_net_eur_h"].mean()


def test_peak_event_raises_event_pressure():
    df = _base_df()
    modified = apply_modifiers(df, SCENARIOS["peak_event"]["modifiers"], np.random.default_rng(0))
    assert modified["event_pressure"].mean() > df["event_pressure"].mean()
    assert modified["demand_index"].mean() > df["demand_index"].mean()


def test_scenario_seed_reproducible():
    with tempfile.TemporaryDirectory() as tmp:
        out1 = Path(tmp) / "s1"
        out2 = Path(tmp) / "s2"
        generate_scenario("heavy_rain", out1, n=100, seed=42)
        generate_scenario("heavy_rain", out2, n=100, seed=42)
        df1 = pd.read_parquet(out1 / "heavy_rain_offers.parquet")
        df2 = pd.read_parquet(out2 / "heavy_rain_offers.parquet")
        pd.testing.assert_frame_equal(df1, df2)


def test_generate_scenario_creates_files():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "rain"
        generate_scenario("heavy_rain", out, n=100, seed=0)
        assert (out / "heavy_rain_offers.parquet").exists()
        assert (out / "heavy_rain_config.json").exists()
        cfg = json.loads((out / "heavy_rain_config.json").read_text())
        assert cfg["scenario"] == "heavy_rain"
        assert cfg["n_offers"] == 100
        assert cfg["seed"] == 0


def test_run_all_scenarios_produces_comparison():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "scenarios"
        result = run_all_scenarios(out, n=200, seed=99)
        assert len(result["scenarios"]) == len(SCENARIOS)
        names = {s["scenario"] for s in result["scenarios"]}
        assert "copilot" not in names  # scenario names, not strategy names
        assert (out / "scenarios_comparison.json").exists()
