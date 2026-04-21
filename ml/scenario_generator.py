"""Générateur de scénarios de démo reproductibles pour FleetStream.

Chaque scénario applique des modificateurs métier sur un jeu d'offres
synthétiques (seed fixe) et produit un dataset + une config JSON rejouables.

Scénarios disponibles:
  heavy_rain      – pluie forte : météo dégradée, trafic ralenti, demande accrue
  traffic_jam     – embouteillages : durées x1.8, profit/h effondré
  fuel_spike      – hausse carburant +40% : marge nette réduite
  peak_event      – grand évènement public : forte pression demand/event
  baseline        – conditions nominales (référence)

Usage:
    python ml/scenario_generator.py
    python ml/scenario_generator.py --scenario heavy_rain --out data/scenarios
    python ml/scenario_generator.py --list
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "ml"))

from backtest_copilot import (
    FUEL_PRICE_EUR_L,
    PLATFORM_FEE_PCT,
    _generate_synthetic_offers,
    _net_eur_h,
    run_backtest,
)

DEFAULT_SEED = 42
DEFAULT_N_OFFERS = 2000

SCENARIOS: dict[str, dict] = {
    "baseline": {
        "label": "Conditions nominales",
        "description": "Météo normale, trafic fluide, prix carburant standard.",
        "modifiers": {},
    },
    "heavy_rain": {
        "label": "Pluie forte",
        "description": "Précipitations intenses : demande +35%, vitesse -25%, météo x1.4.",
        "modifiers": {
            "demand_index_mul": 1.35,
            "weather_factor_add": 0.40,
            "weather_precip_mm_add": 22.0,
            "weather_intensity_add": 0.55,
            "traffic_factor_mul": 0.75,
            "estimated_duration_min_mul": 1.30,
        },
    },
    "traffic_jam": {
        "label": "Trafic saturé",
        "description": "Embouteillages majeurs : durées x1.8, profit horaire effondré.",
        "modifiers": {
            "traffic_factor_mul": 0.45,
            "estimated_duration_min_mul": 1.80,
            "supply_index_mul": 0.70,
        },
    },
    "fuel_spike": {
        "label": "Hausse carburant +40%",
        "description": "Carburant +40% : coûts d'exploitation en hausse, marge nette réduite.",
        "modifiers": {
            "fuel_price_mul": 1.40,
        },
    },
    "peak_event": {
        "label": "Grand évènement public",
        "description": "Stade / concert : pression évènements x3, demande +50%.",
        "modifiers": {
            "demand_index_mul": 1.50,
            "event_pressure_add": 0.45,
            "event_count_nearby_add": 6.0,
            "temporal_pressure_add": 0.35,
        },
    },
}


def apply_modifiers(df: pd.DataFrame, mods: dict, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()

    if "demand_index_mul" in mods:
        df["demand_index"] = np.clip(df["demand_index"] * mods["demand_index_mul"], 0.0, 1.0)
    if "supply_index_mul" in mods:
        df["supply_index"] = np.clip(df["supply_index"] * mods["supply_index_mul"], 0.0, 1.0)
    if "weather_factor_add" in mods:
        df["weather_factor"] = np.clip(df["weather_factor"] + mods["weather_factor_add"], 0.5, 2.5)
    if "weather_precip_mm_add" in mods:
        df["weather_precip_mm"] = df["weather_precip_mm"] + mods["weather_precip_mm_add"]
    if "weather_intensity_add" in mods:
        df["weather_intensity"] = np.clip(df["weather_intensity"] + mods["weather_intensity_add"], 0.0, 1.0)
    if "traffic_factor_mul" in mods:
        df["traffic_factor"] = np.clip(df["traffic_factor"] * mods["traffic_factor_mul"], 0.1, 2.0)
    if "estimated_duration_min_mul" in mods:
        df["estimated_duration_min"] = df["estimated_duration_min"] * mods["estimated_duration_min_mul"]
        if "actual_duration_min" in df.columns:
            df["actual_duration_min"] = df["actual_duration_min"] * mods["estimated_duration_min_mul"]
    if "event_pressure_add" in mods:
        df["event_pressure"] = np.clip(df["event_pressure"] + mods["event_pressure_add"], 0.0, 1.0)
    if "event_count_nearby_add" in mods:
        df["event_count_nearby"] = df["event_count_nearby"] + mods["event_count_nearby_add"]
    if "temporal_pressure_add" in mods:
        df["temporal_pressure"] = np.clip(df["temporal_pressure"] + mods["temporal_pressure_add"], 0.0, 1.0)

    # Fuel price modifier: recalculate net_eur_h with adjusted fuel cost
    fuel_mul = mods.get("fuel_price_mul", 1.0)
    if fuel_mul != 1.0:
        effective_fuel = FUEL_PRICE_EUR_L * fuel_mul
        from backtest_copilot import CONSUMPTION_L_100KM
        def _net_h_adj(r):
            fare = float(r["estimated_fare_eur"] or 0)
            dist = float(r["estimated_distance_km"] or 1)
            dur = float(r["estimated_duration_min"] or 1)
            fuel_cost = dist / 100.0 * CONSUMPTION_L_100KM * effective_fuel
            net = fare * (1.0 - PLATFORM_FEE_PCT / 100.0) - fuel_cost
            return net / (max(dur, 1.0) / 60.0)
        df["estimated_net_eur_h"] = df.apply(_net_h_adj, axis=1)
    else:
        df["estimated_net_eur_h"] = df.apply(
            lambda r: _net_eur_h(
                float(r["estimated_fare_eur"] or 0),
                float(r["estimated_distance_km"] or 1),
                float(r["estimated_duration_min"] or 1),
            ),
            axis=1,
        )

    # Recompute pressure_ratio
    df["pressure_ratio"] = np.clip(
        df["demand_index"] / np.maximum(df["supply_index"], 0.01), 0.0, 5.0
    )

    return df


def generate_scenario(
    name: str,
    out_dir: Path,
    n: int = DEFAULT_N_OFFERS,
    seed: int = DEFAULT_SEED,
) -> dict:
    if name not in SCENARIOS:
        raise ValueError(f"Unknown scenario '{name}'. Available: {list(SCENARIOS)}")

    spec = SCENARIOS[name]
    rng = np.random.default_rng(seed)

    base_df = _generate_synthetic_offers(n, seed=seed)
    df = apply_modifiers(base_df, spec["modifiers"], rng)

    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / f"{name}_offers.parquet"
    df.to_parquet(parquet_path, index=False)

    config = {
        "scenario": name,
        "label": spec["label"],
        "description": spec["description"],
        "seed": seed,
        "n_offers": n,
        "modifiers": spec["modifiers"],
        "parquet_path": str(parquet_path),
    }
    config_path = out_dir / f"{name}_config.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    print(f"[scenario] {name}: {n} offres → {parquet_path.name}")
    return config


def run_all_scenarios(
    out_dir: Path,
    n: int = DEFAULT_N_OFFERS,
    seed: int = DEFAULT_SEED,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    for name in SCENARIOS:
        results[name] = generate_scenario(name, out_dir / name, n=n, seed=seed)

    # Comparison summary: run backtest on each scenario
    print("\n── Scénarios : impact sur le Copilot ─────────────────────────────")
    print(f"{'Scénario':<18} {'Accept%':>8} {'Net €/h':>9} {'Label'}")
    print("─" * 65)

    comparison = []
    for name in SCENARIOS:
        scenario_parquet_dir = out_dir / name
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_backtest(
                scenario_parquet_dir,
                Path(tmp),
                model_path=None,
                _override_df=pd.read_parquet(scenario_parquet_dir / f"{name}_offers.parquet"),
            )
        copilot = next(s for s in summary["strategies"] if s["strategy"] == "copilot")
        comparison.append({"scenario": name, **copilot})
        print(f"{name:<18} {copilot['accept_rate']*100:>7.1f}% {copilot['net_eur_h_mean']:>9.2f}  {SCENARIOS[name]['label']}")

    print("─" * 65)

    summary_path = out_dir / "scenarios_comparison.json"
    with open(summary_path, "w") as f:
        json.dump({"scenarios": comparison}, f, indent=2)
    print(f"\n[scenario] Comparaison sauvée : {summary_path}")

    return {"scenarios": comparison}


def main() -> None:
    parser = argparse.ArgumentParser(description="Générateur de scénarios FleetStream")
    parser.add_argument("--scenario", default=None, help="Scénario à générer (ou 'all')")
    parser.add_argument("--out", default="data/scenarios", help="Répertoire de sortie")
    parser.add_argument("--n", type=int, default=DEFAULT_N_OFFERS, help="Nombre d'offres")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Graine aléatoire")
    parser.add_argument("--list", action="store_true", help="Lister les scénarios disponibles")
    args = parser.parse_args()

    if args.list:
        for name, spec in SCENARIOS.items():
            print(f"  {name:<18} {spec['label']}")
        return

    out_dir = _ROOT / args.out
    if args.scenario is None or args.scenario == "all":
        run_all_scenarios(out_dir, n=args.n, seed=args.seed)
    else:
        generate_scenario(args.scenario, out_dir / args.scenario, n=args.n, seed=args.seed)


if __name__ == "__main__":
    main()
