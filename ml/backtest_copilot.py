"""Backtesting offline: Copilot vs baseline strategies.

Compares four decision strategies on order-offer events:
  copilot          – accept if ML score >= threshold (or heuristic if no model)
  greedy_eur_h     – accept if estimated_net_eur_h >= target
  random           – accept with fixed probability (seed-stable)
  always_accept    – accept everything

Usage:
    python ml/backtest_copilot.py
    python ml/backtest_copilot.py --parquet data/parquet_events --out data/reports
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "api"))

FUEL_PRICE_EUR_L = float(os.getenv("FUEL_PRICE_EUR_L", "1.85"))
CONSUMPTION_L_100KM = float(os.getenv("CONSUMPTION_L_100KM", "7.5"))
PLATFORM_FEE_PCT = float(os.getenv("PLATFORM_FEE_PCT", "25.0"))
COPILOT_SCORE_THRESHOLD = float(os.getenv("COPILOT_SCORE_THRESHOLD", "0.55"))
GREEDY_NET_EUR_H_MIN = float(os.getenv("GREEDY_NET_EUR_H_MIN", "12.0"))
RANDOM_ACCEPT_PROB = float(os.getenv("RANDOM_ACCEPT_PROB", "0.6"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))


def _fuel_cost(distance_km: float) -> float:
    return distance_km / 100.0 * CONSUMPTION_L_100KM * FUEL_PRICE_EUR_L


def _net_eur(fare: float, distance_km: float) -> float:
    gross = fare * (1.0 - PLATFORM_FEE_PCT / 100.0)
    return gross - _fuel_cost(distance_km)


def _net_eur_h(fare, distance_km, duration_min):
    """Works with scalars and numpy arrays."""
    dur = np.where(np.asarray(duration_min) <= 0, 1.0, np.asarray(duration_min)) if not isinstance(duration_min, float) else max(duration_min, 1.0)
    return _net_eur(fare, distance_km) / (dur / 60.0)


def _copilot_score(row: pd.Series, model: object | None) -> float:
    """Return ML probability or fallback heuristic score."""
    if model is not None:
        try:
            from copilot_logic import FEATURE_COLUMNS
            feats = []
            for col in FEATURE_COLUMNS:
                feats.append(float(row.get(col, 0.0) or 0.0))
            return float(model.predict_proba([feats])[0][1])
        except Exception:
            pass
    # Heuristic fallback
    net_h = _net_eur_h(
        float(row.get("estimated_fare_eur", 0) or 0),
        float(row.get("estimated_distance_km", 1) or 1),
        float(row.get("estimated_duration_min", 1) or 1),
    )
    demand = float(row.get("demand_index", 0.5) or 0.5)
    supply = float(row.get("supply_index", 0.5) or 0.5)
    pressure = demand / max(supply, 0.01)
    raw = (net_h / 20.0) * 0.55 + min(pressure / 3.0, 1.0) * 0.30 + 0.15
    return min(max(raw, 0.0), 1.0)


def _load_offers(parquet_dir: Path) -> pd.DataFrame:
    offers_path = parquet_dir / "topic=order-offers-v1"
    if not offers_path.exists():
        return pd.DataFrame()
    con = duckdb.connect()
    df = con.execute(
        f"SELECT * FROM read_parquet('{offers_path}/**/*.parquet', hive_partitioning=true)"
    ).fetchdf()
    con.close()
    return df


def _generate_synthetic_offers(n: int = 2000, seed: int = 42) -> pd.DataFrame:
    """Generate realistic synthetic offers when parquet data is too sparse."""
    rng = np.random.default_rng(seed)
    hours = rng.integers(0, 24, size=n)
    is_peak = ((hours >= 7) & (hours <= 9)) | ((hours >= 17) & (hours <= 20))
    demand = rng.uniform(0.3, 1.0, n) + is_peak.astype(float) * 0.3
    supply = rng.uniform(0.2, 0.9, n)
    fare = rng.uniform(4.0, 25.0, n) * (1.0 + demand * 0.3)
    dist = rng.uniform(1.5, 15.0, n)
    dur = dist / rng.uniform(15.0, 35.0, n) * 60.0
    weather = rng.choice([0.8, 1.0, 1.15, 1.35], n, p=[0.3, 0.45, 0.15, 0.10])
    traffic = rng.uniform(0.7, 1.3, n)
    event_p = rng.uniform(0.0, 0.5, n)
    return pd.DataFrame(
        {
            "offer_id": [f"synth-{i:05d}" for i in range(n)],
            "courier_id": [f"drv-{i % 20:03d}" for i in range(n)],
            "zone_id": rng.choice(["Z01", "Z02", "Z03", "Z04", "Z05"], n).tolist(),
            "estimated_fare_eur": fare,
            "estimated_distance_km": dist,
            "estimated_duration_min": dur,
            "actual_fare_eur": fare * rng.uniform(0.92, 1.08, n),
            "actual_distance_km": dist * rng.uniform(0.95, 1.05, n),
            "actual_duration_min": dur * rng.uniform(0.90, 1.10, n),
            "demand_index": np.clip(demand, 0.0, 1.0),
            "supply_index": np.clip(supply, 0.0, 1.0),
            "weather_factor": weather,
            "traffic_factor": traffic,
            "event_pressure": event_p,
            "event_count_nearby": rng.integers(0, 8, n).astype(float),
            "weather_precip_mm": rng.choice([0.0, 2.5, 10.0, 25.0], n, p=[0.55, 0.2, 0.15, 0.1]),
            "weather_wind_kmh": rng.uniform(0.0, 60.0, n),
            "weather_intensity": rng.uniform(0.0, 1.0, n),
            "temporal_hour_local": hours.astype(float),
            "is_peak_hour": is_peak.astype(float),
            "is_weekend": rng.choice([0.0, 1.0], n, p=[0.7, 0.3]),
            "is_holiday": rng.choice([0.0, 1.0], n, p=[0.92, 0.08]),
            "temporal_pressure": rng.uniform(0.0, 1.0, n),
            "context_fallback_applied": rng.choice([0.0, 1.0], n, p=[0.85, 0.15]),
            "context_stale_sources": rng.integers(0, 3, n).astype(float),
            "pressure_ratio": np.clip(demand / np.maximum(supply, 0.01), 0.0, 5.0),
            "estimated_net_eur_h": _net_eur_h(fare, dist, dur),
        }
    )


def _compute_kpis(df: pd.DataFrame, accepted: np.ndarray, strategy: str) -> dict:
    acc_df = df[accepted].copy()
    total = len(df)
    n_accepted = int(accepted.sum())

    if n_accepted == 0:
        return {
            "strategy": strategy,
            "total_offers": total,
            "accepted": 0,
            "accept_rate": 0.0,
            "net_eur_total": 0.0,
            "net_eur_h_mean": 0.0,
            "km_total": 0.0,
            "fuel_cost_eur": 0.0,
            "trips_per_driver": 0.0,
        }

    fare_col = "actual_fare_eur" if "actual_fare_eur" in acc_df.columns else "estimated_fare_eur"
    dist_col = "actual_distance_km" if "actual_distance_km" in acc_df.columns else "estimated_distance_km"
    dur_col = "actual_duration_min" if "actual_duration_min" in acc_df.columns else "estimated_duration_min"

    net_eur = acc_df.apply(
        lambda r: _net_eur(float(r[fare_col] or 0), float(r[dist_col] or 0)), axis=1
    )
    net_eur_h = acc_df.apply(
        lambda r: _net_eur_h(float(r[fare_col] or 0), float(r[dist_col] or 0), float(r[dur_col] or 1)), axis=1
    )
    km = acc_df[dist_col].fillna(0).astype(float)
    fuel = km.apply(_fuel_cost)
    n_drivers = acc_df["courier_id"].nunique() if "courier_id" in acc_df.columns else 1

    return {
        "strategy": strategy,
        "total_offers": total,
        "accepted": n_accepted,
        "accept_rate": round(n_accepted / total, 4),
        "net_eur_total": round(float(net_eur.sum()), 2),
        "net_eur_h_mean": round(float(net_eur_h.mean()), 2),
        "km_total": round(float(km.sum()), 1),
        "fuel_cost_eur": round(float(fuel.sum()), 2),
        "trips_per_driver": round(n_accepted / max(n_drivers, 1), 1),
    }


def run_backtest(
    parquet_dir: Path,
    out_dir: Path,
    model_path: Path | None = None,
) -> dict:
    df = _load_offers(parquet_dir)
    synthetic = len(df) < 100
    if synthetic:
        print(f"[backtest] Only {len(df)} real offers — using 2000 synthetic offers for demo.")
        df = _generate_synthetic_offers(2000, seed=RANDOM_SEED)

    rng = random.Random(RANDOM_SEED)

    model = None
    if model_path and model_path.exists():
        try:
            import joblib
            model = joblib.load(model_path)
            print(f"[backtest] Loaded model from {model_path}")
        except Exception as e:
            print(f"[backtest] Could not load model: {e}")

    # --- Strategy decisions ---
    n = len(df)
    scores = np.array([_copilot_score(df.iloc[i], model) for i in range(n)])
    net_h = df.apply(
        lambda r: _net_eur_h(
            float(r.get("estimated_fare_eur", 0) or 0),
            float(r.get("estimated_distance_km", 1) or 1),
            float(r.get("estimated_duration_min", 1) or 1),
        ),
        axis=1,
    ).values

    decisions = {
        "copilot": scores >= COPILOT_SCORE_THRESHOLD,
        "greedy_eur_h": net_h >= GREEDY_NET_EUR_H_MIN,
        "random": np.array([rng.random() < RANDOM_ACCEPT_PROB for _ in range(n)]),
        "always_accept": np.ones(n, dtype=bool),
    }

    results = [_compute_kpis(df, mask, strat) for strat, mask in decisions.items()]
    summary = {
        "meta": {
            "synthetic_data": synthetic,
            "n_offers": n,
            "seed": RANDOM_SEED,
            "copilot_threshold": COPILOT_SCORE_THRESHOLD,
            "greedy_net_eur_h_min": GREEDY_NET_EUR_H_MIN,
            "random_accept_prob": RANDOM_ACCEPT_PROB,
        },
        "strategies": results,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "backtest_summary.json"
    csv_path = out_dir / "backtest_summary.csv"

    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    pd.DataFrame(results).to_csv(csv_path, index=False)
    print(f"[backtest] Saved {json_path}")
    print(f"[backtest] Saved {csv_path}")

    # Print comparison table
    copilot_kpi = next(r for r in results if r["strategy"] == "copilot")
    print("\n── Backtest Results ────────────────────────────────────────────────")
    print(f"{'Strategy':<18} {'Accept%':>8} {'Net €/h':>9} {'Net €':>10} {'Km':>8} {'Fuel €':>8}")
    print("─" * 68)
    for r in results:
        marker = " ◄" if r["strategy"] == "copilot" else ""
        print(
            f"{r['strategy']:<18} {r['accept_rate']*100:>7.1f}%"
            f" {r['net_eur_h_mean']:>9.2f} {r['net_eur_total']:>10.2f}"
            f" {r['km_total']:>8.1f} {r['fuel_cost_eur']:>8.2f}{marker}"
        )
    print("─" * 68)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Copilot vs baselines")
    parser.add_argument("--parquet", default="data/parquet_events", help="Path to parquet_events dir")
    parser.add_argument("--out", default="data/reports", help="Output directory for reports")
    parser.add_argument("--model", default="ml/copilot_model.pkl", help="Path to trained model (optional)")
    args = parser.parse_args()

    parquet_dir = _ROOT / args.parquet
    out_dir = _ROOT / args.out
    model_path = _ROOT / args.model

    run_backtest(parquet_dir, out_dir, model_path if model_path.exists() else None)


if __name__ == "__main__":
    main()
