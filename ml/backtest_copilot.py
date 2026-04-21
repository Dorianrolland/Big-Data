"""Backtesting offline: Copilot vs baseline strategies.

Compares four decision strategies on order-offer events:
  - copilot: accept if ML score >= threshold (fallback heuristic if no model)
  - greedy_eur_h: accept if estimated net EUR/h >= target
  - random: accept with fixed probability (seed-stable)
  - always_accept: accept everything

Usage:
  python ml/backtest_copilot.py
  python ml/backtest_copilot.py --parquet data/parquet_events --out data/reports
  python ml/backtest_copilot.py --out data/reports --max-offers 3000
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
if str(_ROOT / "api") not in sys.path:
    sys.path.insert(0, str(_ROOT / "api"))

FUEL_PRICE_EUR_L = float(os.getenv("FUEL_PRICE_EUR_L", "1.85"))
CONSUMPTION_L_100KM = float(os.getenv("CONSUMPTION_L_100KM", "7.5"))
PLATFORM_FEE_PCT = float(os.getenv("PLATFORM_FEE_PCT", "25.0"))
COPILOT_SCORE_THRESHOLD = float(os.getenv("COPILOT_SCORE_THRESHOLD", "0.55"))
GREEDY_NET_EUR_H_MIN = float(os.getenv("GREEDY_NET_EUR_H_MIN", "12.0"))
RANDOM_ACCEPT_PROB = float(os.getenv("RANDOM_ACCEPT_PROB", "0.6"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
BACKTEST_MAX_OFFERS_DEFAULT = int(os.getenv("BACKTEST_MAX_OFFERS", "5000"))
BACKTEST_REQUIRED_COLUMNS = (
    "estimated_fare_eur",
    "estimated_distance_km",
    "estimated_duration_min",
)
BACKTEST_OPTIONAL_COLUMNS = (
    "offer_id",
    "courier_id",
    "zone_id",
    "actual_fare_eur",
    "actual_distance_km",
    "actual_duration_min",
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
)


def _safe_print(text: str) -> None:
    encoding = sys.stdout.encoding or "utf-8"
    print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def _duck_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _coerce_max_offers(value: int | None) -> int | None:
    if value is None:
        return None
    if value <= 0:
        return None
    return int(value)


def _topic_columns(con: duckdb.DuckDBPyConnection, glob: str) -> set[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 0"
    ).fetchall()
    return {str(name) for name, *_ in rows}


def _to_float_array(values: pd.Series | np.ndarray | float | int, default: float = 0.0) -> np.ndarray:
    if isinstance(values, pd.Series):
        return pd.to_numeric(values, errors="coerce").fillna(default).astype(float).to_numpy()
    arr = np.asarray(values, dtype=float)
    if arr.ndim == 0:
        return np.array([float(arr)])
    return arr


def _fuel_cost(distance_km: float | np.ndarray) -> float | np.ndarray:
    dist = np.asarray(distance_km, dtype=float)
    return dist / 100.0 * CONSUMPTION_L_100KM * FUEL_PRICE_EUR_L


def _net_eur(fare: float | np.ndarray, distance_km: float | np.ndarray) -> float | np.ndarray:
    gross = np.asarray(fare, dtype=float) * (1.0 - PLATFORM_FEE_PCT / 100.0)
    return gross - _fuel_cost(distance_km)


def _net_eur_h(
    fare: float | np.ndarray,
    distance_km: float | np.ndarray,
    duration_min: float | np.ndarray,
) -> float | np.ndarray:
    """Compute net EUR/h for scalar or vector inputs."""
    dur = np.asarray(duration_min, dtype=float)
    dur = np.where(dur <= 0.0, 1.0, dur)
    return _net_eur(fare, distance_km) / (dur / 60.0)


def _copilot_score(row: pd.Series, model: object | None) -> float:
    """Return ML probability or fallback heuristic score for one row."""
    if model is not None:
        try:
            from copilot_logic import FEATURE_COLUMNS

            feats = []
            for col in FEATURE_COLUMNS:
                feats.append(float(row.get(col, 0.0) or 0.0))
            return float(model.predict_proba([feats])[0][1])
        except Exception:
            pass

    net_h_col = row.get("estimated_net_eur_h")
    if net_h_col is not None and float(net_h_col) != 0.0:
        net_h = float(net_h_col)
    else:
        net_h = float(
            _net_eur_h(
                float(row.get("estimated_fare_eur", 0.0) or 0.0),
                float(row.get("estimated_distance_km", 1.0) or 1.0),
                float(row.get("estimated_duration_min", 1.0) or 1.0),
            )
        )
    demand = float(row.get("demand_index", 0.5) or 0.5)
    supply = float(row.get("supply_index", 0.5) or 0.5)
    pressure = demand / max(supply, 0.01)
    raw = (net_h / 20.0) * 0.55 + min(pressure / 3.0, 1.0) * 0.30 + 0.15
    return float(min(max(raw, 0.0), 1.0))


def _copilot_scores(df: pd.DataFrame, model: object | None) -> np.ndarray:
    n = len(df)
    if n == 0:
        return np.array([], dtype=float)

    if model is not None:
        try:
            from copilot_logic import FEATURE_COLUMNS

            matrix = []
            for col in FEATURE_COLUMNS:
                if col in df.columns:
                    matrix.append(pd.to_numeric(df[col], errors="coerce").fillna(0.0).to_numpy(dtype=float))
                else:
                    matrix.append(np.zeros(n, dtype=float))
            feats = np.column_stack(matrix)
            return model.predict_proba(feats)[:, 1].astype(float)
        except Exception:
            pass

    if "estimated_net_eur_h" in df.columns:
        net_h = pd.to_numeric(df["estimated_net_eur_h"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    else:
        fare = _to_float_array(df.get("estimated_fare_eur", 0.0), 0.0)
        dist = _to_float_array(df.get("estimated_distance_km", 1.0), 1.0)
        dur = _to_float_array(df.get("estimated_duration_min", 1.0), 1.0)
        net_h = np.asarray(_net_eur_h(fare, dist, dur), dtype=float)

    demand = _to_float_array(df.get("demand_index", 0.5), 0.5)
    supply = _to_float_array(df.get("supply_index", 0.5), 0.5)
    pressure = demand / np.maximum(supply, 0.01)
    raw = (net_h / 20.0) * 0.55 + np.minimum(pressure / 3.0, 1.0) * 0.30 + 0.15
    return np.clip(raw, 0.0, 1.0)


def _load_offers(parquet_dir: Path, max_offers: int | None) -> pd.DataFrame:
    offers_path = parquet_dir / "topic=order-offers-v1"
    if not offers_path.exists():
        return pd.DataFrame()

    glob = _duck_path(offers_path / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        cols = _topic_columns(con, glob)
        missing = [col for col in BACKTEST_REQUIRED_COLUMNS if col not in cols]
        if missing:
            _safe_print(
                f"[backtest] Missing required columns in order-offers-v1: {missing}. "
                "Falling back to synthetic offers."
            )
            return pd.DataFrame()

        selected_cols = list(BACKTEST_REQUIRED_COLUMNS) + [
            col for col in BACKTEST_OPTIONAL_COLUMNS if col in cols and col not in BACKTEST_REQUIRED_COLUMNS
        ]
        select_sql = ", ".join(selected_cols)
        limit_sql = f" LIMIT {int(max_offers)}" if max_offers is not None else ""
        query = (
            f"SELECT {select_sql} "
            f"FROM read_parquet('{glob}', hive_partitioning=true)"
            f"{limit_sql}"
        )
        return con.execute(query).fetchdf()
    finally:
        con.close()


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

    def _pick_actual_or_estimated(actual_col: str, est_col: str) -> str:
        if actual_col in acc_df.columns:
            actual_series = pd.to_numeric(acc_df[actual_col], errors="coerce")
            if actual_series.notna().any():
                return actual_col
        return est_col

    fare_col = _pick_actual_or_estimated("actual_fare_eur", "estimated_fare_eur")
    dist_col = _pick_actual_or_estimated("actual_distance_km", "estimated_distance_km")
    dur_col = _pick_actual_or_estimated("actual_duration_min", "estimated_duration_min")

    fare = _to_float_array(acc_df[fare_col], 0.0)
    dist = _to_float_array(acc_df[dist_col], 0.0)
    dur = _to_float_array(acc_df[dur_col], 1.0)

    net_eur = np.asarray(_net_eur(fare, dist), dtype=float)
    net_eur_h = np.asarray(_net_eur_h(fare, dist, dur), dtype=float)
    fuel = np.asarray(_fuel_cost(dist), dtype=float)
    n_drivers = acc_df["courier_id"].nunique() if "courier_id" in acc_df.columns else 1

    return {
        "strategy": strategy,
        "total_offers": total,
        "accepted": n_accepted,
        "accept_rate": round(n_accepted / max(total, 1), 4),
        "net_eur_total": round(float(net_eur.sum()), 2),
        "net_eur_h_mean": round(float(net_eur_h.mean()), 2),
        "km_total": round(float(dist.sum()), 1),
        "fuel_cost_eur": round(float(fuel.sum()), 2),
        "trips_per_driver": round(n_accepted / max(n_drivers, 1), 1),
    }


def run_backtest(
    parquet_dir: Path,
    out_dir: Path,
    model_path: Path | None = None,
    _override_df: pd.DataFrame | None = None,
    max_offers: int | None = BACKTEST_MAX_OFFERS_DEFAULT,
) -> dict:
    max_offers = _coerce_max_offers(max_offers)
    synthetic = False

    if _override_df is not None:
        df = _override_df.copy()
    else:
        df = _load_offers(parquet_dir, max_offers=max_offers)
        if len(df) < 100:
            synthetic = True
            synthetic_n = max(2000, max_offers or 2000)
            _safe_print(f"[backtest] Only {len(df)} real offers - using {synthetic_n} synthetic offers.")
            df = _generate_synthetic_offers(synthetic_n, seed=RANDOM_SEED)

    if max_offers is not None and len(df) > max_offers:
        df = df.head(max_offers).copy()
    if max_offers is not None:
        _safe_print(f"[backtest] Processing up to {max_offers} offers (demo speed mode).")

    rng = random.Random(RANDOM_SEED)
    model = None
    if model_path and model_path.exists():
        try:
            import joblib

            model = joblib.load(model_path)
            _safe_print(f"[backtest] Loaded model from {model_path}")
        except Exception as exc:
            _safe_print(f"[backtest] Could not load model: {exc}")

    n = len(df)
    scores = _copilot_scores(df, model)
    if scores.shape[0] != n:
        scores = np.array([_copilot_score(df.iloc[i], model) for i in range(n)], dtype=float)

    fare_est = _to_float_array(df.get("estimated_fare_eur", 0.0), 0.0)
    dist_est = _to_float_array(df.get("estimated_distance_km", 1.0), 1.0)
    dur_est = _to_float_array(df.get("estimated_duration_min", 1.0), 1.0)
    net_h = np.asarray(_net_eur_h(fare_est, dist_est, dur_est), dtype=float)

    decisions = {
        "copilot": scores >= COPILOT_SCORE_THRESHOLD,
        "greedy_eur_h": net_h >= GREEDY_NET_EUR_H_MIN,
        "random": np.fromiter((rng.random() < RANDOM_ACCEPT_PROB for _ in range(n)), dtype=bool, count=n),
        "always_accept": np.ones(n, dtype=bool),
    }

    results = [_compute_kpis(df, mask, strategy) for strategy, mask in decisions.items()]
    summary = {
        "meta": {
            "synthetic_data": synthetic,
            "n_offers": n,
            "seed": RANDOM_SEED,
            "copilot_threshold": COPILOT_SCORE_THRESHOLD,
            "greedy_net_eur_h_min": GREEDY_NET_EUR_H_MIN,
            "random_accept_prob": RANDOM_ACCEPT_PROB,
            "max_offers": max_offers,
        },
        "strategies": results,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "backtest_summary.json"
    csv_path = out_dir / "backtest_summary.csv"

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=True)

    pd.DataFrame(results).to_csv(csv_path, index=False, encoding="utf-8")
    _safe_print(f"[backtest] Saved {json_path}")
    _safe_print(f"[backtest] Saved {csv_path}")

    _safe_print("")
    _safe_print("-- Backtest Results ----------------------------------------------------")
    _safe_print(f"{'Strategy':<18} {'Accept%':>8} {'Net EUR/h':>10} {'Net EUR':>10} {'Km':>8} {'Fuel EUR':>10}")
    _safe_print("-" * 72)
    for row in results:
        marker = " <-" if row["strategy"] == "copilot" else ""
        _safe_print(
            f"{row['strategy']:<18} {row['accept_rate']*100:>7.1f}%"
            f" {row['net_eur_h_mean']:>10.2f} {row['net_eur_total']:>10.2f}"
            f" {row['km_total']:>8.1f} {row['fuel_cost_eur']:>10.2f}{marker}"
        )
    _safe_print("-" * 72)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest Copilot vs baselines")
    parser.add_argument("--parquet", default="data/parquet_events", help="Path to parquet_events directory")
    parser.add_argument("--out", default="data/reports", help="Output directory for reports")
    parser.add_argument("--model", default="ml/copilot_model.pkl", help="Path to trained model (optional)")
    parser.add_argument(
        "--max-offers",
        type=int,
        default=BACKTEST_MAX_OFFERS_DEFAULT,
        help="Max offers to process (0 = all offers). Default comes from BACKTEST_MAX_OFFERS or 5000.",
    )
    args = parser.parse_args()

    parquet_dir = _ROOT / args.parquet
    out_dir = _ROOT / args.out
    model_path = _ROOT / args.model

    run_backtest(
        parquet_dir,
        out_dir,
        model_path if model_path.exists() else None,
        max_offers=args.max_offers,
    )


if __name__ == "__main__":
    main()
