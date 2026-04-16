"""Train local copilot acceptance model from parquet events.

Supports a fixed-window training split driven by two environment flags
(also exposed as CLI args):

- ``--train-start YYYY-MM`` / ``--train-months N`` defines a closed window
  ``[train_start, train_start + N months)`` that is the ONLY time range used
  to fit the model. Events outside this window are ignored.
- When the window is unchanged across runs and the target ``--out`` already
  exists with matching metadata, training is skipped and the cached model is
  reused as-is (no retrain on restart, consistent with the "reset runtime,
  keep model" policy).
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

FEATURES = [
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


def expected_calibration_error(y_true: pd.Series, proba: pd.Series, bins: int = 10) -> float:
    frame = pd.DataFrame({"y": y_true.astype(float), "p": proba.astype(float)})
    frame["bin"] = pd.cut(frame["p"], bins=bins, labels=False, include_lowest=True)

    total = max(len(frame), 1)
    ece = 0.0
    for b in range(bins):
        bucket = frame[frame["bin"] == b]
        if bucket.empty:
            continue
        confidence = bucket["p"].mean()
        accuracy = bucket["y"].mean()
        ece += (len(bucket) / total) * abs(confidence - accuracy)
    return float(ece)


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + delta
    return idx // 12, (idx % 12) + 1


def compute_train_window(
    start_month: str | None,
    month_count: int,
) -> tuple[datetime, datetime] | None:
    """Return a (start_utc, end_utc) half-open window or None if not set."""
    if not start_month or month_count <= 0:
        return None
    try:
        y, m = start_month.split("-", 1)
        yi, mi = int(y), int(m)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid train-start '{start_month}' (expected YYYY-MM)") from exc
    start = datetime(yi, mi, 1, tzinfo=timezone.utc)
    end_y, end_m = _add_months(yi, mi, month_count)
    end = datetime(end_y, end_m, 1, tzinfo=timezone.utc)
    return start, end


def build_training_frame(
    events_root: Path,
    revenue_threshold_eur_h: float,
    context_window_minutes: int,
    positive_quantile: float,
    train_window: tuple[datetime, datetime] | None = None,
) -> pd.DataFrame:
    conn = duckdb.connect(":memory:")
    glob = str(events_root / "**" / "*.parquet")
    window_seconds = int(max(1, context_window_minutes) * 60)

    window_clause = ""
    if train_window is not None:
        start_iso = train_window[0].isoformat()
        end_iso = train_window[1].isoformat()
        window_clause = (
            f" AND CAST(ts AS TIMESTAMPTZ) >= TIMESTAMPTZ '{start_iso}'"
            f" AND CAST(ts AS TIMESTAMPTZ) <  TIMESTAMPTZ '{end_iso}'"
        )

    query = f"""
    WITH offers AS (
        SELECT
            offer_id,
            courier_id,
            zone_id,
            CAST(ts AS TIMESTAMPTZ) AS ts_offer,
            estimated_fare_eur,
            estimated_distance_km,
            estimated_duration_min,
            COALESCE(demand_index, 1.0) AS demand_index,
            COALESCE(weather_factor, 1.0) AS weather_factor,
            COALESCE(traffic_factor, 1.0) AS traffic_factor
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE event_type = 'order.offer.v1'
          AND offer_id IS NOT NULL
          AND offer_id <> ''
          {window_clause}
    ),
    order_outcomes AS (
        SELECT
            offer_id,
            MAX(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted_flag,
            MAX(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected_flag,
            MAX(CASE WHEN status = 'dropped_off' THEN 1 ELSE 0 END) AS dropped_off_flag,
            MAX(NULLIF(actual_fare_eur, 0)) AS actual_fare_eur
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE event_type = 'order.event.v1'
          AND offer_id IS NOT NULL
          AND offer_id <> ''
          {window_clause}
    ),
    context_signals AS (
        SELECT
            zone_id,
            CAST(ts AS TIMESTAMPTZ) AS ts_ctx,
            COALESCE(supply_index, 1.0) AS supply_index
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE event_type = 'context.signal.v1'
          AND zone_id IS NOT NULL
          AND zone_id <> ''
          AND supply_index IS NOT NULL
    )
    SELECT
        o.offer_id,
        o.courier_id,
        o.zone_id,
        o.ts_offer AS ts,
        o.estimated_fare_eur,
        o.estimated_distance_km,
        o.estimated_duration_min,
        o.demand_index,
        COALESCE((
            SELECT c.supply_index
            FROM context_signals c
            WHERE c.zone_id = o.zone_id
              AND ABS(epoch(c.ts_ctx) - epoch(o.ts_offer)) <= {window_seconds}
            ORDER BY ABS(epoch(c.ts_ctx) - epoch(o.ts_offer))
            LIMIT 1
        ), 1.0) AS supply_index,
        o.weather_factor,
        o.traffic_factor,
        COALESCE(out.accepted_flag, 0) AS accepted_flag,
        COALESCE(out.rejected_flag, 0) AS rejected_flag,
        COALESCE(out.dropped_off_flag, 0) AS dropped_off_flag,
        out.actual_fare_eur
    FROM offers o
    LEFT JOIN order_outcomes out ON out.offer_id = o.offer_id
    WHERE o.estimated_fare_eur IS NOT NULL
      AND o.estimated_distance_km IS NOT NULL
      AND o.estimated_duration_min IS NOT NULL
    """
    df = conn.execute(query).df()
    if df.empty:
        return df

    numeric_defaults = {
        "estimated_fare_eur": 0.0,
        "estimated_distance_km": 0.0,
        "estimated_duration_min": 1.0,
        "demand_index": 1.0,
        "supply_index": 1.0,
        "weather_factor": 1.0,
        "traffic_factor": 1.0,
        "actual_fare_eur": None,
    }
    for col, default in numeric_defaults.items():
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if default is not None:
            df[col] = df[col].fillna(default)

    df["estimated_duration_min"] = df["estimated_duration_min"].clip(lower=1.0)
    df["supply_index"] = df["supply_index"].clip(lower=0.2)

    variable_cost = df["estimated_distance_km"] * 0.35
    df["estimated_net_eur_h"] = ((df["estimated_fare_eur"] - variable_cost) / df["estimated_duration_min"]) * 60.0
    df["realized_net_eur_h"] = (
        (df["actual_fare_eur"] - variable_cost) / df["estimated_duration_min"]
    ) * 60.0
    df["pressure_ratio"] = df["demand_index"] / df["supply_index"]

    target_net_eur_h = df["realized_net_eur_h"].fillna(df["estimated_net_eur_h"])
    positive_quantile = min(max(float(positive_quantile), 0.05), 0.95)
    quantile_threshold = float(target_net_eur_h.quantile(positive_quantile))
    effective_threshold = max(float(revenue_threshold_eur_h), quantile_threshold)

    base_label = (target_net_eur_h >= effective_threshold).astype(int)
    label = base_label.copy()
    label_source = pd.Series("fallback_proxy", index=df.index)

    rejected_mask = df["rejected_flag"] == 1
    label.loc[rejected_mask] = 0
    label_source.loc[rejected_mask] = "observed_rejected"

    realized_mask = (~rejected_mask) & (df["dropped_off_flag"] == 1) & df["realized_net_eur_h"].notna()
    label.loc[realized_mask] = (df.loc[realized_mask, "realized_net_eur_h"] >= effective_threshold).astype(int)
    label_source.loc[realized_mask] = "observed_realized"

    accepted_proxy_mask = (~rejected_mask) & (~realized_mask) & (df["accepted_flag"] == 1)
    label_source.loc[accepted_proxy_mask] = "accepted_proxy"

    df["label"] = label.astype(int)
    df["label_source"] = label_source
    df["label_threshold_eur_h"] = effective_threshold

    return df


def train(df: pd.DataFrame, random_state: int = 42) -> tuple[Pipeline, dict[str, Any]]:
    frame = df.dropna(subset=FEATURES + ["label"]).copy()
    X = frame[FEATURES].astype(float)
    y = frame["label"].astype(int)

    if len(frame) < 20:
        raise ValueError("not enough rows to train a model")

    if y.nunique() < 2:
        # Last-resort balancing so that logistic regression can still fit.
        pivot = float(frame["estimated_net_eur_h"].median())
        y = (frame["estimated_net_eur_h"] >= pivot).astype(int)
        if y.nunique() < 2:
            raise ValueError("unable to create a two-class target")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=random_state,
        stratify=y,
    )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=600, solver="lbfgs", class_weight="balanced")),
        ]
    )
    model.fit(X_train, y_train)

    proba = pd.Series(model.predict_proba(X_test)[:, 1])
    pred = (proba >= 0.5).astype(int)

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, proba)),
        "average_precision": float(average_precision_score(y_test, proba)),
        "precision_at_0_5": float(precision_score(y_test, pred, zero_division=0)),
        "recall_at_0_5": float(recall_score(y_test, pred, zero_division=0)),
        "f1_at_0_5": float(f1_score(y_test, pred, zero_division=0)),
        "brier_score": float(brier_score_loss(y_test, proba)),
        "ece_10_bins": float(expected_calibration_error(y_test, proba, bins=10)),
        "test_rows": int(len(y_test)),
        "positive_rate_test": float(y_test.mean()),
    }
    return model, metrics


def _train_window_signature(window: tuple[datetime, datetime] | None) -> str:
    if window is None:
        return ""
    return f"{window[0].isoformat()}::{window[1].isoformat()}"


def _cached_model_is_current(
    out_path: Path,
    window_signature: str,
    force: bool,
) -> bool:
    if force:
        return False
    if not out_path.exists():
        return False
    meta_path = out_path.with_suffix(".json")
    if not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return str(meta.get("train_window_signature", "")) == window_signature and bool(window_signature)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train copilot model")
    parser.add_argument("--data", default="./data/parquet_events", help="Path to copilot events parquet root")
    parser.add_argument("--out", default="./data/models/copilot_model.joblib", help="Output model path")
    parser.add_argument("--min-rows", type=int, default=100, help="Minimum rows required before training")
    parser.add_argument(
        "--revenue-threshold-eur-h",
        type=float,
        default=18.0,
        help="Label threshold for net EUR/hour",
    )
    parser.add_argument(
        "--context-window-minutes",
        type=int,
        default=20,
        help="Nearest context.signal search window around each offer",
    )
    parser.add_argument(
        "--positive-quantile",
        type=float,
        default=0.60,
        help="Revenue quantile used to shape positive labels (0.05 - 0.95)",
    )
    parser.add_argument(
        "--train-start",
        default=os.getenv("TLC_MONTH") or None,
        help="First month of the training window (YYYY-MM). Defaults to TLC_MONTH.",
    )
    parser.add_argument(
        "--train-months",
        type=int,
        default=int(os.getenv("TLC_TRAIN_MONTH_COUNT", "0") or "0"),
        help="Number of consecutive months included in the training window. "
        "0 disables windowing (all parquet events are used).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Retrain even if the cached model already matches the window signature.",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        train_window = compute_train_window(args.train_start, args.train_months)
    except ValueError as exc:
        raise SystemExit(str(exc))

    window_sig = _train_window_signature(train_window)
    if _cached_model_is_current(out_path, window_sig, args.force):
        print(f"cached model up-to-date for window={window_sig} -> skipping retrain")
        return

    df = build_training_frame(
        events_root=data_path,
        revenue_threshold_eur_h=args.revenue_threshold_eur_h,
        context_window_minutes=args.context_window_minutes,
        positive_quantile=args.positive_quantile,
        train_window=train_window,
    )
    if df.empty:
        raise SystemExit(f"No training rows found in {data_path}")
    if len(df) < args.min_rows:
        raise SystemExit(f"Not enough rows to train: {len(df)} < min_rows={args.min_rows}")

    model, metrics = train(df)
    label_source_counts = {str(k): int(v) for k, v in df["label_source"].value_counts().to_dict().items()}

    payload = {
        "model": model,
        "feature_columns": FEATURES,
        "trained_rows": int(len(df)),
        "positive_rate": float(df["label"].mean()),
        "label_threshold_eur_h": float(df["label_threshold_eur_h"].iloc[0]),
        "base_threshold_eur_h": float(args.revenue_threshold_eur_h),
        "positive_quantile": float(args.positive_quantile),
        "context_window_minutes": int(args.context_window_minutes),
        "label_source_counts": label_source_counts,
        "metrics": metrics,
        "model_version": "copilot_v2",
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "train_window_start": train_window[0].isoformat() if train_window else None,
        "train_window_end": train_window[1].isoformat() if train_window else None,
        "train_window_signature": window_sig,
    }
    joblib.dump(payload, out_path)

    meta_path = out_path.with_suffix(".json")
    meta_path.write_text(
        json.dumps(
            {
                "feature_columns": FEATURES,
                "trained_rows": int(len(df)),
                "positive_rate": round(float(df["label"].mean()), 4),
                "label_threshold_eur_h": float(df["label_threshold_eur_h"].iloc[0]),
                "base_threshold_eur_h": float(args.revenue_threshold_eur_h),
                "positive_quantile": float(args.positive_quantile),
                "context_window_minutes": int(args.context_window_minutes),
                "label_source_counts": label_source_counts,
                "metrics": {k: round(float(v), 6) if isinstance(v, (float, int)) else v for k, v in metrics.items()},
                "model_version": "copilot_v2",
                "trained_at_utc": payload["trained_at_utc"],
                "train_window_start": payload["train_window_start"],
                "train_window_end": payload["train_window_end"],
                "train_window_signature": window_sig,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"model saved to {out_path}")
    print(f"metadata saved to {meta_path}")
    print(
        f"trained_rows={len(df)} positive_rate={df['label'].mean():.4f} "
        f"label_threshold={float(df['label_threshold_eur_h'].iloc[0]):.2f}"
    )
    print(
        f"roc_auc={metrics['roc_auc']:.4f} average_precision={metrics['average_precision']:.4f} "
        f"ece_10_bins={metrics['ece_10_bins']:.4f}"
    )


if __name__ == "__main__":
    main()
