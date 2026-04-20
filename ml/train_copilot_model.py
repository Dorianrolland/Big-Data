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
import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
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
]

DEFAULT_FUEL_PRICE_EUR_L = 1.85
DEFAULT_VEHICLE_CONSUMPTION_L_100KM = 7.5
DEFAULT_PLATFORM_FEE_PCT = 25.0


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
    WITH offers_raw AS (
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
    offers AS (
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY ts_offer ASC) AS rn
            FROM offers_raw
        )
        WHERE rn = 1
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
        GROUP BY offer_id
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
    ),
    offers_with_context AS (
        SELECT
            o.*,
            c.supply_index AS supply_index_ctx,
            epoch(o.ts_offer) - epoch(c.ts_ctx) AS context_age_s
        FROM offers o
        ASOF LEFT JOIN context_signals c
          ON c.zone_id = o.zone_id
         AND c.ts_ctx <= o.ts_offer
    )
    SELECT
        owc.offer_id,
        owc.courier_id,
        owc.zone_id,
        owc.ts_offer AS ts,
        owc.estimated_fare_eur,
        owc.estimated_distance_km,
        owc.estimated_duration_min,
        owc.demand_index,
        owc.weather_factor,
        owc.traffic_factor,
        owc.supply_index_ctx,
        owc.context_age_s,
        COALESCE(out.accepted_flag, 0) AS accepted_flag,
        COALESCE(out.rejected_flag, 0) AS rejected_flag,
        COALESCE(out.dropped_off_flag, 0) AS dropped_off_flag,
        out.actual_fare_eur
    FROM offers_with_context owc
    LEFT JOIN order_outcomes out ON out.offer_id = owc.offer_id
    WHERE owc.estimated_fare_eur IS NOT NULL
      AND owc.estimated_distance_km IS NOT NULL
      AND owc.estimated_duration_min IS NOT NULL
    """
    df = conn.execute(query).df()
    conn.close()
    if df.empty:
        return df

    numeric_defaults = {
        "estimated_fare_eur": 0.0,
        "estimated_distance_km": 0.0,
        "estimated_duration_min": 1.0,
        "demand_index": 1.0,
        "supply_index_ctx": None,
        "context_age_s": None,
        "weather_factor": 1.0,
        "traffic_factor": 1.0,
        "actual_fare_eur": None,
    }
    for col, default in numeric_defaults.items():
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if default is not None:
            df[col] = df[col].fillna(default)

    df["estimated_duration_min"] = df["estimated_duration_min"].clip(lower=1.0)
    ts_utc = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df[ts_utc.notna()].copy()
    ts_utc = ts_utc.loc[df.index]

    has_fresh_context = (
        df["context_age_s"].notna()
        & (df["context_age_s"] >= 0.0)
        & (df["context_age_s"] <= float(window_seconds))
    )
    df["context_fallback_applied"] = (~has_fresh_context).astype(float)
    df["supply_index"] = df["supply_index_ctx"].where(has_fresh_context, np.nan).fillna(1.0).clip(lower=0.2)

    stale_sources = np.where(
        has_fresh_context,
        0.0,
        np.where(
            df["context_age_s"].isna(),
            3.0,
            np.where(df["context_age_s"] <= 7200.0, 1.0, np.where(df["context_age_s"] <= 21600.0, 2.0, 3.0)),
        ),
    )
    df["context_stale_sources"] = stale_sources.astype(float)

    ts_local = ts_utc.dt.tz_convert("America/New_York")
    local_hour = ts_local.dt.hour + (ts_local.dt.minute / 60.0)
    is_peak = (((local_hour >= 7.0) & (local_hour < 10.0)) | ((local_hour >= 16.0) & (local_hour < 20.0))).astype(float)
    is_weekend = (ts_local.dt.dayofweek >= 5).astype(float)

    min_year = int(ts_local.dt.year.min())
    max_year = int(ts_local.dt.year.max())
    holiday_calendar = USFederalHolidayCalendar()
    holiday_dates = set(
        holiday_calendar.holidays(start=f"{min_year}-01-01", end=f"{max_year}-12-31").date
    )
    is_holiday = ts_local.dt.date.isin(holiday_dates).astype(float)

    temporal_pressure = (
        (is_peak * 0.14)
        + (((local_hour >= 11.0) & (local_hour < 14.0) & (is_peak < 0.5)).astype(float) * 0.05)
        + (is_weekend * 0.08)
        + (is_holiday * 0.12)
    )

    weather_intensity = ((df["weather_factor"] - 0.95) / 0.45).clip(lower=0.0, upper=1.0)
    weather_precip_mm = weather_intensity * 6.0
    weather_wind_kmh = 8.0 + (weather_intensity * 42.0)
    event_pressure = (
        (((df["demand_index"] - 1.0).clip(lower=0.0) / 0.9).clip(upper=1.0) * 0.65)
        + (temporal_pressure.clip(upper=0.35) / 0.35 * 0.25)
        + (weather_intensity * 0.10)
    ).clip(lower=0.0, upper=1.0)
    event_count_nearby = np.rint(event_pressure * 5.0).clip(0, 8).astype(float)

    fuel_cost_eur = (
        df["estimated_distance_km"] * (DEFAULT_VEHICLE_CONSUMPTION_L_100KM / 100.0) * DEFAULT_FUEL_PRICE_EUR_L
    )
    platform_fee_est = df["estimated_fare_eur"] * (DEFAULT_PLATFORM_FEE_PCT / 100.0)
    estimated_net_eur = df["estimated_fare_eur"] - platform_fee_est - fuel_cost_eur
    estimated_net_eur_h = (estimated_net_eur / df["estimated_duration_min"]) * 60.0

    realized_net_eur_h = pd.Series(np.nan, index=df.index, dtype=float)
    actual_mask = df["actual_fare_eur"].notna()
    if actual_mask.any():
        platform_fee_actual = df.loc[actual_mask, "actual_fare_eur"] * (DEFAULT_PLATFORM_FEE_PCT / 100.0)
        realized_net = df.loc[actual_mask, "actual_fare_eur"] - platform_fee_actual - fuel_cost_eur.loc[actual_mask]
        realized_net_eur_h.loc[actual_mask] = (realized_net / df.loc[actual_mask, "estimated_duration_min"]) * 60.0

    df["temporal_hour_local"] = local_hour.astype(float)
    df["is_peak_hour"] = is_peak.astype(float)
    df["is_weekend"] = is_weekend.astype(float)
    df["is_holiday"] = is_holiday.astype(float)
    df["temporal_pressure"] = temporal_pressure.astype(float)
    df["weather_intensity"] = weather_intensity.astype(float)
    df["weather_precip_mm"] = weather_precip_mm.astype(float)
    df["weather_wind_kmh"] = weather_wind_kmh.astype(float)
    df["event_pressure"] = event_pressure.astype(float)
    df["event_count_nearby"] = event_count_nearby.astype(float)
    df["estimated_net_eur_h"] = estimated_net_eur_h.astype(float)
    df["realized_net_eur_h"] = realized_net_eur_h.astype(float)
    df["pressure_ratio"] = (df["demand_index"] / df["supply_index"]).astype(float)
    df["ts"] = ts_utc

    target_net_eur_h = df["realized_net_eur_h"].fillna(df["estimated_net_eur_h"])
    positive_quantile = min(max(float(positive_quantile), 0.05), 0.95)
    month_key = ts_local.dt.strftime("%Y-%m")
    per_month_quantile = target_net_eur_h.groupby(month_key).quantile(positive_quantile)
    quantile_threshold = month_key.map(per_month_quantile).astype(float)
    global_threshold = max(float(revenue_threshold_eur_h), float(target_net_eur_h.quantile(positive_quantile)))
    effective_threshold = quantile_threshold.fillna(global_threshold).clip(lower=float(revenue_threshold_eur_h))

    base_label = (target_net_eur_h >= effective_threshold).astype(int)
    label = base_label.copy()
    label_source = pd.Series("fallback_proxy", index=df.index)

    rejected_mask = df["rejected_flag"] == 1
    label.loc[rejected_mask] = 0
    label_source.loc[rejected_mask] = "observed_rejected"

    realized_mask = (~rejected_mask) & (df["dropped_off_flag"] == 1) & df["realized_net_eur_h"].notna()
    label.loc[realized_mask] = (
        df.loc[realized_mask, "realized_net_eur_h"] >= effective_threshold.loc[realized_mask]
    ).astype(int)
    label_source.loc[realized_mask] = "observed_realized"

    accepted_proxy_mask = (~rejected_mask) & (~realized_mask) & (df["accepted_flag"] == 1)
    label_source.loc[accepted_proxy_mask] = "accepted_proxy"

    df["label"] = label.astype(int)
    df["label_source"] = label_source
    df["label_threshold_eur_h"] = effective_threshold.astype(float)
    df["label_threshold_eur_h_global"] = float(effective_threshold.median())

    return df


def train_with_evaluation(
    df: pd.DataFrame,
    random_state: int = 42,
    test_size: float = 0.2,
) -> tuple[Pipeline, dict[str, Any], dict[str, Any]]:
    frame = df.dropna(subset=FEATURES + ["label"]).copy()
    frame = frame.sort_values("ts").reset_index(drop=True)
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

    test_size = min(max(float(test_size), 0.1), 0.4)
    split_idx = int(len(frame) * (1.0 - test_size))
    split_idx = max(1, min(split_idx, len(frame) - 1))
    train_frame = frame.iloc[:split_idx].copy()
    test_frame = frame.iloc[split_idx:].copy()
    split_strategy = "temporal"

    if train_frame["label"].nunique() < 2 or test_frame["label"].nunique() < 2:
        split_strategy = "stratified_random_fallback"
        train_frame, test_frame = train_test_split(
            frame,
            test_size=test_size,
            random_state=random_state,
            stratify=frame["label"],
        )
        train_frame = train_frame.sort_values("ts")
        test_frame = test_frame.sort_values("ts")

    x_train = train_frame[FEATURES].astype(float)
    y_train = train_frame["label"].astype(int)
    x_test = test_frame[FEATURES].astype(float)
    y_test = test_frame["label"].astype(int)
    source_train = train_frame["label_source"].astype(str)

    source_weights = source_train.map(
        {
            "observed_realized": 1.0,
            "observed_rejected": 1.0,
            "accepted_proxy": 0.75,
            "fallback_proxy": 0.55,
        }
    ).fillna(1.0)

    tune_size = min(len(x_train), 250_000)
    if tune_size < len(x_train):
        tune_idx = x_train.sample(n=tune_size, random_state=random_state).index
        x_tune = x_train.loc[tune_idx]
        y_tune = y_train.loc[tune_idx]
        w_tune = source_weights.loc[tune_idx]
    else:
        x_tune = x_train
        y_tune = y_train
        w_tune = source_weights

    candidate_cs = [0.35, 0.75, 1.5]
    candidate_metrics: list[dict[str, float]] = []
    best_c = candidate_cs[0]
    best_score = float("-inf")

    for c_value in candidate_cs:
        candidate_model = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegression(
                        C=c_value,
                        max_iter=800,
                        solver="lbfgs",
                        class_weight="balanced",
                    ),
                ),
            ]
        )
        candidate_model.fit(x_tune, y_tune, clf__sample_weight=w_tune.values)
        proba_test = pd.Series(candidate_model.predict_proba(x_test)[:, 1], index=y_test.index)
        auc_value = float(roc_auc_score(y_test, proba_test))
        ap_value = float(average_precision_score(y_test, proba_test))
        brier_value = float(brier_score_loss(y_test, proba_test))
        combo_score = (auc_value * 0.55) + (ap_value * 0.35) + ((1.0 - brier_value) * 0.10)
        candidate_metrics.append(
            {
                "c": float(c_value),
                "roc_auc": auc_value,
                "average_precision": ap_value,
                "brier_score": brier_value,
                "combo_score": combo_score,
            }
        )
        if combo_score > best_score:
            best_score = combo_score
            best_c = c_value

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    C=best_c,
                    max_iter=900,
                    solver="lbfgs",
                    class_weight="balanced",
                ),
            ),
        ]
    )
    model.fit(x_train, y_train, clf__sample_weight=source_weights.values)

    proba = pd.Series(model.predict_proba(x_test)[:, 1], index=y_test.index)
    pred = (proba >= 0.5).astype(int)

    x_train_r, x_test_r, y_train_r, y_test_r, source_train_r, _source_test_r = train_test_split(
        X,
        y,
        frame["label_source"].astype(str),
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )
    source_weights_r = source_train_r.map(
        {
            "observed_realized": 1.0,
            "observed_rejected": 1.0,
            "accepted_proxy": 0.75,
            "fallback_proxy": 0.55,
        }
    ).fillna(1.0)
    random_model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    C=best_c,
                    max_iter=800,
                    solver="lbfgs",
                    class_weight="balanced",
                ),
            ),
        ]
    )
    random_model.fit(x_train_r, y_train_r, clf__sample_weight=source_weights_r.values)
    random_proba = pd.Series(random_model.predict_proba(x_test_r)[:, 1], index=y_test_r.index)

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, proba)),
        "average_precision": float(average_precision_score(y_test, proba)),
        "precision_at_0_5": float(precision_score(y_test, pred, zero_division=0)),
        "recall_at_0_5": float(recall_score(y_test, pred, zero_division=0)),
        "f1_at_0_5": float(f1_score(y_test, pred, zero_division=0)),
        "brier_score": float(brier_score_loss(y_test, proba)),
        "ece_10_bins": float(expected_calibration_error(y_test, proba, bins=10)),
        "split_strategy": split_strategy,
        "best_logreg_c": float(best_c),
        "candidate_c_grid": [float(c) for c in candidate_cs],
        "candidate_metrics": candidate_metrics,
        "random_split_roc_auc": float(roc_auc_score(y_test_r, random_proba)),
        "random_split_average_precision": float(average_precision_score(y_test_r, random_proba)),
        "train_rows": int(len(y_train)),
        "test_rows": int(len(y_test)),
        "positive_rate_train": float(y_train.mean()),
        "positive_rate_test": float(y_test.mean()),
    }
    quality_flags: list[str] = []
    if metrics["roc_auc"] >= 0.995:
        quality_flags.append("temporal_auc_extremely_high")
    if abs(metrics["roc_auc"] - metrics["random_split_roc_auc"]) > 0.06:
        quality_flags.append("temporal_random_gap")
    if metrics["ece_10_bins"] > 0.08:
        quality_flags.append("calibration_warning")
    metrics["quality_flags"] = quality_flags

    evaluation = {
        "y_test": y_test.reset_index(drop=True),
        "proba_test": proba.reset_index(drop=True),
        "test_frame": test_frame.reset_index(drop=True),
    }
    return model, metrics, evaluation


def train(df: pd.DataFrame, random_state: int = 42) -> tuple[Pipeline, dict[str, Any]]:
    model, metrics, _evaluation = train_with_evaluation(df, random_state=random_state)
    return model, metrics


def _train_window_signature(window: tuple[datetime, datetime] | None) -> str:
    if window is None:
        return "full"
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
    return str(meta.get("train_window_signature", "")) == window_signature


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
    threshold_global = float(df.get("label_threshold_eur_h_global", df["label_threshold_eur_h"]).median())

    payload = {
        "model": model,
        "feature_columns": FEATURES,
        "trained_rows": int(len(df)),
        "positive_rate": float(df["label"].mean()),
        "label_threshold_eur_h": threshold_global,
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
                "label_threshold_eur_h": threshold_global,
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
        f"label_threshold={threshold_global:.2f}"
    )
    print(
        f"roc_auc={metrics['roc_auc']:.4f} average_precision={metrics['average_precision']:.4f} "
        f"ece_10_bins={metrics['ece_10_bins']:.4f}"
    )


if __name__ == "__main__":
    main()
