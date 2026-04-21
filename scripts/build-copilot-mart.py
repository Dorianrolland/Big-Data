"""COP-025 — Construit le Data Mart analytique Copilot.

Lit data/parquet_events (cold path) et produit des tables Parquet
optimisées dans data/marts/copilot/ prêtes pour requêtes DuckDB rapides.

Tables produites:
  fact_offers           — offres reçues + features Copilot
  fact_order_events     — événements de commande (acceptation, refus, complétion)
  fact_context_signals  — signaux contextuels (météo, trafic, IRVE, demande)
  fact_missions         — journal des missions (depuis mission_journal si dispo)

Usage:
    python3 scripts/build-copilot-mart.py
    python3 scripts/build-copilot-mart.py --parquet data/parquet_events --out data/marts/copilot
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent

FUEL_PRICE_EUR_L = 1.85
CONSUMPTION_L_100KM = 7.5
PLATFORM_FEE_PCT = 25.0


def _fuel_cost_sql() -> str:
    return f"(estimated_distance_km / 100.0 * {CONSUMPTION_L_100KM} * {FUEL_PRICE_EUR_L})"


def _net_eur_sql() -> str:
    return f"(estimated_fare_eur * (1.0 - {PLATFORM_FEE_PCT} / 100.0) - {_fuel_cost_sql()})"


def _net_eur_h_sql() -> str:
    return f"(CASE WHEN estimated_duration_min > 0 THEN {_net_eur_sql()} / (estimated_duration_min / 60.0) ELSE 0 END)"


def build_mart(parquet_dir: Path, out_dir: Path) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    counts: dict[str, int] = {}
    started = datetime.now(timezone.utc)

    def _parquet_glob(topic: str) -> str:
        p = parquet_dir / f"topic={topic}"
        return str(p / "**" / "*.parquet")

    def _source_exists(topic: str) -> bool:
        return (parquet_dir / f"topic={topic}").exists()

    # ── fact_offers ────────────────────────────────────────────────────────────
    if _source_exists("order-offers-v1"):
        print("[mart] Building fact_offers...", end=" ", flush=True)
        path = out_dir / "fact_offers.parquet"
        con.execute(f"""
            COPY (
                SELECT
                    offer_id,
                    order_id,
                    courier_id,
                    zone_id,
                    source_platform,
                    ts::TIMESTAMPTZ AS ts,
                    year, month, day, hour,
                    estimated_fare_eur,
                    estimated_distance_km,
                    estimated_duration_min,
                    actual_fare_eur,
                    actual_distance_km,
                    actual_duration_min,
                    demand_index,
                    supply_index,
                    weather_factor,
                    traffic_factor,
                    demand_index / NULLIF(supply_index, 0) AS pressure_ratio,
                    {_net_eur_sql()} AS net_eur,
                    {_net_eur_h_sql()} AS net_eur_h,
                    {_fuel_cost_sql()} AS fuel_cost_eur,
                    status
                FROM read_parquet('{_parquet_glob("order-offers-v1")}', hive_partitioning=true)
            ) TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
        counts["fact_offers"] = n
        print(f"{n} rows")

    # ── fact_order_events ─────────────────────────────────────────────────────
    if _source_exists("order-events-v1"):
        print("[mart] Building fact_order_events...", end=" ", flush=True)
        path = out_dir / "fact_order_events.parquet"
        con.execute(f"""
            COPY (
                SELECT
                    event_id,
                    offer_id,
                    order_id,
                    courier_id,
                    zone_id,
                    source_platform,
                    event_type,
                    status,
                    ts::TIMESTAMPTZ AS ts,
                    year, month, day, hour,
                    estimated_fare_eur,
                    estimated_distance_km,
                    estimated_duration_min,
                    actual_fare_eur,
                    actual_distance_km,
                    actual_duration_min,
                    demand_index,
                    supply_index,
                    weather_factor,
                    traffic_factor,
                    {_net_eur_sql()} AS net_eur,
                    {_net_eur_h_sql()} AS net_eur_h
                FROM read_parquet('{_parquet_glob("order-events-v1")}', hive_partitioning=true)
            ) TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
        counts["fact_order_events"] = n
        print(f"{n} rows")

    # ── fact_context_signals ──────────────────────────────────────────────────
    if _source_exists("context-signals-v1"):
        print("[mart] Building fact_context_signals...", end=" ", flush=True)
        path = out_dir / "fact_context_signals.parquet"
        con.execute(f"""
            COPY (
                SELECT
                    event_id,
                    courier_id,
                    zone_id,
                    source_platform,
                    ts::TIMESTAMPTZ AS ts,
                    year, month, day, hour,
                    demand_index,
                    supply_index,
                    weather_factor,
                    traffic_factor,
                    demand_index / NULLIF(supply_index, 0) AS pressure_ratio,
                    CASE WHEN demand_index > 0.7 AND supply_index < 0.4 THEN TRUE ELSE FALSE END AS surge_candidate
                FROM read_parquet('{_parquet_glob("context-signals-v1")}', hive_partitioning=true)
            ) TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        n = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
        counts["fact_context_signals"] = n
        print(f"{n} rows")

    # ── fact_missions (synthétique depuis parquet_events si aucun journal) ────
    print("[mart] Building fact_missions (from offers + events join)...", end=" ", flush=True)
    path = out_dir / "fact_missions.parquet"
    if _source_exists("order-events-v1") and _source_exists("order-offers-v1"):
        con.execute(f"""
            COPY (
                SELECT
                    e.event_id AS mission_id,
                    e.courier_id,
                    e.zone_id,
                    e.source_platform,
                    e.ts::TIMESTAMPTZ AS completed_at,
                    e.year, e.month, e.day,
                    e.actual_fare_eur,
                    e.actual_distance_km,
                    e.actual_duration_min,
                    o.estimated_fare_eur,
                    o.estimated_distance_km,
                    o.estimated_duration_min,
                    {_net_eur_sql().replace('estimated_fare_eur', 'e.actual_fare_eur').replace('estimated_distance_km', 'e.actual_distance_km')} AS realized_net_eur,
                    {_net_eur_h_sql().replace('estimated_fare_eur', 'e.actual_fare_eur').replace('estimated_distance_km', 'e.actual_distance_km').replace('estimated_duration_min', 'e.actual_duration_min')} AS realized_net_eur_h,
                    e.demand_index,
                    e.supply_index,
                    e.weather_factor,
                    e.traffic_factor,
                    e.status
                FROM read_parquet('{_parquet_glob("order-events-v1")}', hive_partitioning=true) e
                LEFT JOIN read_parquet('{_parquet_glob("order-offers-v1")}', hive_partitioning=true) o
                    ON e.offer_id = o.offer_id
            ) TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
    elif _source_exists("order-events-v1"):
        con.execute(f"""
            COPY (
                SELECT event_id AS mission_id, courier_id, zone_id, source_platform,
                    ts::TIMESTAMPTZ AS completed_at, year, month, day,
                    actual_fare_eur, actual_distance_km, actual_duration_min,
                    demand_index, supply_index, weather_factor, traffic_factor, status
                FROM read_parquet('{_parquet_glob("order-events-v1")}', hive_partitioning=true)
            ) TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
    else:
        con.execute(f"""
            COPY (SELECT 'no_data' AS mission_id, 0 AS realized_net_eur)
            TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
    n = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    counts["fact_missions"] = n
    print(f"{n} rows")

    # ── meta ──────────────────────────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\n[mart] Done in {elapsed:.1f}s — tables in {out_dir}")
    print(f"[mart] Rows: { {k: v for k, v in counts.items()} }")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Copilot Data Mart")
    parser.add_argument("--parquet", default="data/parquet_events")
    parser.add_argument("--out", default="data/marts/copilot")
    args = parser.parse_args()

    parquet_dir = _ROOT / args.parquet
    out_dir = _ROOT / args.out

    if not parquet_dir.exists():
        print(f"[mart] ERROR: parquet directory not found: {parquet_dir}")
        sys.exit(1)

    build_mart(parquet_dir, out_dir)


if __name__ == "__main__":
    main()
