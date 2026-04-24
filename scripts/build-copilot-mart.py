"""Build the Copilot analytical mart from cold-path parquet data.

Outputs:
  - fact_offers.parquet
  - fact_order_events.parquet
  - fact_context_signals.parquet
  - fact_missions.parquet

The builder is schema-drift tolerant for source columns:
  - uses `source_platform` when present
  - falls back to `source`
  - otherwise defaults to `unknown` (non-blocking)

Usage:
  python scripts/build-copilot-mart.py
  python scripts/build-copilot-mart.py --parquet data/parquet_events --out data/marts/copilot
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb
import pyarrow.parquet as pq

_ROOT = Path(__file__).resolve().parent.parent
_VALID_TOPIC_FILES_CACHE: dict[tuple[str, str], list[Path]] = {}

KM_TO_MILES = 0.621371
FUEL_PRICE_USD_GALLON = 3.65
VEHICLE_MPG = 31.0
PLATFORM_FEE_PCT = 25.0


def _fuel_cost_sql(distance_col: str = "estimated_distance_km") -> str:
    return f"(({distance_col}) * {KM_TO_MILES} / {VEHICLE_MPG} * {FUEL_PRICE_USD_GALLON})"


def _net_eur_sql() -> str:
    return f"(estimated_fare_eur * (1.0 - {PLATFORM_FEE_PCT} / 100.0) - {_fuel_cost_sql()})"


def _net_eur_h_sql() -> str:
    return (
        f"(CASE WHEN estimated_duration_min > 0 "
        f"THEN GREATEST({_net_eur_sql()} / (estimated_duration_min / 60.0), 0.0) "
        f"ELSE 0 END)"
    )


def _duck_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _quarantine_root(parquet_dir: Path) -> Path:
    return parquet_dir.parent / "parquet_quarantine"


def _quarantine_file(parquet_dir: Path, file_path: Path, exc: Exception) -> None:
    quarantine_root = _quarantine_root(parquet_dir)
    relative = file_path.relative_to(parquet_dir)
    destination = quarantine_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination = destination.with_name(f"{destination.stem}_{int(datetime.now(timezone.utc).timestamp())}.parquet")
    shutil.move(str(file_path), str(destination))
    print(f"[mart] quarantined invalid parquet: {file_path} -> {destination} ({exc})")


def _validated_topic_files(parquet_dir: Path, topic: str) -> list[Path]:
    cache_key = (str(parquet_dir.resolve()), topic)
    cached = _VALID_TOPIC_FILES_CACHE.get(cache_key)
    if cached is not None:
        return list(cached)

    root = parquet_dir / f"topic={topic}"
    valid: list[Path] = []
    if root.exists():
        for file_path in sorted(root.rglob("*.parquet")):
            try:
                pq.read_metadata(file_path)
            except Exception as exc:
                _quarantine_file(parquet_dir, file_path, exc)
                continue
            valid.append(file_path)
    _VALID_TOPIC_FILES_CACHE[cache_key] = list(valid)
    return valid


def _topic_scan_sql(parquet_dir: Path, topic: str) -> str:
    files = _validated_topic_files(parquet_dir, topic)
    if not files:
        raise RuntimeError(f"[mart] no valid parquet files for topic={topic}")
    quoted = ", ".join(f"'{_duck_path(path)}'" for path in files)
    return f"read_parquet([{quoted}], hive_partitioning=true)"


def _source_exists(parquet_dir: Path, topic: str) -> bool:
    return bool(_validated_topic_files(parquet_dir, topic))


def _topic_columns(con: duckdb.DuckDBPyConnection, parquet_dir: Path, topic: str) -> set[str]:
    scan_sql = _topic_scan_sql(parquet_dir, topic)
    rows = con.execute(f"DESCRIBE SELECT * FROM {scan_sql} LIMIT 0").fetchall()
    return {str(name) for name, *_ in rows}


def _assert_contract(
    topic: str,
    cols: set[str],
    *,
    required: Iterable[str],
    any_of_groups: list[tuple[str, ...]] | None = None,
) -> None:
    missing = [col for col in required if col not in cols]
    if missing:
        raise RuntimeError(
            f"[mart] schema contract failed for topic={topic}: missing required columns={missing}"
        )

    if any_of_groups:
        for group in any_of_groups:
            if not any(col in cols for col in group):
                raise RuntimeError(
                    f"[mart] schema contract failed for topic={topic}: missing one of columns={list(group)}"
                )


def _txt_expr(cols: set[str], name: str, default_sql: str = "NULL") -> str:
    if name in cols:
        return f"NULLIF(CAST({name} AS VARCHAR), '')"
    return default_sql


def _num_expr(cols: set[str], name: str, default_sql: str = "0.0") -> str:
    if name in cols:
        return f"COALESCE(TRY_CAST({name} AS DOUBLE), {default_sql})"
    return default_sql


def _int_expr(cols: set[str], name: str, default_sql: str) -> str:
    if name in cols:
        return f"COALESCE(TRY_CAST({name} AS INTEGER), {default_sql})"
    return default_sql


def _source_platform_expr(cols: set[str]) -> str:
    if "source_platform" in cols and "source" in cols:
        return (
            "COALESCE(NULLIF(CAST(source_platform AS VARCHAR), ''), "
            "NULLIF(CAST(source AS VARCHAR), ''), 'unknown')"
        )
    if "source_platform" in cols:
        return "COALESCE(NULLIF(CAST(source_platform AS VARCHAR), ''), 'unknown')"
    if "source" in cols:
        return "COALESCE(NULLIF(CAST(source AS VARCHAR), ''), 'unknown')"
    return "'unknown'"


def _status_expr(cols: set[str], name: str = "status", default: str = "unknown") -> str:
    if name not in cols:
        return f"'{default}'"
    return (
        f"COALESCE(NULLIF(LOWER(TRIM(CAST({name} AS VARCHAR))), ''), '{default}')"
    )


def _time_parts_select(cols: set[str]) -> tuple[str, str, str, str]:
    year_sql = _int_expr(cols, "year", "EXTRACT(YEAR FROM CAST(ts AS TIMESTAMPTZ))")
    month_sql = _int_expr(cols, "month", "EXTRACT(MONTH FROM CAST(ts AS TIMESTAMPTZ))")
    day_sql = _int_expr(cols, "day", "EXTRACT(DAY FROM CAST(ts AS TIMESTAMPTZ))")
    hour_sql = _int_expr(cols, "hour", "EXTRACT(HOUR FROM CAST(ts AS TIMESTAMPTZ))")
    return year_sql, month_sql, day_sql, hour_sql


def _count_rows(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM '{_duck_path(parquet_path)}'").fetchone()[0])


def build_mart(parquet_dir: Path, out_dir: Path) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    _VALID_TOPIC_FILES_CACHE.clear()
    con = duckdb.connect()
    counts: dict[str, int] = {}
    started = datetime.now(timezone.utc)

    try:
        # -- fact_offers -----------------------------------------------------
        if _source_exists(parquet_dir, "order-offers-v1"):
            print("[mart] Building fact_offers...", end=" ", flush=True)
            topic = "order-offers-v1"
            scan_sql = _topic_scan_sql(parquet_dir, topic)
            cols = _topic_columns(con, parquet_dir, topic)
            _assert_contract(
                topic,
                cols,
                required=("offer_id", "ts"),
            )
            year_sql, month_sql, day_sql, hour_sql = _time_parts_select(cols)
            source_sql = _source_platform_expr(cols)
            path = out_dir / "fact_offers.parquet"
            con.execute(
                f"""
                COPY (
                    WITH src AS (
                        SELECT
                            {_txt_expr(cols, "offer_id", "'unknown_offer'")} AS offer_id,
                            {_txt_expr(cols, "order_id")} AS order_id,
                            {_txt_expr(cols, "courier_id", "'unknown_courier'")} AS courier_id,
                            {_txt_expr(cols, "zone_id", "'unknown_zone'")} AS zone_id,
                            {source_sql} AS source_platform,
                            CAST(ts AS TIMESTAMPTZ) AS ts,
                            {year_sql} AS year,
                            {month_sql} AS month,
                            {day_sql} AS day,
                            {hour_sql} AS hour,
                            {_num_expr(cols, "estimated_fare_eur", "0.0")} AS estimated_fare_eur,
                            {_num_expr(cols, "estimated_distance_km", "0.0")} AS estimated_distance_km,
                            {_num_expr(cols, "estimated_duration_min", "0.0")} AS estimated_duration_min,
                            {_num_expr(cols, "actual_fare_eur", "NULL")} AS actual_fare_eur,
                            {_num_expr(cols, "actual_distance_km", "NULL")} AS actual_distance_km,
                            {_num_expr(cols, "actual_duration_min", "NULL")} AS actual_duration_min,
                            {_num_expr(cols, "demand_index", "1.0")} AS demand_index,
                            {_num_expr(cols, "supply_index", "1.0")} AS supply_index,
                            {_num_expr(cols, "weather_factor", "1.0")} AS weather_factor,
                            {_num_expr(cols, "traffic_factor", "1.0")} AS traffic_factor,
                            {_status_expr(cols, "status")} AS status
                        FROM {scan_sql}
                    )
                    SELECT
                        offer_id,
                        order_id,
                        courier_id,
                        zone_id,
                        source_platform,
                        ts,
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
                        COALESCE(demand_index / NULLIF(supply_index, 0), 0.0) AS pressure_ratio,
                        {_net_eur_sql()} AS net_eur,
                        {_net_eur_h_sql()} AS net_eur_h,
                        {_fuel_cost_sql()} AS fuel_cost_usd,
                        status
                    FROM src
                ) TO '{_duck_path(path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
            n = _count_rows(con, path)
            counts["fact_offers"] = n
            print(f"{n} rows")

        # -- fact_order_events -----------------------------------------------
        if _source_exists(parquet_dir, "order-events-v1"):
            print("[mart] Building fact_order_events...", end=" ", flush=True)
            topic = "order-events-v1"
            scan_sql = _topic_scan_sql(parquet_dir, topic)
            cols = _topic_columns(con, parquet_dir, topic)
            _assert_contract(
                topic,
                cols,
                required=("event_id", "offer_id", "ts", "status"),
            )
            year_sql, month_sql, day_sql, hour_sql = _time_parts_select(cols)
            source_sql = _source_platform_expr(cols)
            path = out_dir / "fact_order_events.parquet"
            con.execute(
                f"""
                COPY (
                    WITH src AS (
                        SELECT
                            {_txt_expr(cols, "event_id", "'unknown_event'")} AS event_id,
                            {_txt_expr(cols, "offer_id", "'unknown_offer'")} AS offer_id,
                            {_txt_expr(cols, "order_id")} AS order_id,
                            {_txt_expr(cols, "courier_id", "'unknown_courier'")} AS courier_id,
                            {_txt_expr(cols, "zone_id", "'unknown_zone'")} AS zone_id,
                            {source_sql} AS source_platform,
                            COALESCE({_txt_expr(cols, "event_type")}, 'order.event.v1') AS event_type,
                            {_status_expr(cols, "status")} AS status,
                            CAST(ts AS TIMESTAMPTZ) AS ts,
                            {year_sql} AS year,
                            {month_sql} AS month,
                            {day_sql} AS day,
                            {hour_sql} AS hour,
                            {_num_expr(cols, "estimated_fare_eur", "0.0")} AS estimated_fare_eur,
                            {_num_expr(cols, "estimated_distance_km", "0.0")} AS estimated_distance_km,
                            {_num_expr(cols, "estimated_duration_min", "0.0")} AS estimated_duration_min,
                            {_num_expr(cols, "actual_fare_eur", "NULL")} AS actual_fare_eur,
                            {_num_expr(cols, "actual_distance_km", "NULL")} AS actual_distance_km,
                            {_num_expr(cols, "actual_duration_min", "NULL")} AS actual_duration_min,
                            {_num_expr(cols, "demand_index", "1.0")} AS demand_index,
                            {_num_expr(cols, "supply_index", "1.0")} AS supply_index,
                            {_num_expr(cols, "weather_factor", "1.0")} AS weather_factor,
                            {_num_expr(cols, "traffic_factor", "1.0")} AS traffic_factor
                        FROM {scan_sql}
                    )
                    SELECT
                        event_id,
                        offer_id,
                        order_id,
                        courier_id,
                        zone_id,
                        source_platform,
                        event_type,
                        status,
                        ts,
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
                    FROM src
                ) TO '{_duck_path(path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
            n = _count_rows(con, path)
            counts["fact_order_events"] = n
            print(f"{n} rows")

        # -- fact_context_signals --------------------------------------------
        if _source_exists(parquet_dir, "context-signals-v1"):
            print("[mart] Building fact_context_signals...", end=" ", flush=True)
            topic = "context-signals-v1"
            scan_sql = _topic_scan_sql(parquet_dir, topic)
            cols = _topic_columns(con, parquet_dir, topic)
            _assert_contract(
                topic,
                cols,
                required=("zone_id", "ts"),
            )
            year_sql, month_sql, day_sql, hour_sql = _time_parts_select(cols)
            source_sql = _source_platform_expr(cols)
            path = out_dir / "fact_context_signals.parquet"
            con.execute(
                f"""
                COPY (
                    WITH src AS (
                        SELECT
                            {_txt_expr(cols, "event_id", "'unknown_event'")} AS event_id,
                            {_txt_expr(cols, "courier_id", "'unknown_courier'")} AS courier_id,
                            {_txt_expr(cols, "zone_id", "'unknown_zone'")} AS zone_id,
                            {source_sql} AS source_platform,
                            CAST(ts AS TIMESTAMPTZ) AS ts,
                            {year_sql} AS year,
                            {month_sql} AS month,
                            {day_sql} AS day,
                            {hour_sql} AS hour,
                            {_num_expr(cols, "demand_index", "1.0")} AS demand_index,
                            {_num_expr(cols, "supply_index", "1.0")} AS supply_index,
                            {_num_expr(cols, "weather_factor", "1.0")} AS weather_factor,
                            {_num_expr(cols, "traffic_factor", "1.0")} AS traffic_factor
                        FROM {scan_sql}
                    )
                    SELECT
                        event_id,
                        courier_id,
                        zone_id,
                        source_platform,
                        ts,
                        year, month, day, hour,
                        demand_index,
                        supply_index,
                        weather_factor,
                        traffic_factor,
                        COALESCE(demand_index / NULLIF(supply_index, 0), 0.0) AS pressure_ratio,
                        CASE WHEN demand_index > 0.7 AND supply_index < 0.4 THEN TRUE ELSE FALSE END AS surge_candidate
                    FROM src
                ) TO '{_duck_path(path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
            n = _count_rows(con, path)
            counts["fact_context_signals"] = n
            print(f"{n} rows")

        # -- fact_missions ---------------------------------------------------
        print("[mart] Building fact_missions (from offers + events join)...", end=" ", flush=True)
        missions_path = out_dir / "fact_missions.parquet"
        offers_path = out_dir / "fact_offers.parquet"
        events_path = out_dir / "fact_order_events.parquet"

        if events_path.exists() and offers_path.exists():
            con.execute(
                f"""
                COPY (
                    SELECT
                        e.event_id AS mission_id,
                        e.courier_id,
                        e.zone_id,
                        e.source_platform,
                        e.ts AS completed_at,
                        e.year, e.month, e.day,
                        e.actual_fare_eur,
                        e.actual_distance_km,
                        e.actual_duration_min,
                        o.estimated_fare_eur,
                        o.estimated_distance_km,
                        o.estimated_duration_min,
                        CASE
                            WHEN e.actual_fare_eur IS NULL OR e.actual_distance_km IS NULL THEN NULL
                            ELSE (e.actual_fare_eur * (1.0 - {PLATFORM_FEE_PCT} / 100.0)
                                  - {_fuel_cost_sql("e.actual_distance_km")})
                        END AS realized_net_eur,
                        CASE
                            WHEN e.actual_fare_eur IS NULL OR e.actual_distance_km IS NULL OR COALESCE(e.actual_duration_min, 0) <= 0 THEN NULL
                            ELSE (
                                (e.actual_fare_eur * (1.0 - {PLATFORM_FEE_PCT} / 100.0)
                                 - {_fuel_cost_sql("e.actual_distance_km")})
                                / (e.actual_duration_min / 60.0)
                            )
                        END AS realized_net_eur_h,
                        e.demand_index,
                        e.supply_index,
                        e.weather_factor,
                        e.traffic_factor,
                        e.status
                    FROM '{_duck_path(events_path)}' e
                    LEFT JOIN (
                        SELECT *
                        EXCLUDE (rn)
                        FROM (
                            SELECT
                                *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY offer_id
                                    ORDER BY ts DESC NULLS LAST
                                ) AS rn
                            FROM '{_duck_path(offers_path)}'
                        )
                        WHERE rn = 1
                    ) o
                        ON e.offer_id = o.offer_id
                ) TO '{_duck_path(missions_path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
        elif events_path.exists():
            con.execute(
                f"""
                COPY (
                    SELECT
                        event_id AS mission_id,
                        courier_id,
                        zone_id,
                        source_platform,
                        ts AS completed_at,
                        year, month, day,
                        actual_fare_eur,
                        actual_distance_km,
                        actual_duration_min,
                        CAST(NULL AS DOUBLE) AS estimated_fare_eur,
                        CAST(NULL AS DOUBLE) AS estimated_distance_km,
                        CAST(NULL AS DOUBLE) AS estimated_duration_min,
                        CASE
                            WHEN actual_fare_eur IS NULL OR actual_distance_km IS NULL THEN NULL
                            ELSE (actual_fare_eur * (1.0 - {PLATFORM_FEE_PCT} / 100.0)
                                  - {_fuel_cost_sql("actual_distance_km")})
                        END AS realized_net_eur,
                        CASE
                            WHEN actual_fare_eur IS NULL OR actual_distance_km IS NULL OR COALESCE(actual_duration_min, 0) <= 0 THEN NULL
                            ELSE (
                                (actual_fare_eur * (1.0 - {PLATFORM_FEE_PCT} / 100.0)
                                 - {_fuel_cost_sql("actual_distance_km")})
                                / (actual_duration_min / 60.0)
                            )
                        END AS realized_net_eur_h,
                        demand_index,
                        supply_index,
                        weather_factor,
                        traffic_factor,
                        status
                    FROM '{_duck_path(events_path)}'
                ) TO '{_duck_path(missions_path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )
        else:
            con.execute(
                f"""
                COPY (
                    SELECT
                        'no_data' AS mission_id,
                        CAST(NULL AS VARCHAR) AS courier_id,
                        CAST(NULL AS VARCHAR) AS zone_id,
                        'unknown' AS source_platform,
                        CAST(NULL AS TIMESTAMPTZ) AS completed_at,
                        CAST(NULL AS INTEGER) AS year,
                        CAST(NULL AS INTEGER) AS month,
                        CAST(NULL AS INTEGER) AS day,
                        CAST(NULL AS DOUBLE) AS actual_fare_eur,
                        CAST(NULL AS DOUBLE) AS actual_distance_km,
                        CAST(NULL AS DOUBLE) AS actual_duration_min,
                        CAST(NULL AS DOUBLE) AS estimated_fare_eur,
                        CAST(NULL AS DOUBLE) AS estimated_distance_km,
                        CAST(NULL AS DOUBLE) AS estimated_duration_min,
                        CAST(NULL AS DOUBLE) AS realized_net_eur,
                        CAST(NULL AS DOUBLE) AS realized_net_eur_h,
                        CAST(NULL AS DOUBLE) AS demand_index,
                        CAST(NULL AS DOUBLE) AS supply_index,
                        CAST(NULL AS DOUBLE) AS weather_factor,
                        CAST(NULL AS DOUBLE) AS traffic_factor,
                        'unknown' AS status
                ) TO '{_duck_path(missions_path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """
            )

        n = _count_rows(con, missions_path)
        counts["fact_missions"] = n
        print(f"{n} rows")

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        print(f"\n[mart] Done in {elapsed:.1f}s - tables in {out_dir}")
        print(f"[mart] Rows: {counts}")
        return counts
    finally:
        con.close()


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
