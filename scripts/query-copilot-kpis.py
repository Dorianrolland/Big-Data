"""Run KPI queries on the Copilot data mart."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent

KPI_QUERIES: list[dict] = [
    {
        "id": "kpi_01",
        "label": "Volume total de signaux contextuels",
        "table": "fact_context_signals",
        "sql": "SELECT COUNT(*) AS total_signals FROM '{table}'",
    },
    {
        "id": "kpi_02",
        "label": "Taux de surge (demand>0.7 & supply<0.4)",
        "table": "fact_context_signals",
        "sql": "SELECT ROUND(100.0 * SUM(CASE WHEN surge_candidate THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS surge_rate_pct FROM '{table}'",
    },
    {
        "id": "kpi_03",
        "label": "Ratio pression moyen (demand/supply)",
        "table": "fact_context_signals",
        "sql": "SELECT ROUND(AVG(pressure_ratio), 4) AS avg_pressure_ratio FROM '{table}'",
    },
    {
        "id": "kpi_04",
        "label": "Top 5 zones par volume de signaux",
        "table": "fact_context_signals",
        "sql": "SELECT zone_id, COUNT(*) AS signals FROM '{table}' GROUP BY zone_id ORDER BY signals DESC LIMIT 5",
    },
    {
        "id": "kpi_05",
        "label": "Accept rate reel (offres decidees)",
        "table": "fact_order_events",
        "sql": """
            WITH normalized AS (
                SELECT
                    offer_id,
                    COALESCE(NULLIF(LOWER(TRIM(status)), ''), 'unknown') AS status_norm,
                    ts
                FROM '{table}'
                WHERE offer_id IS NOT NULL AND offer_id <> ''
            ),
            final_decisions AS (
                SELECT
                    offer_id,
                    CASE WHEN status_norm = 'accepted' THEN 1 ELSE 0 END AS accepted_flag,
                    CASE WHEN status_norm = 'rejected' THEN 1 ELSE 0 END AS rejected_flag
                FROM (
                    SELECT
                        offer_id,
                        status_norm,
                        ROW_NUMBER() OVER (
                            PARTITION BY offer_id
                            ORDER BY ts DESC NULLS LAST
                        ) AS rn
                    FROM normalized
                    WHERE status_norm IN ('accepted', 'rejected')
                )
                WHERE rn = 1
            )
            SELECT
                CAST(COUNT(*) AS BIGINT) AS decided_offers,
                CAST(COALESCE(SUM(CASE WHEN accepted_flag = 1 THEN 1 ELSE 0 END), 0) AS BIGINT) AS accepted_offers,
                CAST(COALESCE(SUM(CASE WHEN rejected_flag = 1 THEN 1 ELSE 0 END), 0) AS BIGINT) AS rejected_offers,
                ROUND(
                    100.0 *
                    SUM(CASE WHEN accepted_flag = 1 THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS accept_rate_pct
            FROM final_decisions
        """,
    },
    {
        "id": "kpi_06",
        "label": "Net EUR/h moyen sur offres",
        "table": "fact_offers",
        "sql": "SELECT ROUND(AVG(net_eur_h), 2) AS avg_net_eur_h, ROUND(AVG(net_eur), 2) AS avg_net_eur FROM '{table}'",
    },
    {
        "id": "kpi_07",
        "label": "Repartition par source_platform",
        "table": "fact_context_signals",
        "sql": """
            SELECT
                COALESCE(NULLIF(split_part(source_platform, ';', 1), ''), 'unknown') AS source_platform,
                COUNT(*) AS cnt
            FROM '{table}'
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 10
        """,
    },
    {
        "id": "kpi_08",
        "label": "Profil horaire (heure pic de demande)",
        "table": "fact_context_signals",
        "sql": "SELECT hour, ROUND(AVG(demand_index), 4) AS avg_demand, ROUND(AVG(pressure_ratio), 4) AS avg_pressure FROM '{table}' GROUP BY hour ORDER BY avg_demand DESC LIMIT 5",
    },
    {
        "id": "kpi_09",
        "label": "Impact meteo sur pression offre",
        "table": "fact_context_signals",
        "sql": "SELECT ROUND(weather_factor, 1) AS weather_bucket, ROUND(AVG(demand_index), 4) AS avg_demand, COUNT(*) AS n FROM '{table}' GROUP BY weather_bucket ORDER BY weather_bucket",
    },
    {
        "id": "kpi_10",
        "label": "Missions realisees net EUR/h",
        "table": "fact_missions",
        "sql": "SELECT COUNT(*) AS missions, ROUND(AVG(realized_net_eur_h), 2) AS avg_realized_net_eur_h FROM '{table}' WHERE realized_net_eur_h IS NOT NULL",
    },
]


def _duck_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _safe_print(text: str) -> None:
    encoding = sys.stdout.encoding or "utf-8"
    print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def run_kpis(mart_dir: Path, fmt: str = "table") -> list[dict]:
    con = duckdb.connect()
    results = []
    total_start = time.perf_counter()

    try:
        for kpi in KPI_QUERIES:
            table_path = mart_dir / f"{kpi['table']}.parquet"
            if not table_path.exists():
                if fmt == "table":
                    _safe_print(f"  [{kpi['id']}] {kpi['label']}: SKIP (table missing)")
                continue

            sql = kpi["sql"].replace("{table}", _duck_path(table_path))
            t0 = time.perf_counter()
            try:
                df = con.execute(sql).fetchdf()
                elapsed_ms = (time.perf_counter() - t0) * 1000
            except Exception as exc:
                if fmt == "table":
                    _safe_print(f"  [{kpi['id']}] {kpi['label']}: ERROR - {exc}")
                continue

            entry = {
                "id": kpi["id"],
                "label": kpi["label"],
                "elapsed_ms": round(elapsed_ms, 1),
                "rows": len(df),
                "data": df.to_dict(orient="records"),
            }
            results.append(entry)

            if fmt == "table":
                _safe_print(f"\n  [{kpi['id']}] {kpi['label']}  ({elapsed_ms:.1f} ms)")
                _safe_print(df.to_string(index=False))

        total_ms = (time.perf_counter() - total_start) * 1000
        if fmt == "table":
            _safe_print(f"\n-- {len(results)} KPIs en {total_ms:.0f} ms --")
        return results
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Requetes KPI Copilot Data Mart")
    parser.add_argument("--mart", default="data/marts/copilot")
    parser.add_argument("--fmt", choices=["table", "json"], default="table")
    args = parser.parse_args()

    mart_dir = _ROOT / args.mart
    if not mart_dir.exists():
        _safe_print(f"[kpi] Mart not found: {mart_dir} - build it first:")
        _safe_print("  python scripts/build-copilot-mart.py")
        sys.exit(1)

    if args.fmt == "table":
        _safe_print("\n== FleetStream - KPIs Data Mart Copilot ==")
    results = run_kpis(mart_dir, fmt=args.fmt)

    if args.fmt == "json":
        _safe_print(json.dumps(results, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
