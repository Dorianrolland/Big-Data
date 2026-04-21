"""COP-025 — Requêtes KPI sur le Data Mart Copilot.

Sort 10 KPIs clés en < 5s pour démonstration Big Data jury.

Usage:
    python3 scripts/query-copilot-kpis.py
    python3 scripts/query-copilot-kpis.py --mart data/marts/copilot --fmt table
    python3 scripts/query-copilot-kpis.py --fmt json
"""
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
        "sql": "SELECT ROUND(100.0 * SUM(CASE WHEN surge_candidate THEN 1 ELSE 0 END) / COUNT(*), 2) AS surge_rate_pct FROM '{table}'",
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
        "label": "Accept rate (offres → events)",
        "table": "fact_order_events",
        "sql": "SELECT COUNT(*) AS events, COUNT(DISTINCT courier_id) AS drivers FROM '{table}'",
    },
    {
        "id": "kpi_06",
        "label": "Net EUR/h moyen sur offres",
        "table": "fact_offers",
        "sql": "SELECT ROUND(AVG(net_eur_h), 2) AS avg_net_eur_h, ROUND(AVG(net_eur), 2) AS avg_net_eur FROM '{table}'",
    },
    {
        "id": "kpi_07",
        "label": "Répartition par source_platform",
        "table": "fact_context_signals",
        "sql": "SELECT source_platform, COUNT(*) AS cnt FROM '{table}' GROUP BY source_platform ORDER BY cnt DESC",
    },
    {
        "id": "kpi_08",
        "label": "Profil horaire (heure pic de demande)",
        "table": "fact_context_signals",
        "sql": "SELECT hour, ROUND(AVG(demand_index), 4) AS avg_demand, ROUND(AVG(pressure_ratio), 4) AS avg_pressure FROM '{table}' GROUP BY hour ORDER BY avg_demand DESC LIMIT 5",
    },
    {
        "id": "kpi_09",
        "label": "Impact météo sur pression offre",
        "table": "fact_context_signals",
        "sql": "SELECT ROUND(weather_factor, 1) AS weather_bucket, ROUND(AVG(demand_index), 4) AS avg_demand, COUNT(*) AS n FROM '{table}' GROUP BY weather_bucket ORDER BY weather_bucket",
    },
    {
        "id": "kpi_10",
        "label": "Missions réalisées net EUR/h vs predit",
        "table": "fact_missions",
        "sql": "SELECT COUNT(*) AS missions, ROUND(AVG(realized_net_eur_h), 2) AS avg_realized_net_eur_h FROM '{table}' WHERE realized_net_eur_h IS NOT NULL",
    },
]


def run_kpis(mart_dir: Path, fmt: str = "table") -> list[dict]:
    con = duckdb.connect()
    results = []
    total_start = time.perf_counter()

    for kpi in KPI_QUERIES:
        table_path = mart_dir / f"{kpi['table']}.parquet"
        if not table_path.exists():
            if fmt == "table":
                print(f"  [{kpi['id']}] {kpi['label']}: SKIP (table missing)")
            continue

        sql = kpi["sql"].replace("{table}", str(table_path))
        t0 = time.perf_counter()
        try:
            df = con.execute(sql).fetchdf()
            elapsed_ms = (time.perf_counter() - t0) * 1000
        except Exception as e:
            if fmt == "table":
                print(f"  [{kpi['id']}] {kpi['label']}: ERROR — {e}")
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
            print(f"\n  [{kpi['id']}] {kpi['label']}  ({elapsed_ms:.1f} ms)")
            print(df.to_string(index=False))

    total_ms = (time.perf_counter() - total_start) * 1000
    if fmt == "table":
        print(f"\n── {len(results)} KPIs en {total_ms:.0f} ms ──")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Requêtes KPI Copilot Data Mart")
    parser.add_argument("--mart", default="data/marts/copilot")
    parser.add_argument("--fmt", choices=["table", "json"], default="table")
    args = parser.parse_args()

    mart_dir = _ROOT / args.mart
    if not mart_dir.exists():
        print(f"[kpi] Mart not found: {mart_dir} — build it first:")
        print("  python3 scripts/build-copilot-mart.py")
        sys.exit(1)

    if args.fmt == "table":
        print("\n══ FleetStream — KPIs Data Mart Copilot ══")
    results = run_kpis(mart_dir, fmt=args.fmt)

    if args.fmt == "json":
        print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
