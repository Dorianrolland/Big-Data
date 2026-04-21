"""COP-026 — Compaction des petits fichiers Parquet du cold path.

Chaque batch Kafka flush un petit fichier Parquet (~425 kB). Après une session
de démo, on se retrouve avec 200 fichiers pour ~6 MB. Ce script compacte les
fichiers par partition (topic/year/month/day/hour) en un seul fichier Parquet
snappy, sans perte de données.

Produit un rapport avant/après: nb fichiers, taille, latence requêtes DuckDB.

Usage:
    python3 scripts/compact-events-parquet.py
    python3 scripts/compact-events-parquet.py --parquet data/parquet_events --dry-run
    python3 scripts/compact-events-parquet.py --report-only
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent

BENCHMARK_QUERIES = [
    {
        "id": "q1",
        "label": "COUNT + AVG (scan global)",
        "sql": "SELECT COUNT(*) AS n, AVG(demand_index) AS avg_demand FROM read_parquet('{glob}', hive_partitioning=true)",
    },
    {
        "id": "q2",
        "label": "GROUP BY zone + ORDER BY",
        "sql": "SELECT zone_id, ROUND(AVG(pressure_ratio), 4) AS avg_pressure FROM read_parquet('{glob}', hive_partitioning=true) GROUP BY zone_id ORDER BY avg_pressure DESC LIMIT 10",
    },
]


def _count_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*.parquet"))


def _dir_size_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*.parquet") if f.is_file())


def _benchmark(glob_pattern: str, con: duckdb.DuckDBPyConnection) -> list[dict]:
    results = []
    for q in BENCHMARK_QUERIES:
        sql = q["sql"].replace("{glob}", glob_pattern)
        t0 = time.perf_counter()
        try:
            con.execute(sql).fetchdf()
            elapsed_ms = (time.perf_counter() - t0) * 1000
        except Exception as e:
            elapsed_ms = -1.0
        results.append({"id": q["id"], "label": q["label"], "elapsed_ms": round(elapsed_ms, 1)})
    return results


def compact_topic(topic_dir: Path, out_topic_dir: Path, dry_run: bool = False) -> dict:
    """Compacte tous les fichiers d'un topic par partition heure vers out_topic_dir.

    Écrit dans un répertoire de sortie (ne modifie pas les originaux) pour
    rester compatible avec des données produites par Docker (root:root).
    """
    topic_name = topic_dir.name.replace("topic=", "")
    con = duckdb.connect()

    # Gather all partitions (topic/year=X/month=Y/day=Z/hour=H)
    partitions: list[Path] = []
    for hour_dir in topic_dir.rglob("hour=*"):
        files = list(hour_dir.glob("*.parquet"))
        if len(files) >= 1:
            partitions.append(hour_dir)

    files_before = _count_files(topic_dir)
    size_before = _dir_size_bytes(topic_dir)
    glob_before = str(topic_dir / "**" / "*.parquet")
    bench_before = _benchmark(glob_before, con)

    rows_before = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob_before}', hive_partitioning=true)"
    ).fetchone()[0]

    if dry_run:
        print(f"  [DRY-RUN] {topic_name}: {len(partitions)} partitions à compacter ({files_before} fichiers → ~{len(partitions)} fichiers)")
        return {"topic": topic_name, "dry_run": True, "partitions": len(partitions)}

    # Write compacted files to output dir, replicating partition structure
    compacted = 0
    for part_dir in partitions:
        files = list(part_dir.glob("*.parquet"))
        if not files:
            continue

        # Replicate partition path relative to topic_dir
        rel = part_dir.relative_to(topic_dir)
        out_part = out_topic_dir / rel
        out_part.mkdir(parents=True, exist_ok=True)

        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        out_file = out_part / "compacted.parquet"
        df.to_parquet(out_file, index=False, compression="snappy")
        compacted += 1

    files_after = _count_files(out_topic_dir)
    size_after = _dir_size_bytes(out_topic_dir)
    glob_after = str(out_topic_dir / "**" / "*.parquet")
    bench_after = _benchmark(glob_after, con)

    rows_after = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{glob_after}', hive_partitioning=true)"
    ).fetchone()[0]

    result = {
        "topic": topic_name,
        "partitions_compacted": compacted,
        "files_before": files_before,
        "files_after": files_after,
        "files_reduced_pct": round((files_before - files_after) / max(files_before, 1) * 100, 1),
        "size_before_bytes": size_before,
        "size_after_bytes": size_after,
        "size_delta_bytes": size_before - size_after,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "row_count_ok": (rows_before == rows_after),
        "benchmark_before": bench_before,
        "benchmark_after": bench_after,
    }
    return result


def run_compaction(
    parquet_dir: Path,
    out_dir: Path | None = None,
    dry_run: bool = False,
    report_only: bool = False,
) -> dict:
    topics = [d for d in parquet_dir.iterdir() if d.is_dir() and d.name.startswith("topic=")]
    if not topics:
        print(f"[compact] Aucun topic trouvé dans {parquet_dir}")
        return {}

    # Output dir : data/parquet_compacted/ next to parquet_events/
    if out_dir is None:
        out_dir = parquet_dir.parent / "parquet_compacted"
    out_dir.mkdir(parents=True, exist_ok=True)

    total_before = _count_files(parquet_dir)
    print(f"\n── Compaction Parquet Events ─────────────────────────────")
    print(f"  Source     : {parquet_dir}")
    print(f"  Sortie     : {out_dir}")
    print(f"  Fichiers   : {total_before}")
    print(f"  Mode       : {'dry-run' if dry_run else 'report-only' if report_only else 'compaction → ' + str(out_dir)}")

    if report_only:
        con = duckdb.connect()
        for topic_dir in topics:
            glob = str(topic_dir / "**" / "*.parquet")
            n = _count_files(topic_dir)
            size = _dir_size_bytes(topic_dir)
            bench = _benchmark(glob, con)
            print(f"\n  {topic_dir.name}: {n} fichiers, {size//1024} kB")
            for b in bench:
                print(f"    [{b['id']}] {b['label']}: {b['elapsed_ms']} ms")
        return {}

    summary = {"topics": [], "out_dir": str(out_dir)}
    for topic_dir in topics:
        out_topic_dir = out_dir / topic_dir.name
        print(f"\n  Compactage {topic_dir.name} → {out_topic_dir.name}...")
        result = compact_topic(topic_dir, out_topic_dir, dry_run=dry_run)
        summary["topics"].append(result)
        if not dry_run:
            row_ok = "OK" if result.get("row_count_ok") else "ERREUR! row_count mismatch"
            print(f"    {result['files_before']} → {result['files_after']} fichiers "
                  f"({result.get('files_reduced_pct', 0)}% réduit) | rows: {row_ok}")
            for b_bef, b_aft in zip(result["benchmark_before"], result["benchmark_after"]):
                delta = b_bef["elapsed_ms"] - b_aft["elapsed_ms"]
                sign = "+" if delta > 0 else ""
                print(f"    [{b_bef['id']}] {b_bef['label']}: {b_bef['elapsed_ms']}ms → {b_aft['elapsed_ms']}ms ({sign}{delta:.1f}ms)")

    # Save report
    report_path = parquet_dir.parent / "reports" / "compaction_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n[compact] Rapport sauvé: {report_path}")

    total_after = _count_files(parquet_dir)
    print(f"[compact] Total: {total_before} → {total_after} fichiers")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Compaction Parquet cold path")
    parser.add_argument("--parquet", default="data/parquet_events")
    parser.add_argument("--out", default=None, help="Répertoire de sortie (défaut: data/parquet_compacted)")
    parser.add_argument("--dry-run", action="store_true", help="Affiche ce qui serait fait sans modifier")
    parser.add_argument("--report-only", action="store_true", help="Affiche stats sans compacter")
    args = parser.parse_args()

    parquet_dir = _ROOT / args.parquet
    out_dir = (_ROOT / args.out) if args.out else None
    if not parquet_dir.exists():
        print(f"[compact] Répertoire introuvable: {parquet_dir}")
        sys.exit(1)

    run_compaction(parquet_dir, out_dir=out_dir, dry_run=args.dry_run, report_only=args.report_only)


if __name__ == "__main__":
    main()
