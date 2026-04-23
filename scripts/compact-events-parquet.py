"""COP-026 - Compaction des petits fichiers Parquet du cold path.

Chaque batch Kafka flush un petit fichier Parquet. Ce script compacte les
fichiers par partition (topic/year/month/day/hour) en un seul fichier Parquet
snappy, sans perte de donnees, a partir d'une liste figee de fichiers valides.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

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


def _duck_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _count_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*.parquet"))


def _dir_size_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*.parquet") if f.is_file())


def _benchmark(glob_pattern: str, con: duckdb.DuckDBPyConnection) -> list[dict]:
    results = []
    for q in BENCHMARK_QUERIES:
        sql = q["sql"].replace("{glob}", glob_pattern.replace("\\", "/"))
        t0 = time.perf_counter()
        try:
            con.execute(sql).fetchdf()
            elapsed_ms = (time.perf_counter() - t0) * 1000
        except Exception:
            elapsed_ms = -1.0
        results.append({"id": q["id"], "label": q["label"], "elapsed_ms": round(elapsed_ms, 1)})
    return results


def _quarantine_invalid_file(parquet_root: Path, file_path: Path, exc: Exception) -> None:
    if not file_path.exists():
        print(f"  [warn] invalid parquet vanished before quarantine: {file_path} ({exc})")
        return
    quarantine_root = parquet_root.parent / "parquet_quarantine"
    destination = quarantine_root / file_path.relative_to(parquet_root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination = destination.with_name(f"{destination.stem}_{int(time.time())}.parquet")
    try:
        shutil.move(str(file_path), str(destination))
    except FileNotFoundError:
        print(f"  [warn] invalid parquet vanished before quarantine: {file_path} ({exc})")
        return
    print(f"  [warn] invalid parquet quarantined: {file_path} -> {destination} ({exc})")


def _is_transient_parquet_file(file_path: Path) -> bool:
    name = file_path.name
    return name.startswith(".") or name.endswith(".tmp.parquet")


def _validated_topic_files(topic_dir: Path) -> list[Path]:
    parquet_root = topic_dir.parent
    valid: list[Path] = []
    for file_path in sorted(topic_dir.rglob("*.parquet")):
        if _is_transient_parquet_file(file_path):
            continue
        try:
            pq.read_metadata(file_path)
        except Exception as exc:
            _quarantine_invalid_file(parquet_root, file_path, exc)
            continue
        valid.append(file_path)
    return valid


def _file_list_sql(files: list[Path]) -> str:
    if not files:
        return "[]"
    return "[" + ", ".join(f"'{_duck_path(path)}'" for path in files) + "]"


def _count_rows_from_files(files: list[Path], con: duckdb.DuckDBPyConnection) -> int:
    if not files:
        return 0
    return int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet({_file_list_sql(files)}, hive_partitioning=false)"
        ).fetchone()[0]
    )


def _partition_snapshot(topic_dir: Path) -> dict[Path, list[Path]]:
    partitions: dict[Path, list[Path]] = defaultdict(list)
    for file_path in _validated_topic_files(topic_dir):
        partitions[file_path.parent].append(file_path)
    return dict(sorted(partitions.items(), key=lambda item: str(item[0])))


def compact_topic(topic_dir: Path, out_topic_dir: Path, dry_run: bool = False) -> dict:
    """Compacte tous les fichiers d'un topic par partition heure vers out_topic_dir."""

    topic_name = topic_dir.name.replace("topic=", "")
    con = duckdb.connect()

    partitions = _partition_snapshot(topic_dir)
    all_files = [path for files in partitions.values() for path in files]
    files_before = len(all_files)
    size_before = sum(path.stat().st_size for path in all_files if path.exists())
    bench_before = _benchmark(str(topic_dir / "**" / "*.parquet"), con)
    rows_before = _count_rows_from_files(all_files, con)

    if dry_run:
        print(
            f"  [DRY-RUN] {topic_name}: {len(partitions)} partitions a compacter "
            f"({files_before} fichiers -> ~{len(partitions)} fichiers)"
        )
        return {
            "topic": topic_name,
            "dry_run": True,
            "partitions": len(partitions),
            "files_before": files_before,
            "rows_before": rows_before,
        }

    if out_topic_dir.exists():
        shutil.rmtree(out_topic_dir)
    out_topic_dir.mkdir(parents=True, exist_ok=True)

    compacted = 0
    for partition_dir, files in partitions.items():
        if not files:
            continue
        rel = partition_dir.relative_to(topic_dir)
        out_part = out_topic_dir / rel
        out_part.mkdir(parents=True, exist_ok=True)
        out_file = out_part / "compacted.parquet"
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet({_file_list_sql(files)}, hive_partitioning=false)
            ) TO '{_duck_path(out_file)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """
        )
        compacted += 1

    out_files = sorted(out_topic_dir.rglob("*.parquet"))
    files_after = len(out_files)
    size_after = sum(path.stat().st_size for path in out_files if path.exists())
    bench_after = _benchmark(str(out_topic_dir / "**" / "*.parquet"), con)
    rows_after = _count_rows_from_files(out_files, con)

    return {
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
        "row_count_ok": rows_before == rows_after,
        "benchmark_before": bench_before,
        "benchmark_after": bench_after,
    }


def run_compaction(
    parquet_dir: Path,
    out_dir: Path | None = None,
    dry_run: bool = False,
    report_only: bool = False,
) -> dict:
    topics = [d for d in parquet_dir.iterdir() if d.is_dir() and d.name.startswith("topic=")]
    if not topics:
        print(f"[compact] Aucun topic trouve dans {parquet_dir}")
        return {}

    if out_dir is None:
        out_dir = parquet_dir.parent / "parquet_compacted"
    out_dir.mkdir(parents=True, exist_ok=True)

    total_before = _count_files(parquet_dir)
    print("\n-- Compaction Parquet Events --------------------------------")
    print(f"  Source     : {parquet_dir}")
    print(f"  Sortie     : {out_dir}")
    print(f"  Fichiers   : {total_before}")
    print(f"  Mode       : {'dry-run' if dry_run else 'report-only' if report_only else 'compaction -> ' + str(out_dir)}")

    if report_only:
        con = duckdb.connect()
        for topic_dir in topics:
            _partition_snapshot(topic_dir)
            glob = str(topic_dir / "**" / "*.parquet")
            n = _count_files(topic_dir)
            size = _dir_size_bytes(topic_dir)
            bench = _benchmark(glob, con)
            print(f"\n  {topic_dir.name}: {n} fichiers, {size // 1024} kB")
            for result in bench:
                print(f"    [{result['id']}] {result['label']}: {result['elapsed_ms']} ms")
        return {}

    summary = {"topics": [], "out_dir": str(out_dir)}
    for topic_dir in topics:
        out_topic_dir = out_dir / topic_dir.name
        print(f"\n  Compactage {topic_dir.name} -> {out_topic_dir.name}...")
        result = compact_topic(topic_dir, out_topic_dir, dry_run=dry_run)
        summary["topics"].append(result)
        if not dry_run:
            row_ok = "OK" if result.get("row_count_ok") else "ERREUR row_count mismatch"
            print(
                f"    {result['files_before']} -> {result['files_after']} fichiers "
                f"({result.get('files_reduced_pct', 0)}% reduit) | rows: {row_ok}"
            )
            for before, after in zip(result["benchmark_before"], result["benchmark_after"]):
                delta = before["elapsed_ms"] - after["elapsed_ms"]
                sign = "+" if delta > 0 else ""
                print(
                    f"    [{before['id']}] {before['label']}: "
                    f"{before['elapsed_ms']}ms -> {after['elapsed_ms']}ms ({sign}{delta:.1f}ms)"
                )

    report_path = parquet_dir.parent / "reports" / "compaction_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(f"\n[compact] Rapport sauve: {report_path}")

    total_after = _count_files(out_dir)
    print(f"[compact] Total: {total_before} -> {total_after} fichiers")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Compaction Parquet cold path")
    parser.add_argument("--parquet", default="data/parquet_events")
    parser.add_argument("--out", default=None, help="Repertoire de sortie (defaut: data/parquet_compacted)")
    parser.add_argument("--dry-run", action="store_true", help="Affiche ce qui serait fait sans modifier")
    parser.add_argument("--report-only", action="store_true", help="Affiche stats sans compacter")
    args = parser.parse_args()

    parquet_dir = _ROOT / args.parquet
    out_dir = (_ROOT / args.out) if args.out else None
    if not parquet_dir.exists():
        print(f"[compact] Repertoire introuvable: {parquet_dir}")
        sys.exit(1)

    run_compaction(parquet_dir, out_dir=out_dir, dry_run=args.dry_run, report_only=args.report_only)


if __name__ == "__main__":
    main()
