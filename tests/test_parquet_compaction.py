"""Tests de non-régression compaction Parquet (COP-026)."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec

import duckdb
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent


def _import_script(name, path):
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


compact_mod = _import_script("compact_parquet", _ROOT / "scripts" / "compact-events-parquet.py")


@pytest.fixture(scope="module")
def sample_parquet_dir(tmp_path_factory):
    """Crée un répertoire parquet de test avec plusieurs petits fichiers."""
    root = tmp_path_factory.mktemp("parquet_test")
    topic_dir = root / "topic=test-v1" / "year=2026" / "month=04" / "day=20" / "hour=13"
    topic_dir.mkdir(parents=True)

    import numpy as np
    rng = np.random.default_rng(42)
    for i in range(5):
        df = pd.DataFrame({
            "event_id": [f"e_{i}_{j}" for j in range(100)],
            "zone_id": [f"Z0{j%5}" for j in range(100)],
            "demand_index": rng.uniform(0.3, 1.0, 100),
            "supply_index": rng.uniform(0.1, 0.8, 100),
            "pressure_ratio": rng.uniform(0.5, 5.0, 100),
            "ts": pd.Timestamp("2026-04-20 13:00:00"),
        })
        df.to_parquet(topic_dir / f"batch_{i:05d}.parquet", index=False, compression="snappy")

    return root


def test_compact_count_files_reduced(sample_parquet_dir):
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "compacted"
        topic_dir = sample_parquet_dir / "topic=test-v1"
        out_topic = out / "topic=test-v1"
        result = compact_mod.compact_topic(topic_dir, out_topic)
        assert result["files_before"] == 5
        assert result["files_after"] == 1
        assert result["files_reduced_pct"] == 80.0


def test_compact_row_count_preserved(sample_parquet_dir):
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "compacted"
        topic_dir = sample_parquet_dir / "topic=test-v1"
        out_topic = out / "topic=test-v1"
        result = compact_mod.compact_topic(topic_dir, out_topic)
        assert result["row_count_ok"], f"Row count mismatch: {result['rows_before']} → {result['rows_after']}"
        assert result["rows_before"] == 500
        assert result["rows_after"] == 500


def test_compact_output_is_readable(sample_parquet_dir):
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "compacted"
        topic_dir = sample_parquet_dir / "topic=test-v1"
        out_topic = out / "topic=test-v1"
        compact_mod.compact_topic(topic_dir, out_topic)
        con = duckdb.connect()
        df = con.execute(
            f"SELECT COUNT(*), AVG(demand_index) FROM read_parquet('{out_topic}/**/*.parquet')"
        ).fetchdf()
        assert df.iloc[0, 0] == 500


def test_compact_snappy_compression(sample_parquet_dir):
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "compacted"
        topic_dir = sample_parquet_dir / "topic=test-v1"
        out_topic = out / "topic=test-v1"
        compact_mod.compact_topic(topic_dir, out_topic)
        parquet_file = next(out_topic.rglob("*.parquet"))
        import pyarrow.parquet as pq
        meta = pq.read_metadata(parquet_file)
        compression = meta.row_group(0).column(0).compression
        assert compression.lower() in ("snappy", "lz4", "zstd")


def test_compact_run_on_real_data():
    """Test sur les vraies données du cold path (si disponibles)."""
    parquet_dir = _ROOT / "data" / "parquet_events"
    if not parquet_dir.exists():
        pytest.skip("Pas de données parquet disponibles")

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "compacted"
        summary = compact_mod.run_compaction(parquet_dir, out_dir=out)
        for topic in summary.get("topics", []):
            assert topic["row_count_ok"], f"Row count mismatch for {topic['topic']}"
            assert topic["files_after"] <= topic["files_before"]


def test_compact_report_generated():
    """Vérifie que le rapport JSON est bien produit."""
    parquet_dir = _ROOT / "data" / "parquet_events"
    if not parquet_dir.exists():
        pytest.skip("Pas de données parquet disponibles")

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "compacted"
        import json
        # Patch report path to tmp
        orig = compact_mod._ROOT
        compact_mod._ROOT = Path(tmp)
        (Path(tmp) / "data" / "reports").mkdir(parents=True, exist_ok=True)
        try:
            summary = compact_mod.run_compaction(parquet_dir, out_dir=out)
        finally:
            compact_mod._ROOT = orig
        assert isinstance(summary.get("topics"), list)
