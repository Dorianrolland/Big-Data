"""Tests de non-régression pour le Data Mart Copilot (COP-025)."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "scripts"))

from importlib.util import spec_from_file_location, module_from_spec


def _import_script(name: str, path: Path):
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_synthetic_parquet_dir(base: Path) -> Path:
    """Génère un répertoire parquet_events synthétique pour les tests CI."""
    rng = np.random.default_rng(42)
    n = 500

    for topic, suffix in [
        ("order-offers-v1", "order.offer.v1"),
        ("order-events-v1", "order.event.v1"),
        ("context-signals-v1", "context.signal.v1"),
    ]:
        part = base / f"topic={topic}" / "year=2026" / "month=04" / "day=20" / "hour=13"
        part.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({
            "topic": topic,
            "event_type": suffix,
            "event_id": [f"e_{i:05d}" for i in range(n)],
            "offer_id": [f"o_{i:05d}" for i in range(n)],
            "order_id": [f"ord_{i:05d}" for i in range(n)],
            "courier_id": [f"drv_{i % 10:03d}" for i in range(n)],
            "status": "offered",
            "zone_id": [f"Z0{i % 5}" for i in range(n)],
            "pickup_lat": rng.uniform(40.6, 40.9, n),
            "pickup_lon": rng.uniform(-74.1, -73.8, n),
            "dropoff_lat": rng.uniform(40.6, 40.9, n),
            "dropoff_lon": rng.uniform(-74.1, -73.8, n),
            "estimated_fare_eur": rng.uniform(5.0, 25.0, n),
            "estimated_distance_km": rng.uniform(1.5, 12.0, n),
            "estimated_duration_min": rng.uniform(8.0, 35.0, n),
            "actual_fare_eur": rng.uniform(5.0, 25.0, n),
            "actual_distance_km": rng.uniform(1.5, 12.0, n),
            "actual_duration_min": rng.uniform(8.0, 35.0, n),
            "demand_index": rng.uniform(0.3, 1.0, n),
            "supply_index": rng.uniform(0.1, 0.8, n),
            "weather_factor": rng.uniform(1.0, 1.4, n),
            "traffic_factor": rng.uniform(0.7, 1.3, n),
            "source": "synthetic",
            "source_platform": "context_poller_public",
            "ts": pd.Timestamp("2026-04-20 13:00:00"),
            "day": 20, "hour": 13, "month": 4, "year": 2026,
        })
        df.to_parquet(part / "batch_00001.parquet", index=False, compression="snappy")

    return base


@pytest.fixture(scope="module")
def mart_dir(tmp_path_factory):
    """Build a fresh mart — uses real data if available, synthetic otherwise."""
    build_mart = _import_script("build_copilot_mart", _ROOT / "scripts" / "build-copilot-mart.py")
    real_parquet = _ROOT / "data" / "parquet_events"

    if real_parquet.exists() and any(real_parquet.rglob("*.parquet")):
        parquet_dir = real_parquet
    else:
        synthetic_base = tmp_path_factory.mktemp("synthetic_parquet")
        parquet_dir = _make_synthetic_parquet_dir(synthetic_base)

    out = tmp_path_factory.mktemp("mart")
    build_mart.build_mart(parquet_dir, out)
    return out


def test_mart_fact_context_signals_exists(mart_dir):
    assert (mart_dir / "fact_context_signals.parquet").exists()


def test_mart_row_count_consistent(mart_dir):
    """Row count in mart must match row count in raw parquet (real data only)."""
    raw_dir = _ROOT / "data" / "parquet_events" / "topic=context-signals-v1"
    if not raw_dir.exists():
        pytest.skip("context-signals parquet not found — skipping real-data row count check")

    con = duckdb.connect()
    raw_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{raw_dir}/**/*.parquet', hive_partitioning=true)"
    ).fetchone()[0]
    mart_count = con.execute(
        f"SELECT COUNT(*) FROM '{mart_dir / 'fact_context_signals.parquet'}'"
    ).fetchone()[0]
    assert mart_count == raw_count, f"Row count mismatch: mart={mart_count} raw={raw_count}"


def test_mart_pressure_ratio_non_null(mart_dir):
    con = duckdb.connect()
    path = mart_dir / "fact_context_signals.parquet"
    df = con.execute(
        f"SELECT pressure_ratio FROM '{path}' WHERE pressure_ratio IS NULL LIMIT 1"
    ).fetchdf()
    assert len(df) == 0, "pressure_ratio should not be null"


def test_mart_surge_candidate_boolean(mart_dir):
    con = duckdb.connect()
    path = mart_dir / "fact_context_signals.parquet"
    df = con.execute(f"SELECT DISTINCT surge_candidate FROM '{path}'").fetchdf()
    values = set(df["surge_candidate"].tolist())
    assert values.issubset({True, False})


def test_mart_offers_net_eur_h_positive(mart_dir):
    path = mart_dir / "fact_offers.parquet"
    if not path.exists():
        pytest.skip("fact_offers not built (no offer parquet data)")
    con = duckdb.connect()
    df = con.execute(f"SELECT net_eur_h FROM '{path}' WHERE net_eur_h < 0 LIMIT 1").fetchdf()
    assert len(df) == 0, "net_eur_h should not be negative"


def test_kpi_script_runs(mart_dir):
    query_kpis = _import_script("query_copilot_kpis", _ROOT / "scripts" / "query-copilot-kpis.py")
    results = query_kpis.run_kpis(mart_dir, fmt="json")
    assert len(results) >= 3
    for r in results:
        assert "id" in r
        assert "elapsed_ms" in r
        assert r["elapsed_ms"] < 5000, f"KPI {r['id']} too slow: {r['elapsed_ms']} ms"


def test_kpi_signals_count(mart_dir):
    query_kpis = _import_script("query_copilot_kpis", _ROOT / "scripts" / "query-copilot-kpis.py")
    results = {r["id"]: r for r in query_kpis.run_kpis(mart_dir, fmt="json")}
    kpi01 = results.get("kpi_01")
    if kpi01:
        total = kpi01["data"][0]["total_signals"]
        assert total > 0, "Expected context signals in mart"
