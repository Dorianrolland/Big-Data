"""Regression tests for Copilot Data Mart build + KPI coherence."""
from __future__ import annotations

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "scripts"))


def _import_script(name: str, path: Path):
    spec = spec_from_file_location(name, path)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_synthetic_parquet_dir(
    base: Path,
    *,
    include_source_platform: bool,
    include_source: bool = True,
    n: int = 500,
) -> Path:
    """Generate synthetic parquet_events tree for CI-safe mart tests."""
    rng = np.random.default_rng(42)

    topics = [
        ("order-offers-v1", "order.offer.v1"),
        ("order-events-v1", "order.event.v1"),
        ("context-signals-v1", "context.signal.v1"),
    ]

    for topic, event_type in topics:
        part = base / f"topic={topic}" / "year=2026" / "month=04" / "day=20" / "hour=13"
        part.mkdir(parents=True, exist_ok=True)

        if topic == "order-events-v1":
            statuses = np.where(
                np.arange(n) % 3 == 0,
                "accepted",
                np.where(np.arange(n) % 3 == 1, "rejected", "dropped_off"),
            )
        elif topic == "order-offers-v1":
            statuses = np.repeat("offered", n)
        else:
            statuses = np.repeat("signal", n)

        df = pd.DataFrame(
            {
                "topic": topic,
                "event_type": event_type,
                "event_id": [f"e_{i:05d}" for i in range(n)],
                "offer_id": [f"o_{i:05d}" for i in range(n)],
                "order_id": [f"ord_{i:05d}" for i in range(n)],
                "courier_id": [f"drv_{i % 10:03d}" for i in range(n)],
                "status": statuses,
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
                "ts": pd.Timestamp("2026-04-20 13:00:00", tz="UTC"),
                "day": 20,
                "hour": 13,
                "month": 4,
                "year": 2026,
            }
        )

        if include_source:
            df["source"] = np.repeat("synthetic_source", n)

        if include_source_platform:
            if topic == "context-signals-v1":
                source_platform = "context_poller_public"
            elif topic == "order-events-v1":
                source_platform = "event_stream"
            else:
                source_platform = "offers_stream"
            df["source_platform"] = np.repeat(source_platform, n)

        df.to_parquet(part / "batch_00001.parquet", index=False, compression="snappy")

    return base


def _build_synthetic_mart(
    tmp_path: Path,
    *,
    include_source_platform: bool,
    include_source: bool = True,
) -> Path:
    build_mart = _import_script("build_copilot_mart", _ROOT / "scripts" / "build-copilot-mart.py")
    parquet_dir = _make_synthetic_parquet_dir(
        tmp_path / ("synthetic_with_source_platform" if include_source_platform else "synthetic_source_only"),
        include_source_platform=include_source_platform,
        include_source=include_source,
    )
    suffix = (
        "mart_with_source_platform"
        if include_source_platform
        else ("mart_source_only" if include_source else "mart_no_source_columns")
    )
    out_dir = tmp_path / suffix
    build_mart.build_mart(parquet_dir, out_dir)
    return out_dir


@pytest.fixture(scope="module")
def mart_dir(tmp_path_factory):
    """Build mart from real data if present, otherwise synthetic fallback."""
    build_mart = _import_script("build_copilot_mart", _ROOT / "scripts" / "build-copilot-mart.py")
    real_parquet = _ROOT / "data" / "parquet_events"

    if real_parquet.exists() and any(real_parquet.rglob("*.parquet")):
        parquet_dir = real_parquet
    else:
        synthetic_base = tmp_path_factory.mktemp("synthetic_parquet")
        parquet_dir = _make_synthetic_parquet_dir(synthetic_base, include_source_platform=True)

    out = tmp_path_factory.mktemp("mart")
    build_mart.build_mart(parquet_dir, out)
    return out


def test_build_mart_supports_source_platform_schema(tmp_path):
    mart = _build_synthetic_mart(tmp_path, include_source_platform=True)
    con = duckdb.connect()
    try:
        for table in ["fact_offers", "fact_order_events", "fact_context_signals"]:
            path = mart / f"{table}.parquet"
            rows = con.execute(f"SELECT COUNT(*) FROM '{path}' WHERE source_platform IS NULL OR source_platform = ''").fetchone()[0]
            assert rows == 0
    finally:
        con.close()


def test_build_mart_supports_legacy_source_only_schema(tmp_path):
    mart = _build_synthetic_mart(
        tmp_path,
        include_source_platform=False,
        include_source=True,
    )
    con = duckdb.connect()
    try:
        for table in ["fact_offers", "fact_order_events", "fact_context_signals"]:
            path = mart / f"{table}.parquet"
            cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()}
            assert "source_platform" in cols
            distinct_values = {
                row[0]
                for row in con.execute(f"SELECT DISTINCT source_platform FROM '{path}'").fetchall()
            }
            assert "synthetic_source" in distinct_values
    finally:
        con.close()


def test_build_mart_supports_missing_source_columns(tmp_path):
    mart = _build_synthetic_mart(
        tmp_path,
        include_source_platform=False,
        include_source=False,
    )
    con = duckdb.connect()
    try:
        for table in ["fact_offers", "fact_order_events", "fact_context_signals"]:
            path = mart / f"{table}.parquet"
            distinct_values = {
                row[0]
                for row in con.execute(f"SELECT DISTINCT source_platform FROM '{path}'").fetchall()
            }
            assert distinct_values == {"unknown"}
    finally:
        con.close()


def test_mart_fact_context_signals_exists(mart_dir):
    assert (mart_dir / "fact_context_signals.parquet").exists()


def test_mart_row_count_consistent(mart_dir):
    """Mart row count should be coherent with raw parquet under live-ingest drift."""
    raw_dir = _ROOT / "data" / "parquet_events" / "topic=context-signals-v1"
    if not raw_dir.exists():
        pytest.skip("context-signals parquet not found - skipping real-data row count check")

    con = duckdb.connect()
    try:
        raw_glob = f"{str(raw_dir).replace(chr(92), '/')}/**/*.parquet"
        raw_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{raw_glob}', hive_partitioning=true)"
        ).fetchone()[0]
        mart_count = con.execute(
            f"SELECT COUNT(*) FROM '{mart_dir / 'fact_context_signals.parquet'}'"
        ).fetchone()[0]
        # In CI/dev, parquet ingestion may append files while the mart is being built.
        # We still require a coherent snapshot: mart cannot exceed the raw count.
        assert mart_count <= raw_count, f"Row count mismatch: mart={mart_count} raw={raw_count}"
        # Tolerate a bounded positive drift due to concurrent appends during build.
        assert (raw_count - mart_count) < 10000, (
            f"Row count drift too large: mart={mart_count} raw={raw_count}"
        )
    finally:
        con.close()


def test_mart_pressure_ratio_non_null(mart_dir):
    con = duckdb.connect()
    try:
        path = mart_dir / "fact_context_signals.parquet"
        df = con.execute(f"SELECT pressure_ratio FROM '{path}' WHERE pressure_ratio IS NULL LIMIT 1").fetchdf()
        assert len(df) == 0, "pressure_ratio should not be null"
    finally:
        con.close()


def test_mart_surge_candidate_boolean(mart_dir):
    con = duckdb.connect()
    try:
        path = mart_dir / "fact_context_signals.parquet"
        df = con.execute(f"SELECT DISTINCT surge_candidate FROM '{path}'").fetchdf()
        values = set(df["surge_candidate"].tolist())
        assert values.issubset({True, False})
    finally:
        con.close()


def test_mart_offers_net_eur_h_positive(mart_dir):
    path = mart_dir / "fact_offers.parquet"
    if not path.exists():
        pytest.skip("fact_offers not built (no offer parquet data)")

    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT net_eur_h FROM '{path}' WHERE net_eur_h < 0 LIMIT 1").fetchdf()
        assert len(df) == 0, "net_eur_h should not be negative"
    finally:
        con.close()


def test_kpi_script_runs(mart_dir):
    query_kpis = _import_script("query_copilot_kpis", _ROOT / "scripts" / "query-copilot-kpis.py")
    results = query_kpis.run_kpis(mart_dir, fmt="json")
    assert len(results) >= 3
    for row in results:
        assert "id" in row
        assert "elapsed_ms" in row
        assert row["elapsed_ms"] < 5000, f"KPI {row['id']} too slow: {row['elapsed_ms']} ms"


def test_kpi_signals_count(mart_dir):
    query_kpis = _import_script("query_copilot_kpis", _ROOT / "scripts" / "query-copilot-kpis.py")
    results = {row["id"]: row for row in query_kpis.run_kpis(mart_dir, fmt="json")}
    kpi01 = results.get("kpi_01")
    if kpi01:
        total = kpi01["data"][0]["total_signals"]
        assert total > 0, "Expected context signals in mart"


def test_kpi_accept_rate_is_true_rate_on_synthetic(tmp_path):
    mart = _build_synthetic_mart(tmp_path, include_source_platform=False)
    query_kpis = _import_script("query_copilot_kpis", _ROOT / "scripts" / "query-copilot-kpis.py")
    results = {row["id"]: row for row in query_kpis.run_kpis(mart, fmt="json")}

    kpi05 = results["kpi_05"]
    payload = kpi05["data"][0]

    assert int(payload["decided_offers"]) == 334
    assert int(payload["accepted_offers"]) == 167
    assert int(payload["rejected_offers"]) == 167
    assert float(payload["accept_rate_pct"]) == pytest.approx(50.0, abs=0.01)


def test_kpi_accept_rate_uses_latest_decision(tmp_path):
    mart_dir = tmp_path / "mart_kpi_latest_decision"
    mart_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        # Offer o1: accepted then rejected -> final decision should be rejected.
        {"offer_id": "o1", "status": "accepted", "ts": "2026-04-20T10:00:00Z"},
        {"offer_id": "o1", "status": "rejected", "ts": "2026-04-20T10:05:00Z"},
        # Offer o2: rejected then accepted -> final decision should be accepted.
        {"offer_id": "o2", "status": "rejected", "ts": "2026-04-20T11:00:00Z"},
        {"offer_id": "o2", "status": "accepted", "ts": "2026-04-20T11:10:00Z"},
        # Offer o3: non-decision event only -> should not be counted.
        {"offer_id": "o3", "status": "dropped_off", "ts": "2026-04-20T12:00:00Z"},
    ]
    pd.DataFrame(rows).to_parquet(mart_dir / "fact_order_events.parquet", index=False)

    query_kpis = _import_script("query_copilot_kpis", _ROOT / "scripts" / "query-copilot-kpis.py")
    results = {row["id"]: row for row in query_kpis.run_kpis(mart_dir, fmt="json")}
    payload = results["kpi_05"]["data"][0]

    assert int(payload["decided_offers"]) == 2
    assert int(payload["accepted_offers"]) == 1
    assert int(payload["rejected_offers"]) == 1
    assert float(payload["accept_rate_pct"]) == pytest.approx(50.0, abs=0.01)
