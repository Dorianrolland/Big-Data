from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_perf_lot4_module():
    root = Path(__file__).resolve().parent.parent
    script_path = root / "scripts" / "perf-lot4.py"
    spec = importlib.util.spec_from_file_location("perf_lot4_module", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


perf_lot4 = _load_perf_lot4_module()


def test_dlq_window_delta_tracks_growth():
    start = {"files": 1, "size_mb": 2.0}
    end = {"files": 3, "size_mb": 2.75}
    delta = perf_lot4.dlq_window_delta(start, end)
    assert delta["files_start"] == 1
    assert delta["files_end"] == 3
    assert delta["files_delta"] == 2
    assert delta["size_mb_delta"] == 0.75


def test_evaluate_checks_detects_new_dlq_errors_in_window():
    checks = perf_lot4.evaluate_checks(
        hot_perf={"p99_ms": 1.0},
        score_perf={"p95_ms": 40.0, "errors": 0},
        ingest={"ingestion_msg_s": 30.0, "window_s": 20},
        dlq={"files": 1, "size_mb": 7.0},
        dlq_window={"files_delta": 1, "size_mb_delta": 0.1},
    )
    assert checks["dlq_is_empty"] is False
    assert checks["dlq_no_new_errors_in_window"] is False


def test_compute_passed_non_strict_allows_historical_dlq():
    checks = perf_lot4.evaluate_checks(
        hot_perf={"p99_ms": 1.0},
        score_perf={"p95_ms": 50.0, "errors": 0},
        ingest={"ingestion_msg_s": 25.0, "window_s": 20},
        dlq={"files": 2, "size_mb": 4.0},
        dlq_window={"files_delta": 0, "size_mb_delta": 0.0},
    )
    assert checks["dlq_is_empty"] is False
    assert checks["dlq_no_new_errors_in_window"] is True
    assert perf_lot4.compute_passed(checks, require_dlq_empty=False) is True
    assert perf_lot4.compute_passed(checks, require_dlq_empty=True) is False


def test_required_check_keys_add_cold_flush_when_present():
    checks = {
        "hot_path_p99_lt_10ms": True,
        "score_offer_p95_lt_150ms": True,
        "score_offer_error_free": True,
        "ingestion_rate_gt_20_msg_s": True,
        "dlq_no_new_errors_in_window": True,
        "dlq_is_empty": True,
        "cold_event_parquet_growth_after_flush": True,
    }
    keys = perf_lot4.required_check_keys(checks, require_dlq_empty=True)
    assert "cold_event_parquet_growth_after_flush" in keys
    assert "dlq_is_empty" in keys
