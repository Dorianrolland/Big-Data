from __future__ import annotations

import importlib.util
import json
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


def test_evaluate_checks_uses_configured_ingestion_threshold():
    checks = perf_lot4.evaluate_checks(
        hot_perf={"p99_ms": 1.0},
        score_perf={"p95_ms": 45.0, "errors": 0},
        ingest={"ingestion_msg_s": 0.5, "window_s": 20},
        dlq={"files": 0, "size_mb": 0.0},
        dlq_window={"files_delta": 0, "size_mb_delta": 0.0},
        min_ingestion_msg_s=0.01,
    )
    assert checks["ingestion_rate_gt_target_msg_s"] is True
    assert checks["ingestion_rate_gt_20_msg_s"] is False


def test_evaluate_checks_accepts_ingestion_at_exact_threshold():
    checks = perf_lot4.evaluate_checks(
        hot_perf={"p99_ms": 1.0},
        score_perf={"p95_ms": 45.0, "errors": 0},
        ingest={"ingestion_msg_s": 0.01, "window_s": 20},
        dlq={"files": 0, "size_mb": 0.0},
        dlq_window={"files_delta": 0, "size_mb_delta": 0.0},
        min_ingestion_msg_s=0.01,
    )
    assert checks["ingestion_rate_gt_target_msg_s"] is True


def test_api_json_retries_on_transient_url_errors(monkeypatch):
    class _Resp:
        def __init__(self, payload: bytes):
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return self.payload

    calls = {"n": 0}

    def fake_urlopen(req, timeout=0):  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] < 3:
            raise perf_lot4.urllib.error.URLError("temporary")
        return _Resp(b'{"ok": true}')

    monkeypatch.setattr(perf_lot4.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(perf_lot4.time, "sleep", lambda *_args, **_kwargs: None)

    out = perf_lot4.api_json("http://localhost:8001/health", retries=3)
    assert out["ok"] is True
    assert calls["n"] == 3


def test_replay_count_falls_back_to_zero_on_http_error(monkeypatch):
    def fake_api_json(*_args, **_kwargs):
        raise RuntimeError("gateway timeout")

    monkeypatch.setattr(perf_lot4, "api_json", fake_api_json)
    assert perf_lot4.replay_count("http://localhost:8001") == 0


def test_latest_report_applies_predicate(monkeypatch, tmp_path):
    monkeypatch.setattr(perf_lot4, "REPORT_DIR", tmp_path)
    (tmp_path / "smoke_e2e_20260417T100000Z.json").write_text(
        json.dumps({"passed": False, "base_url": "http://localhost:8001"}),
        encoding="utf-8",
    )
    (tmp_path / "smoke_e2e_20260417T100100Z.json").write_text(
        json.dumps({"passed": True, "base_url": "http://localhost:8001"}),
        encoding="utf-8",
    )

    selected = perf_lot4.latest_report("smoke_e2e_*.json", predicate=lambda payload: bool(payload.get("passed")))
    assert selected is not None
    path, payload = selected
    assert path.name == "smoke_e2e_20260417T100100Z.json"
    assert payload["passed"] is True


def test_select_smoke_report_prefers_same_base_and_passed(monkeypatch, tmp_path):
    monkeypatch.setattr(perf_lot4, "REPORT_DIR", tmp_path)
    (tmp_path / "smoke_e2e_20260417T100200Z.json").write_text(
        json.dumps({"passed": True, "base_url": "http://other-host:8001"}),
        encoding="utf-8",
    )
    (tmp_path / "smoke_e2e_20260417T100300Z.json").write_text(
        json.dumps({"passed": False, "base_url": "http://localhost:8001"}),
        encoding="utf-8",
    )
    (tmp_path / "smoke_e2e_20260417T100000Z.json").write_text(
        json.dumps({"passed": True, "base_url": "http://localhost:8001"}),
        encoding="utf-8",
    )

    selected = perf_lot4.select_smoke_report("http://localhost:8001")
    assert selected is not None
    path, payload = selected
    assert path.name == "smoke_e2e_20260417T100000Z.json"
    assert payload["passed"] is True
    assert payload["base_url"] == "http://localhost:8001"


def test_benchmark_score_offer_normalizes_minimum_requests(monkeypatch):
    monkeypatch.setattr(perf_lot4, "api_json", lambda *_args, **_kwargs: {"ok": True})
    result = perf_lot4.benchmark_score_offer(
        base_url="http://localhost:8001",
        requests_n=0,
        concurrency=4,
        timeout=1.0,
    )
    assert result["requests"] == 1
    assert result["success"] == 1
    assert result["errors"] == 0
    assert result["success_rate_pct"] == 100.0
