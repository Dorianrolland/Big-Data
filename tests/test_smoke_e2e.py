from __future__ import annotations

import importlib.util
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path


def _load_smoke_module():
    root = Path(__file__).resolve().parent.parent
    script_path = root / "scripts" / "smoke-e2e.py"
    spec = importlib.util.spec_from_file_location("smoke_e2e_module", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


smoke_e2e = _load_smoke_module()


def test_compute_replay_window_from_offer_timestamp():
    replay_from, replay_to = smoke_e2e.compute_replay_window("2024-11-01T12:00:00+00:00")
    assert replay_from.isoformat() == "2024-11-01T11:55:00+00:00"
    assert replay_to.isoformat() == "2024-11-01T12:05:00+00:00"


def test_compute_replay_window_without_offer_timestamp():
    replay_from, replay_to = smoke_e2e.compute_replay_window(None)
    assert (replay_to - replay_from).total_seconds() == 600
    assert replay_to >= replay_from


def test_normalize_limit_candidates_dedupes_and_filters():
    assert smoke_e2e._normalize_limit_candidates(50, [50, 25, 0, -3, 25, "40"]) == [50, 25, 40]
    assert smoke_e2e._normalize_limit_candidates(50, []) == [50]


def test_resolve_compose_env_file_prefers_explicit_path(tmp_path):
    env_file = tmp_path / "fleet_demo.env"
    env_file.write_text("TLC_SCENARIO=fleet_demo\n", encoding="utf-8")
    resolved = smoke_e2e.resolve_compose_env_file(str(env_file))
    assert resolved == str(env_file.resolve())


def test_resolve_default_min_drivers_reads_compose_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / "fleet_demo.env"
    env_file.write_text("FLEET_DEMO_WARMUP_MIN_DRIVERS=77\n", encoding="utf-8")
    monkeypatch.setenv("FLEET_DEMO_WARMUP_MIN_DRIVERS", "")
    out = smoke_e2e.resolve_default_min_drivers(str(env_file))
    assert out == 77


def test_resolve_default_min_drivers_prefers_process_env_over_file(tmp_path, monkeypatch):
    env_file = tmp_path / "fleet_demo.env"
    env_file.write_text("FLEET_DEMO_WARMUP_MIN_DRIVERS=77\n", encoding="utf-8")
    monkeypatch.setenv("FLEET_DEMO_WARMUP_MIN_DRIVERS", "12")
    out = smoke_e2e.resolve_default_min_drivers(str(env_file))
    assert out == 12


def test_compose_up_includes_env_file_when_provided(tmp_path, monkeypatch):
    env_file = tmp_path / "fleet_demo.env"
    env_file.write_text("TLC_SCENARIO=fleet_demo\n", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], check: bool = True):  # noqa: ANN001
        _ = check
        calls.append(cmd)
        return None

    monkeypatch.setattr(smoke_e2e, "run_cmd", fake_run_cmd)
    smoke_e2e.compose_up(build=True, compose_env_file=str(env_file))
    assert calls
    assert calls[0][:5] == ["docker", "compose", "--env-file", str(env_file.resolve()), "up"]
    assert calls[0][-1] == "--build"


def test_parse_offer_ts_invalid_value_returns_none():
    assert smoke_e2e.parse_offer_ts("not-a-date") is None


def test_build_replay_url_contains_driver_and_limit():
    replay_from, replay_to = smoke_e2e.compute_replay_window("2024-11-01T12:00:00+00:00")
    url = smoke_e2e.build_replay_url(
        base_url="http://localhost:8001",
        replay_from_ts=replay_from,
        replay_to_ts=replay_to,
        driver_id="drv_demo_001",
        limit=50,
    )
    parsed = urllib.parse.urlparse(url)
    assert parsed.path == "/copilot/replay"
    query = urllib.parse.parse_qs(parsed.query)
    assert query["driver_id"] == ["drv_demo_001"]
    assert query["limit"] == ["50"]
    assert query["from"] == ["2024-11-01T11:55:00+00:00"]
    assert query["to"] == ["2024-11-01T12:05:00+00:00"]


def test_summarize_offer_quality_returns_expected_metrics():
    summary = smoke_e2e.summarize_offer_quality(
        {
            "offers": [
                {"decision": "accept", "accept_score": 0.8, "eur_per_hour_net": 22.0},
                {"decision": "reject", "accept_score": 0.3, "eur_per_hour_net": 9.5},
                {"decision": "accept", "accept_score": 0.7, "eur_per_hour_net": 18.0},
            ]
        }
    )
    assert summary["offers_accept_count"] == 2
    assert summary["offers_reject_count"] == 1
    assert summary["offers_accept_rate_pct"] == 66.67
    assert summary["offers_avg_accept_score"] == 0.6
    assert summary["offers_avg_eur_per_hour"] == 16.5
    assert summary["top_offer_accept_score"] == 0.8
    assert summary["top_offer_eur_per_hour"] == 22.0


def test_wait_for_json_retries_and_passes_request_timeout(monkeypatch):
    calls: list[float] = []
    payloads = [{"count": 0}, {"count": 3}]

    def fake_request_json(url: str, method: str = "GET", payload=None, timeout: float = 10.0):  # noqa: ANN001
        _ = url, method, payload
        calls.append(timeout)
        return payloads.pop(0)

    monkeypatch.setattr(smoke_e2e, "request_json", fake_request_json)
    out = smoke_e2e.wait_for_json(
        "http://localhost:8001/fake",
        lambda x: int(x.get("count", 0)) > 0,
        timeout_s=5,
        step_s=0,
        request_timeout_s=7.5,
    )
    assert out["count"] == 3
    assert calls == [7.5, 7.5]


def test_summarize_offer_quality_handles_invalid_values():
    summary = smoke_e2e.summarize_offer_quality(
        {
            "offers": [
                {"decision": "accept", "accept_score": "0.9", "eur_per_hour_net": "20.5"},
                {"decision": "pending", "accept_score": "bad", "eur_per_hour_net": None},
                {"decision": "reject", "accept_score": 0.2, "eur_per_hour_net": "7.0"},
            ]
        }
    )
    assert summary["offers_accept_count"] == 1
    assert summary["offers_reject_count"] == 1
    assert summary["offers_unknown_decision_count"] == 1
    assert summary["offers_accept_rate_pct"] == 33.33
    assert summary["offers_avg_accept_score"] == 0.3667
    assert summary["offers_avg_eur_per_hour"] == 9.167
    assert summary["top_offer_accept_score"] == 0.9
    assert summary["top_offer_eur_per_hour"] == 20.5


def test_benchmark_score_offer_normalizes_minimum_requests(monkeypatch):
    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self) -> bytes:
            return b"{}"

    class _Opener:
        def open(self, *_args, **_kwargs):
            return _Resp()

    monkeypatch.setattr(smoke_e2e, "LOCAL_HTTP_OPENER", _Opener())

    out = smoke_e2e.benchmark_score_offer(
        base_url="http://localhost:8001",
        requests_n=0,
        concurrency=4,
        timeout=1.0,
    )
    assert out["requests"] == 1
    assert out["success"] == 1
    assert out["errors"] == 0
    assert out["success_rate_pct"] == 100.0


def test_flatten_score_offer_kpi_exports_compat_fields():
    out = smoke_e2e.flatten_score_offer_kpi(
        {
            "requests": "120",
            "success": 119,
            "errors": "1",
            "p95_ms": "24.8912",
            "p99_ms": 29.4,
        }
    )
    assert out["score_offer_requests"] == 120
    assert out["score_offer_success"] == 119
    assert out["score_offer_error_count"] == 1
    assert out["score_offer_p95_ms"] == 24.891
    assert out["score_offer_p99_ms"] == 29.4


def test_fallback_driver_ids_from_env(monkeypatch):
    def fake_getenv(key: str, default: str = "") -> str:
        if key == "COPILOT_SMOKE_DRIVER_FALLBACKS":
            return "drv_a, drv_b,drv_a,,"
        if key == "TLC_SINGLE_DRIVER_ID":
            return "drv_single"
        return default

    monkeypatch.setattr(smoke_e2e.os, "getenv", fake_getenv)
    assert smoke_e2e.fallback_driver_ids() == ["drv_a", "drv_b", "drv_single", "drv_demo_001", "L001"]


def test_discover_driver_id_prefers_nearby_with_offers(monkeypatch):
    monkeypatch.setattr(
        smoke_e2e,
        "wait_for_json",
        lambda *_args, **_kwargs: {"livreurs": [{"livreur_id": "near_1"}, {"livreur_id": "near_2"}]},
    )

    def fake_has_offers(base_url: str, driver_id: str, timeout_s: float = 10.0) -> bool:
        _ = base_url, timeout_s
        return driver_id == "near_2"

    monkeypatch.setattr(smoke_e2e, "driver_has_offers", fake_has_offers)
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: ["drv_demo_001"])

    out = smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120)
    assert out == "near_2"


def test_discover_driver_id_falls_back_when_nearby_empty(monkeypatch):
    monkeypatch.setattr(smoke_e2e, "wait_for_json", lambda *_args, **_kwargs: {"livreurs": []})
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: ["drv_demo_001", "L001"])
    monkeypatch.setattr(smoke_e2e, "driver_has_offers", lambda *_args, **_kwargs: True)
    assert smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120) == "drv_demo_001"


def test_discover_driver_id_raises_when_no_candidate(monkeypatch):
    monkeypatch.setattr(smoke_e2e, "wait_for_json", lambda *_args, **_kwargs: {"livreurs": []})
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: [])
    monkeypatch.setattr(smoke_e2e, "driver_has_offers", lambda *_args, **_kwargs: False)
    try:
        smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "auto-discover driver_id" in str(exc)


def test_discover_driver_id_returns_fallback_even_without_offers(monkeypatch):
    monkeypatch.setattr(smoke_e2e, "wait_for_json", lambda *_args, **_kwargs: {"livreurs": []})
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: ["drv_demo_001"])
    monkeypatch.setattr(smoke_e2e, "driver_has_offers", lambda *_args, **_kwargs: False)
    assert smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120) == "drv_demo_001"


def test_discover_driver_id_returns_first_nearby_when_no_candidate_has_offers(monkeypatch):
    monkeypatch.setattr(
        smoke_e2e,
        "wait_for_json",
        lambda *_args, **_kwargs: {"livreurs": [{"livreur_id": "near_1"}, {"livreur_id": "near_2"}]},
    )
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: ["drv_demo_001"])
    monkeypatch.setattr(smoke_e2e, "driver_has_offers", lambda *_args, **_kwargs: False)

    assert smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120) == "near_1"


def test_active_couriers_count_does_not_fallback_to_tlc_replay_without_flag(monkeypatch):
    def fake_request_json(url: str, method: str = "GET", payload=None, timeout: float = 10.0):  # noqa: ANN001
        _ = method, payload, timeout
        if url.endswith("/stats"):
            return {}
        if "/livreurs-proches" in url:
            return {}
        if url.endswith("/copilot/health"):
            return {"tlc_replay": {"active_trips": "77"}}
        return {}

    monkeypatch.setattr(smoke_e2e, "request_json", fake_request_json)
    monkeypatch.delenv("COPILOT_ALLOW_REPLAY_ACTIVE_FALLBACK", raising=False)
    assert smoke_e2e.active_couriers_count("http://localhost:8001", timeout_s=6) == 0


def test_active_couriers_count_falls_back_to_tlc_replay_active_trips_with_flag(monkeypatch):
    def fake_request_json(url: str, method: str = "GET", payload=None, timeout: float = 10.0):  # noqa: ANN001
        _ = method, payload, timeout
        if url.endswith("/stats"):
            return {}
        if "/livreurs-proches" in url:
            return {}
        if url.endswith("/copilot/health"):
            return {"tlc_replay": {"active_trips": "77"}}
        return {}

    monkeypatch.setattr(smoke_e2e, "request_json", fake_request_json)
    monkeypatch.setenv("COPILOT_ALLOW_REPLAY_ACTIVE_FALLBACK", "1")
    assert smoke_e2e.active_couriers_count("http://localhost:8001", timeout_s=6) == 77


def test_fetch_replay_payload_uses_offer_window_first(monkeypatch):
    calls: list[str] = []

    def fake_wait(url: str, *_args, **_kwargs):  # noqa: ANN001
        calls.append(url)
        return {"count": 3}

    monkeypatch.setattr(smoke_e2e, "wait_for_json", fake_wait)

    payload, replay_url, window, strategy = smoke_e2e.fetch_replay_payload(
        base_url="http://localhost:8001",
        driver_id="drv_demo_001",
        offer_ts_str="2024-11-01T12:00:00+00:00",
        timeout_s=90,
        limit=50,
    )

    assert payload["count"] == 3
    assert strategy == "offer_ts_window"
    assert replay_url == calls[0]
    assert len(calls) == 1
    assert window[0].isoformat() == "2024-11-01T11:55:00+00:00"
    assert window[1].isoformat() == "2024-11-01T12:05:00+00:00"


def test_fetch_replay_payload_falls_back_to_recent_window(monkeypatch):
    offer_from = datetime(2024, 11, 1, 11, 55, tzinfo=timezone.utc)
    offer_to = datetime(2024, 11, 1, 12, 5, tzinfo=timezone.utc)
    recent_from = datetime(2024, 11, 2, 13, 0, tzinfo=timezone.utc)
    recent_to = datetime(2024, 11, 2, 13, 10, tzinfo=timezone.utc)

    monkeypatch.setattr(
        smoke_e2e,
        "compute_replay_window",
        lambda offer_ts: (offer_from, offer_to) if offer_ts is not None else (recent_from, recent_to),
    )

    calls: list[str] = []

    def fake_wait(url: str, *_args, **_kwargs):  # noqa: ANN001
        calls.append(url)
        if len(calls) == 1:
            raise TimeoutError("offer window timeout")
        return {"count": 4}

    monkeypatch.setattr(smoke_e2e, "wait_for_json", fake_wait)

    payload, replay_url, window, strategy = smoke_e2e.fetch_replay_payload(
        base_url="http://localhost:8001",
        driver_id="drv_demo_001",
        offer_ts_str="2024-11-01T12:00:00+00:00",
        timeout_s=90,
        limit=50,
    )

    assert len(calls) == 2
    assert payload["count"] == 4
    assert strategy == "recent_window"
    assert replay_url == calls[1]
    assert window == (recent_from, recent_to)


def test_fetch_replay_payload_tries_lower_limit_on_retry(monkeypatch):
    calls: list[str] = []

    def fake_wait(url: str, *_args, **_kwargs):  # noqa: ANN001
        calls.append(url)
        if "limit=50" in url:
            raise TimeoutError("primary limit timeout")
        return {"count": 2}

    monkeypatch.setattr(smoke_e2e, "wait_for_json", fake_wait)

    payload, replay_url, _window, strategy = smoke_e2e.fetch_replay_payload(
        base_url="http://localhost:8001",
        driver_id="drv_demo_001",
        offer_ts_str="2024-11-01T12:00:00+00:00",
        timeout_s=60,
        limit=50,
        limit_candidates=[50, 25],
    )

    assert payload["count"] == 2
    assert "limit=25" in replay_url
    assert strategy.startswith("offer_ts_window_limit_")
    assert len(calls) == 2


def test_fetch_replay_payload_skips_empty_count_before_success(monkeypatch):
    calls: list[str] = []
    payloads = [{"count": 0}, {"count": 3}]

    def fake_wait(url: str, *_args, **_kwargs):  # noqa: ANN001
        calls.append(url)
        return payloads.pop(0)

    monkeypatch.setattr(smoke_e2e, "wait_for_json", fake_wait)

    payload, replay_url, _window, strategy = smoke_e2e.fetch_replay_payload(
        base_url="http://localhost:8001",
        driver_id="drv_demo_001",
        offer_ts_str="2024-11-01T12:00:00+00:00",
        timeout_s=60,
        limit=50,
        limit_candidates=[50, 25],
    )

    assert payload["count"] == 3
    assert "limit=25" in replay_url
    assert strategy.startswith("offer_ts_window_limit_")
    assert len(calls) == 2


def test_fetch_replay_payload_returns_last_payload_when_all_counts_empty(monkeypatch):
    monkeypatch.setattr(smoke_e2e, "wait_for_json", lambda *_args, **_kwargs: {"count": 0})

    payload, replay_url, _window, strategy = smoke_e2e.fetch_replay_payload(
        base_url="http://localhost:8001",
        driver_id="drv_demo_001",
        offer_ts_str="2024-11-01T12:00:00+00:00",
        timeout_s=60,
        limit=50,
        limit_candidates=[50, 25],
    )

    assert payload["count"] == 0
    assert replay_url
    assert strategy


def test_fetch_replay_payload_raises_when_all_windows_fail(monkeypatch):
    monkeypatch.setattr(
        smoke_e2e,
        "wait_for_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("replay timeout")),
    )
    try:
        smoke_e2e.fetch_replay_payload(
            base_url="http://localhost:8001",
            driver_id="drv_demo_001",
            offer_ts_str="2024-11-01T12:00:00+00:00",
            timeout_s=90,
            limit=50,
        )
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        msg = str(exc)
        assert "offer_ts_window" in msg
        assert "recent_window" in msg


def test_fetch_replay_payload_uses_bounded_attempt_timeout(monkeypatch):
    seen = {"timeout_s": [], "request_timeout_s": []}

    def fake_wait(url: str, predicate, timeout_s: int, step_s: float = 2.0, request_timeout_s: float = 10.0):  # noqa: ANN001
        _ = url, predicate, step_s
        seen["timeout_s"].append(timeout_s)
        seen["request_timeout_s"].append(request_timeout_s)
        return {"count": 1}

    monkeypatch.setattr(smoke_e2e, "wait_for_json", fake_wait)
    _payload, _url, _window, _strategy = smoke_e2e.fetch_replay_payload(
        base_url="http://localhost:8001",
        driver_id="drv_demo_001",
        offer_ts_str="2024-11-01T12:00:00+00:00",
        timeout_s=240,
        limit=40,
        limit_candidates=[40, 20],
    )

    assert seen["timeout_s"]
    assert seen["request_timeout_s"]
    assert seen["timeout_s"][0] <= 45
    assert seen["request_timeout_s"][0] <= 20.0
