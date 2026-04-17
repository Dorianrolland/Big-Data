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

    monkeypatch.setattr(smoke_e2e.urllib.request, "urlopen", lambda *_args, **_kwargs: _Resp())

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


def test_fallback_driver_ids_from_env(monkeypatch):
    monkeypatch.setattr(smoke_e2e.os, "getenv", lambda *_args, **_kwargs: "drv_a, drv_b,drv_a,,")
    assert smoke_e2e.fallback_driver_ids() == ["drv_a", "drv_b"]


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
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: ["drv_demo_001"])
    monkeypatch.setattr(smoke_e2e, "driver_has_offers", lambda *_args, **_kwargs: False)
    try:
        smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120)
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "auto-discover driver_id" in str(exc)


def test_discover_driver_id_returns_first_nearby_when_no_candidate_has_offers(monkeypatch):
    monkeypatch.setattr(
        smoke_e2e,
        "wait_for_json",
        lambda *_args, **_kwargs: {"livreurs": [{"livreur_id": "near_1"}, {"livreur_id": "near_2"}]},
    )
    monkeypatch.setattr(smoke_e2e, "fallback_driver_ids", lambda: ["drv_demo_001"])
    monkeypatch.setattr(smoke_e2e, "driver_has_offers", lambda *_args, **_kwargs: False)

    assert smoke_e2e.discover_driver_id("http://localhost:8001", timeout_s=120) == "near_1"


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
